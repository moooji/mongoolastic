'use strict';

const util = require('util');
const events = require('events');
const _ = require('lodash');
const Bluebird = require('bluebird');
const mongoose = require('mongoose');
const elasticsearch = require('./elasticsearch');
const errors = require('./errors');

// Inherit from EventEmitter
util.inherits(Mongoolastic, events.EventEmitter);

/**
 * Elasticsearch plugin
 * @constructor
 */

function Mongoolastic() {
  this.index = null;
  this.settings = {};
  this.registeredModels = new Map();
  this.es = elasticsearch.create();
}

Mongoolastic.prototype.InvalidArgumentError = errors.InvalidArgumentError;

/**
 * Connects to Elasticsearch, ensures the index
 * and tests the connection with a ping.
 * @param {string|array} hosts
 * @param {string} index
 * @param {object|function} [options]
 * @param {function} [callback]
 * @returns {Promise}
 */

Mongoolastic.prototype.connect = function(hosts, index, options, callback) {

  // Check if options are provided
  // or if the options argument is actually the callback
  if (callback === undefined && _.isFunction(options)) {
    callback = options;
    options = undefined;
  }

  return Bluebird.resolve([hosts, index, options])
    .spread((hosts, index, options) => {

      // Index
      if (this.es.isValidIndex(index)) {
        this.index = index;
      }

      // Options
      if (options && !_.isPlainObject(options)) {
        throw new errors.InvalidArgumentError('invalid-options');
      }

      // Settings
      if (options && this.es.isValidSettings(options.settings)) {
        this.settings = options.settings;
      }

      return this.es.connect(hosts);
    })
    .then(() => {
      return this.es.ensureIndex(this.index, this.settings, this.getMappings());
    })
    .nodeify(callback);
};

/**
 * Registers a model to be connected to with an Elasticsearch index
 * Returns promise or calls callback (if provided)
 * @param {object} model
 * @param {object} [options]
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */

Mongoolastic.prototype.registerModel = function(model, options, callback) {

  const self = this;

  // Check if options are provided
  // or if the options argument is actually the callback
  if (callback === undefined && _.isFunction(options)) {
    callback = options;
    options = undefined;
  }

  return Bluebird.resolve(model)
    .then((model) => {

      // Validation
      if (!(model.schema instanceof mongoose.Schema)) {
        throw new errors.InvalidArgumentError('invalid-model');
      }

      // Mapping
      let mapping = null;
      if (options && options.mapping) {

        if (this.es.isValidMapping(options.mapping)) {
          mapping = options.mapping;
        } else {
          throw new errors.InvalidArgumentError('invalid-mapping');
        }
      }

      // Transform function
      let transform = null;
      if (options && options.transform) {

        if (_.isFunction(options.transform)) {
          transform = Bluebird.promisify(options.transform);
        } else {
          throw new errors.InvalidArgumentError('invalid-transform');
        }
      }

      // Register model, mapping and hooks
      self.registeredModels.set(model.modelName, {model, mapping, transform});

      model.schema.post('remove', function onRemove(doc) {
        self.removeDoc(doc);
      });

      model.schema.post('save', function onSave(doc) {
        self.indexDoc(doc);
      });

      model.schema.statics.sync = function sync(callback) {
        self.sync(this, callback);
      };
    })
    .nodeify(callback);
};

/**
 * Indexes a mongoose document in Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {boolean} useBulk
 * @param {function} [callback]
 * @throws {ModelNotFoundError}
 * @returns {Promise}
 */

Mongoolastic.prototype.indexDoc = function(doc, useBulk, callback) {

  return Bluebird.resolve(doc)
    .then((doc) => {

      // This method is called for ALL documents that share the
      // model's SCHEMA. Thus, we need to check first and filter out
      // all documents that share the same schema, but belong to
      // other, non registered models.
      const id = doc.id;
      const modelName = doc.constructor.modelName;
      const registeredModel = this.registeredModels.get(modelName);

      if (!registeredModel) {
        return;
      }

      // Apply transform
      if (registeredModel.transform) {

        return registeredModel.transform(doc)
          .then((transformedDoc) => {
            return this.es.indexDoc(id, transformedDoc, modelName, this.index, useBulk);
          });
      }

      return this.es.indexDoc(id, doc, modelName, this.index, useBulk);
    })
    .nodeify(callback);
};

/**
 * Removes a mongoose object from Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */

Mongoolastic.prototype.removeDoc = function(doc, index, callback) {

  return Bluebird.resolve(doc)
    .then((doc) => {

      // This method is called for ALL documents that share the
      // model's SCHEMA. Thus we need to check first and filter
      // all documents that share the same schema, but belong to
      // other, non registered models.
      const type = doc.constructor.modelName;
      const isRegistered = this.registeredModels.has(type);

      if (!isRegistered) {
        return;
      }

      const id = doc.id;
      return this.es.deleteDoc(id, type, this.index);
    })
    .nodeify(callback);
};

/**
 * Gets the merged mappings for all registered models
 * @throws {InvalidArgumentError}
 * @returns {object}
 */

Mongoolastic.prototype.getMappings = function() {

  let mappings = {};

  this.registeredModels.forEach((value, key) => {

    if (value.mapping) {
      mappings = _.merge(mappings, {
        [key]: {properties: value.mapping}
      });
    }
  });

  return mappings;
};

/**
 * Syncs all documents of registered model
 * @param {mongoose.Model} model
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */

Mongoolastic.prototype.sync = function(model, callback) {

  return new Bluebird((resolve, reject) => {

    // Validation
    if (!(model.schema instanceof mongoose.Schema)) {
      return reject(new errors.InvalidArgumentError('invalid-model'));
    }

    const query = {};
    const stream = model.find(query).stream();

    stream.on('data', (doc) => {

      stream.pause();

      this.indexDoc(doc, true)
        .then(() => stream.resume());
    });

    stream.on('error', (err) => {
      return reject(err);
    });

    stream.on('close', () => {
      return resolve();
    });
  })
  .nodeify(callback);
};

/**
 * Searches the Elasticsearch index based on a supplied query
 * Returns a promise or calls callback (if provided)
 * @param {object} query
 * @param {function} [callback]
 * @returns {Promise}
 */

Mongoolastic.prototype.search = function(query, callback) {

  return Bluebird.resolve(query)
    .then((query) => {
      return this.es.search(query);
    })
    .nodeify(callback);
};

module.exports = new Mongoolastic();
