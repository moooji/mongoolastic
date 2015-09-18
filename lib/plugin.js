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
  this.es = elasticsearch;
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

  const self = this;

  // Check if options are provided
  // or if the options argument is actually the callback
  if (callback === undefined && _.isFunction(options)) {
    callback = options;
    options = undefined;
  }

  return Bluebird.resolve([hosts, index, options])
    .spread((hosts, index, options) => {

      // Index
      if (!elasticsearch.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index');
      }

      self.index = index;

      // Settings
      if (options && options.settings) {

        if (elasticsearch.isValidSettings(options.settings)) {
          self.settings = options.settings;
        } else {
          throw new errors.InvalidArgumentError('invalid-settings');
        }
      }

      return self.es.connect(hosts);
    })
    .then(() => {
      return self.es.ensureIndex(self.index, self.settings, self.getMappings());
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

        if (elasticsearch.isValidMapping(options.mapping)) {
          mapping = options.mapping;
        } else {
          throw new errors.InvalidArgumentError('invalid-mapping');
        }
      }

      // Transform function
      let transform = null;
      if (options && options.transform) {

        if (_.isFunction(options.transform)) {
          transform = options.transform;
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
    })
    .nodeify(callback);
};

/**
 * Indexes a mongoose document in Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {function} [callback]
 * @throws {ModelNotFoundError}
 * @returns {Promise}
 */

Mongoolastic.prototype.indexDoc = function(doc, callback) {

  const self = this;

  return Bluebird.resolve(doc)
    .then((doc) => {

      // This method is called for ALL documents that share the
      // model's SCHEMA. Thus, we need to check first and filter out
      // all documents that share the same schema, but belong to
      // other, non registered models.
      const id = doc.id;
      const modelName = doc.constructor.modelName;
      const registeredModel = self.registeredModels.get(modelName);

      if (!registeredModel) {
        return;
      }

      // Apply transform
      if (registeredModel.transform) {

        return registeredModel.transform(doc)
          .then((transformedDoc) => {
            return [id, transformedDoc, modelName, self.index];
          });
      }

      return [id, doc, modelName, self.index];
    })
    .spread((id, doc, type, index) => {
      return self.es.indexDoc(id, doc, type, index);
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

  const self = this;

  return Bluebird.resolve(doc)
    .then(function(doc) {

      // This method is called for ALL documents that share the
      // model's SCHEMA. Thus we need to check first and filter
      // all documents that share the same schema, but belong to
      // other, non registered models.
      const type = doc.constructor.modelName;
      const isRegistered = self.registeredModels.has(type);

      if (!isRegistered) {
        return;
      }

      const id = doc.id;
      return self.es.deleteDoc(id, type, self.index);
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
 * Searches the Elasticsearch index based on a supplied query
 * Returns a promise or calls callback (if provided)
 * @param {object} query
 * @param {function} [callback]
 * @returns {Promise}
 */

Mongoolastic.prototype.search = function(query, callback) {

  const self = this;

  return Bluebird.resolve(query)
    .then((query) => {
      return self.es.search(query);
    })
    .nodeify(callback);
};

module.exports = new Mongoolastic();
