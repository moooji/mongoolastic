'use strict';

const util = require('util');
const events = require('events');
const _ = require('lodash');
const Bluebird = require('bluebird');
const mongoose = require('mongoose');
const elasticsearch = require('./elasticsearch');
const errors = require('./errors');
const helpers = require('./helpers');

// Inherit from EventEmitter
util.inherits(Mongoolastic, events.EventEmitter);


/**
 * Elasticsearch plugin
 * @constructor
 */
function Mongoolastic() {
  this.index = null;
  this.settings = {};
  this.mappings = {};
  this.schemas = [];
  this.populationModels = new Map();
  this.registeredModels = new Map();
  this.es = elasticsearch;
}

Mongoolastic.prototype.InvalidArgumentError = errors.InvalidArgumentError;


/**
 * Connects to Elasticsearch, ensures the index
 * and tests the connection with a ping.
 * @param {string|array} hosts
 * @param {string} index
 * @param {object|function} [settings]
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.connect = function(hosts, index, settings, callback) {

  const self = this;

  // Check if settings are provided
  // or if the settings argument is actually the callback
  if (callback === undefined && _.isFunction(settings)) {
    callback = settings;
    settings = undefined;
  }

  return Bluebird.resolve([hosts, index, settings])
    .spread((hosts, index, settings) => {

      // Validation
      if(!elasticsearch.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index');
      }

      if(settings && !elasticsearch.isValidSettings(settings)) {
        throw new errors.InvalidArgumentError('invalid-settings');
      }
      else if(settings) {
        self.settings = settings;
      }

      self.index = index;
      return self.es.connect(hosts);
    })
    .then(() => {
      return self.es.ensureIndex(self.index, self.settings, self.mappings);
    })
    .nodeify(callback);
};


/**
 * Registers a model to be connected to with an Elasticsearch index
 * Returns promise or calls callback (if provided)
 * @param {object} model
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
Mongoolastic.prototype.registerModel = function(model, callback) {

  const self = this;

  return Bluebird.resolve(model)
    .then((model) => {

      // Validation
      if(!(model.schema instanceof mongoose.Schema)) {
        throw new errors.InvalidArgumentError('invalid-model');
      }

      // Register models and hooks
      self.registeredModels.set(model.modelName, model);

      model.schema.post('remove', function onRemove(doc) {
        self.removeDoc(doc);
      });

      model.schema.post('save', function onSave(doc) {
        self.indexDoc(doc);
      });

      return self.updateMappings();
    })
    .nodeify(callback);
};


/**
 * Registers a model to be used to populate paths on registered models
 * @param {Model} model
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
Mongoolastic.prototype.registerPopulation = function(model, callback) {

  const self = this;

  return Bluebird.resolve(model)
    .then((model) => {

      // Validation
      if(!(model.schema instanceof mongoose.Schema)) {
        throw new errors.InvalidArgumentError('invalid-model');
      }

      // Register population
      self.populationModels.set(model.modelName, model);

      return self.updateMappings();
    })
    .nodeify(callback);
};


/**
 * Updates the mappings based on registered models and populations
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
Mongoolastic.prototype.updateMappings = function(callback) {

  const self = this;

  return Bluebird.resolve()
    .then(() => {

      let mappings = {};
      self.registeredModels.forEach((model) => {

        const mapping = helpers.renderMapping(model.schema, self.populationModels);

        if(mapping) {
          mappings = _.merge(mappings, {[model.modelName]: mapping});
        }
      });

      self.mappings = mappings;
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
      const type = doc.constructor.modelName;
      const isRegistered = self.registeredModels.has(type);

      if (!isRegistered) {
        return;
      }

      const id = doc.id;
      const esDoc = helpers.renderDoc(doc, [], []);
      return self.es.indexDoc(id, esDoc, type, self.index);
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


/**
 * Returns the Elasticsearch mappings
 * @returns {object}
 */
Mongoolastic.prototype.getMappings = function() {
  return this.mappings;
};


module.exports = new Mongoolastic();