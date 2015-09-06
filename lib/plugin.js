'use strict';

const util = require('util');
const events = require('events');
const Bluebird = require('bluebird');
const mongoose = require('mongoose');
const _ = require('lodash');
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
  this.mappings = {};
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

      // Register model
      const mapping = {};
      self.registeredModels.set(model.modelName, {model, mapping});

      // Register mongoose hooks
      model.schema.post('remove', function onRemove(doc) {
        self.removeDoc(doc);
      });

      model.schema.post('save', function onSave(doc) {
        self.indexDoc(doc);
      });

      // Register mongoose schema methods
      model.schema.methods.search = self.search;

      /*
       model.schema.statics.sync = function(callback) {
       return elastic.sync(this, options.modelName, callback);
       };

       model.schema.statics.syncById = function(id, callback) {
       return elastic.syncById(this, options.modelName, id, callback);
       };
       */
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
      // model's SCHEMA. Thus we need to check first and filter
      // all documents that share the same schema, but belong to
      // other, non registered models.
      const type = doc.constructor.modelName;
      const isRegistered = self.registeredModels.has(type);

      if (isRegistered) {

        return Bluebird.resolve(doc)
          .then((doc) => {

            const id = doc.id;
            const esDoc = doc.toObject();
            return self.es.indexDoc(id, esDoc, type, self.index);
          });
      }
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
/*
Mongoolastic.prototype.removeDoc = function(doc, index, callback) {

  //let self = this;

  return Promise.resolve(doc)
    .then(function(doc) {


      return spread(
        doc.id,
        doc.constructor.modelName,
        index
      );
    })
    .nodeify(callback);
};
*/

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
 * Populates a mongoose object
 * @param {object} doc
 * @param {function} callback
 */
Mongoolastic.prototype.populateDocument = function populate(doc, callback) {

  //const self = this;

  return Bluebird.resolve(doc)
    .then((doc) => {

      const tree = doc.schema.tree;

      _.forOwn(tree, (value, key) => {
        if (_.has(value, 'elasticsearch.populate') &&
            _.get(value, 'elasticsearch.populate') === true) {

          if(_.get(value, 'elasticsearch.populate')) {
            console.log(_.set({}, key, value));
          }
        }
      });

      return doc;
    })
    .nodeify(callback);
};


/**
 * Render the mapping for the model
 * @param {object} model
 * @param {function} [callback]
 */
Mongoolastic.prototype.renderMapping = function(model, callback) {

  return Bluebird.resolve(model)
    .then((model) => {

      let mapping = {};

      // Helper function that wraps a key / value pair as object
      const wrapValue = (key, value) => {
        return _.set({}, key, value);
      };


      // Get paths with elasticsearch mapping set in schema
      // and merge all their mappings
      const pathMappings = {};

      _.forOwn(model.schema.paths, (value, key) => {

        if(_.has(value, 'options.elastic.mapping')) {
          pathMappings[key] = value.options.elastic.mapping;
        }
      });

      mapping = _.merge(mapping, pathMappings);


      // Elasticsearch requires all nested properties 'sub.subSub'
      // to be wrapped as {sub: {properties: {subSub: {properties: ...}}}}
      let nestedMapping = {};

      _.forOwn(mapping, (value, key) => {

        const nestedKeys = key.split('.');
        let nestedValue = value;

        // Top level
        nestedValue = wrapValue(nestedKeys.pop(), nestedValue);

        // Deeper levels need to be wrapped as 'properties'
        _.forEachRight(nestedKeys, (nestedKey) => {

          nestedValue = wrapValue(nestedKey, {
            properties: nestedValue
          });
        });

        nestedMapping = _.merge(nestedMapping, nestedValue);
      });


      // Wrap the result so that it has the form
      // { 'theModelName': { properties: ...}}
      return wrapValue(model.modelName, {
        properties: nestedMapping
      });
    })
    .nodeify(callback);
};


module.exports = new Mongoolastic();