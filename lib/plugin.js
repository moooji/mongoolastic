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
  this.indices = [];
  this.es = elasticsearch;
}

Mongoolastic.prototype.InvalidArgumentError = errors.InvalidArgumentError;


/**
 * Connects to Elasticsearch and tests the connection with a ping
 * Returns promise or calls callback (if provided)
 * @param {string|array} hosts
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.connect = function(hosts, callback) {

  const self = this;

  return Bluebird.resolve(hosts)
    .then((hosts) => {
      return self.es.connect(hosts);
    })
    .then(() => {
      return self.ensureIndices();
    })
    .nodeify(callback);
};


/**
 * Registers a schema to be connected to with an Elasticsearch index
 * Returns promise or calls callback (if provided)
 * @param {object} schema
 * @param {string} index
 * @param {object|function} [settings]
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
Mongoolastic.prototype.register = function(schema, index, settings, callback) {

  const self = this;

  return Bluebird.resolve([schema, index])
    .spread((schema, index) => {

      // Check if settings are provided
      // or if the settings argument is actually the callback
      if (callback === undefined && _.isFunction(settings)) {
        callback = settings;
        settings = undefined;
      }

      if(!elasticsearch.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if(settings && !elasticsearch.isValidSettings(settings)) {
        throw new errors.InvalidArgumentError('invalid-settings');
      }

      if(!(schema instanceof mongoose.Schema)) {
        throw new errors.InvalidArgumentError('invalid-schema');
      }

      // TODO: Add check that schema cannot register twice
      self.indices.push({
        name: index,
        settings: settings ? settings : {}
      });

      // Register mongoose hooks
      schema.post('remove', function onRemove(doc) {
        self.remove(doc, index);
      });

      schema.post('save', function onSave(doc) {
        self.index(doc, index);
      });

      // Register schema methods
      schema.methods.search = self.search;

      /*
      schema.statics.sync = function(callback) {
        return elastic.sync(this, options.modelName, callback);
      };

      schema.statics.syncById = function(id, callback) {
        return elastic.syncById(this, options.modelName, id, callback);
      };
      */
    })
    .nodeify(callback);
};


/**
 * Ensures the Elasticsearch indices
 * Returns promise or calls callback (if provided)
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.ensureIndices = function(callback) {

  const self = this;

  return Bluebird.resolve()
    .return(self.indices)
    .each((index) => {

      return self.es.ensureIndex(
        index.name,
        index.settings,
        {}
      );
    })
    .nodeify(callback);
};


/**
 * Indexes a mongoose document in Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.index = function(doc, index, callback) {

  const self = this;

  return Bluebird.resolve(doc)
    .then(self.populate)
    .then((doc) => {

      return self.es.indexDoc(
        doc.id,
        doc.toObject(),
        doc.constructor.modelName,
        index
      );
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
Mongoolastic.prototype.remove = function(doc, index, callback) {

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
Mongoolastic.prototype.populate = function populate(doc, callback) {

  //let self = this;

  return Bluebird.resolve(doc)
    .then((doc) => {

      const tree = doc.schema.tree;

      _.forOwn(tree, (value, key) => {
        if (_.has(value, 'elasticsearch')) {
          console.log(_.set({}, key, value));
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
      const wrapValue = function(key, value) {
        return _.set({}, key, value);
      };


      // Get paths with elasticsearch mapping set in schema
      // and merge all their mappings
      const pathMappings = {};

      _.forOwn(model.schema.paths, function(value, key) {

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