'use strict';

var util = require('util');
var events = require('events');
var Promise = require('bluebird');
var _ = require('lodash');
var elasticsearch = require('./elasticsearch');
var errors = require('./errors');


// Inherit from EventEmitter
util.inherits(Mongoolastic, events.EventEmitter);


/**
 * Elasticsearch plugin
 * @constructor
 */
function Mongoolastic() {
  this.indices = [];
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

  return Promise.resolve(hosts)
    .bind(this)
    .then(elasticsearch.connect)
    .nodeify(callback);
};


/**
 * Registers a schema to be connected to with an Elasticsearch index
 * Returns promise or calls callback (if provided)
 * @param {object} schema
 * @param {string} index
 * @param {object} [settings]
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.register = function(schema, index, settings, callback) {

  return Promise.resolve([schema, index])
    .bind(this)
    .spread(function(schema, index) {

      // Check if settings are provided
      // or if the settings argument is actually the callback
      if (callback === undefined && _.isFunction(settings)) {
        callback = settings;
        settings = {};
      }

      // TODO: Add check that schema cannot register twice
      this.indices.push({
        name: index,
        settings: settings
      });

      // Register mongoose hooks
      schema.post('remove', function onRemove(doc) {
        this.remove(doc, index);
      });

      schema.post('save', function onSave(doc) {
        this.index(doc, index);
      });

      // Register schema methods
      schema.methods.search = this.search;

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
 * Validates the plugin options
 * @param {object} options
 * @param {function} [callback]
 */
/*
Mongoolastic.prototype.validateOptions = function(options, callback) {

  return Promise.resolve(options)
    .bind(this)
    .then(function(options) {

      if(!_.isPlainObject(options)) {
        throw new this.InvalidArgumentError('invalid-options');
      }

      // Index name must be supplied in options
      if(!_.isString(options.index)) {
        throw new this.InvalidArgumentError('invalid-index-name');
      }

      // Index settings are optional, but if they are supplied
      // they should be a plain object
      if(options.settings && !_.isPlainObject(options.settings)) {
        throw new this.InvalidArgumentError('invalid-index-settings');
      }

      return options;
    })
    .nodeify(callback);
};
*/

/**
 * Indexes a mongoose document in Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.index = function(doc, index, callback) {

  return Promise.resolve(doc)
    .bind(this)
    .then(this.populate)
    .then(function(doc) {

      var esDoc = this.createElasticsearchDoc(doc);

      return {
        index: index,
        type: esDoc.type,
        id: esDoc.id,
        body: esDoc.body,
        refresh: true
      };
    })
    .then(this.es.index)
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
Mongoolastic.prototype.remove = function(doc, index, callback) {

  return Promise.resolve(doc)
    .bind(this)
    .then(function(doc) {

      var esDoc = this.createElasticsearchDoc(doc);

      return {
        index: index,
        type: esDoc.type,
        id: esDoc.id
      };
    })
    .then(this.es.delete)
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

  return Promise.resolve(query)
    .bind(this)
    .then(this.es.search)
    .nodeify(callback);
};


/**
 * Render the mapping for the model
 * @param {object} model
 * @param {function} [callback]
 */
Mongoolastic.prototype.renderMapping = function(model, callback) {

  return Promise.resolve(model)
    .bind(this)
    .then(function(model) {

      var mapping = {};

      // Helper function that wraps a key / value pair as object
      var wrapValue = function(key, value) {
        return _.set({}, key, value);
      };


      // Get paths with elasticsearch mapping set in schema
      // and merge all their mappings
      var pathMappings = {};

      _.forOwn(model.schema.paths, function(value, key) {

        if(_.has(value, 'options.elastic.mapping')) {
          pathMappings[key] = value.options.elastic.mapping;
        }
      });

      mapping = _.merge(mapping, pathMappings);


      // Elasticsearch requires all nested properties 'sub.subSub'
      // to be wrapped as {sub: {properties: {subSub: {properties: ...}}}}
      var nestedMapping = {};

      _.forOwn(mapping, function(value, key) {

        var nestedKeys = key.split('.');
        var nestedValue = value;

        // Top level
        nestedValue = wrapValue(nestedKeys.pop(), nestedValue);

        // Deeper levels need to be wrapped as 'properties'
        _.forEachRight(nestedKeys, function(nestedKey) {

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


/**
 * Validates and ensures valid type, body and id for a given mongoose doc
 * to be stored in Elasticsearch.
 * @param {object} doc
 * @returns {{type: (string), body: (object), id: (string)}}
 */
Mongoolastic.prototype.createElasticsearchDoc = function(doc) {

  if(!_.isString(doc.id)) {
    throw new this.InvalidArgumentError('invalid-document-id');
  }

  // Resolve model name from document
  var modelName = doc.constructor.modelName;

  if(!_.isString(modelName)) {
    throw new this.InvalidArgumentError('invalid-model-name');
  }

  return {
    type: modelName,
    body: doc.toObject(),
    id: doc.id
  };
};


module.exports = new Mongoolastic();