'use strict';

var util = require('util');
var events = require('events');
var Promise = require('bluebird');
var mongoose = require('mongoose');
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
 * @param {object|function} [settings]
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

      if(!elasticsearch.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if(settings !== undefined && !elasticsearch.isValidSettings(settings)) {
        throw new errors.InvalidArgumentError('invalid-settings');
      }

      if(!(schema instanceof mongoose.Schema)) {
        throw new errors.InvalidArgumentError('invalid-schema');
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

      return [
        doc.id,
        doc.toObject(),
        doc.constructor.modelName,
        index
      ];
    })
    .spread(this.es.indexDoc)
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

      return [
        doc.id,
        doc.constructor.modelName,
        index];
    })
    .spread(this.es.deleteDoc)
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
 * Populates a mongoose object
 * @param {object} doc
 * @param {function} callback
 */
Mongoolastic.prototype.populate = function populate(doc, callback) {

  return Promise.resolve(doc)
    .then(function(doc){
      console.log(doc);
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


module.exports = new Mongoolastic();