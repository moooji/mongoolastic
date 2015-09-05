"use strict";

var util = require('util');
var events = require('events');
var Promise = require('bluebird');
var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var indices = require('./indices');
var errors = require('./errors');


// Inherit from EventEmitter
util.inherits(Mongoolastic, events.EventEmitter);


/**
 * Elasticsearch plugin
 * @constructor
 */
function Mongoolastic() {

  this.es = null;
  this.indices = null;
}

Mongoolastic.prototype.InvalidArgumentError = errors.InvalidArgumentError;


/**
 * Connects to Elasticsearch and tests the connection with a ping
 * Returns promise or calls callback (if provided)
 * @param {string} prefix
 * @param {object} options
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.connect = function (prefix, options, callback) {

  return Promise.resolve()
    .bind(this)
    .then(function () {

      if (!this.es) {
        this.es = new elasticsearch.Client(options);
      }

      if (!this.indices) {
        this.indices = new indices(this);
      }

      return this.es.ping({
        requestTimeout: 1000,
        hello: 'mongoolastic'
      });
    })
    .nodeify(callback);
};


/**
 * Mongoose model plugin
 * @param {object} schema
 * @param {object} options
 */
Mongoolastic.prototype.plugin = function plugin(schema, options) {

  return Promise.resolve(options)
    .bind(this)
    .then(this.validateOptions)
    // TODO apply / ensure custom settings
    .then(this.ensureElasticsearchIndex)
    .then(function() {

      // Register mongoose hooks
      schema.post('remove', function onRemove(doc) {
        this.remove(doc);
      });

      schema.post('save', function onSave(doc) {
        this.index(doc, options.index);
      });

      // Register schema methods
      schema.methods.search = this.search;

      schema.statics.sync = function (callback) {
        return elastic.sync(this, options.modelName, callback);
      };

      schema.statics.syncById = function (id, callback) {
        return elastic.syncById(this, options.modelName, id, callback);
      };
    });
};


/**
 * Validates the plugin options
 * @param {object} options
 * @param {function} [callback]
 */
Mongoolastic.prototype.validateOptions = function(options, callback) {

  return Promise.resolve(options)
    .bind(this)
    .then(function(options) {

      if (!options) {
        return {};
      }

      if (options && !_.isPlainObject(options)) {
        throw new this.Error("Invalid options");
      }

      if (options.index && !_.isString(options.index)) {
        throw new this.Error("Invalid index name");
      }

      if (options.settings && !_.isPlainObject(options.settings)) {
        throw new this.Error("Invalid index settings");
      }

      return options;
    })
    .nodeify(callback);
};

/**
 * Indexes a mongoose document in Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {string} [indexName]
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.index = function (doc, indexName, callback) {

  return Promise.resolve(doc)
    .bind(this)
    .then(this.populate)
    .then(function (doc) {

      var esData = this.ensureElasticsearchDoc(doc, indexName);

      return {
        index: esData.index,
        type: esData.type,
        id: esData.id,
        body: esData.doc,
        refresh: true
      }
    })
    .then(this.es.index)
    .nodeify(callback);
};


/**
 * Removes a mongoose object from Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {string} indexName
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.remove = function (doc, indexName, callback) {

  return Promise.resolve(doc)
    .bind(this)
    .then(function (doc) {

      var esData = this.ensureElasticsearchDoc(doc, indexName);

      return {
        index: esData.index,
        type: esData.type,
        id: esData.id
      }
    })
    .then(this.es.delete)
    .nodeify(callback);
};


/**
 * Deletes whole Elasticsearch index
 * @param {string} indexName
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.deleteIndex = function(indexName, callback) {

  return Promise.resolve(indexName)
    .bind(this)
    .then(function(indexName) {

      if(!_.isString(indexName)) {
        throw new this.InvalidArgumentError("Invalid index name");
      }

      return {
        index: indexName
      };
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
Mongoolastic.prototype.search = function (query, callback) {

  return Promise.resolve(query)
    .bind(this)
    .then(this.ensureElasticsearchQuery)
    .then(this.es.search)
    .nodeify(callback);
};


/**
 * Render the mapping for the model
 * @param {object} model
 * @param {function} [callback]
 */
Mongoolastic.prototype.renderMapping = function (model, callback) {

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

      _.forOwn(model.schema.paths, function (value, key) {

        if (_.has(value, 'options.elastic.mapping')) {
          pathMappings[key] = value.options.elastic.mapping;
        }
      });

      mapping = _.merge(mapping, pathMappings);


      // Elasticsearch requires all nested properties 'sub.subSub'
      // to be wrapped as {sub: {properties: {subSub: {properties: ...}}}}
      var nestedMapping = {};

      _.forOwn(mapping, function (value, key) {

        var nestedKeys = key.split('.');
        var nestedValue = value;

        // Top level
        nestedValue = wrapValue(nestedKeys.pop(), nestedValue);

        // Deeper levels need to be wrapped as 'properties'
        _.forEachRight(nestedKeys, function (nestedKey) {

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
 * Validates and ensures index, type and id for a given doc
 * to be stored in Elasticsearch. If no index name is supplied (optional),
 * the model name will be used as index name.
 * @param {object} doc
 * @param {string} [indexName]
 * @returns {{index: (string), type: (string), doc: (object), id: (string)}}
 */
Mongoolastic.prototype.ensureElasticsearchDoc = function(doc, indexName) {

  if (!_.isString(doc.id)) {
    throw new this.InvalidArgumentError('Invalid document id');
  }

  // If optional index name has been provided,
  // check that it is valid
  if (indexName && !_.isString(indexName)) {
    throw new Error('Invalid index name');
  }

  // Resolve model name from document
  var modelName = doc.constructor.modelName;

  if (!_.isString(modelName)) {
    throw new Error('Invalid model name');
  }

  // If no (optional) index name has been provided,
  // use the model name as elasticsearch index name
  indexName = indexName ? indexName : modelName.toLowerCase();

  return {
    index: indexName,
    type: modelName,
    doc: doc.toObject(),
    id: doc.id
  }
};


/**
 * Validates and ensures an Elasticsearch query
 * @param {object} query
 * @returns {object}
 */
Mongoolastic.prototype.ensureElasticsearchQuery = function(query) {

  if(!_.isPlainObject(query)) {
    throw new this.InvalidArgumentError('Invalid query');
  }

  return query;
};


/**
 * Ensures that an index with given name exists in Elasticsearch
 * If the index does not exist, it will be created.
 * Optionally, index settings can be supplied which
 * will be passed to Elasticsearch on index creation.
 * @type {Mongoolastic}
 */
Mongoolastic.prototype.ensureElasticsearchIndex = function(options, callback) {

  return Promise.resolve(indexName)
    .bind(this)
    .then(function(indexName) {
      elastic.indices.checkCreateByModel(model, options,
        function(err) {
          return callback(err, model);
        }
      );
    })
    .nodeify(callback)
};


module.exports = Mongoolastic;