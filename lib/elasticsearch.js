'use strict';

var Promise = require('bluebird');
var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var errors = require('./errors');


/**
 * Elasticsearch provider
 * @constructor
 */
function ElasticsearchProvider() {

  this.requestTimeout = 1000;
  this.client = null;
}


/**
 * Connects to Elasticsearch and tests the connection with a ping
 * Returns promise or calls callback (if provided)
 * @param {object} options
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.connect = function(options, callback) {

  return Promise.resolve()
    .bind(this)
    .then(function() {

      if(!this.client) {
        this.client = new elasticsearch.Client(options);
      }

      return this.client.ping({
        requestTimeout: this.requestTimeout,
        hello: 'mongoolastic'
      });
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
ElasticsearchProvider.prototype.indexDoc = function(doc, index, callback) {

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
      }
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
ElasticsearchProvider.prototype.deleteDoc = function(doc, index, callback) {

  return Promise.resolve(doc)
    .bind(this)
    .then(function(doc) {

      var esDoc = this.createElasticsearchDoc(doc);

      return {
        index: index,
        type: esDoc.type,
        id: esDoc.id
      }
    })
    .then(this.client.delete)
    .nodeify(callback);
};


/**
 * Searches the Elasticsearch index based on a supplied query
 * Returns a promise or calls callback (if provided)
 * @param {object} query
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.search = function(query, callback) {

  return Promise.resolve(query)
    .bind(this)
    .then(this.client.search)
    .nodeify(callback);
};


/**
 * Ensures that an index with given name exists in Elasticsearch.
 * If the index does not exist, it will be created.
 * Optionally, index settings and mapping can be supplied which
 * will be passed to Elasticsearch on index creation.
 * @param {string} index
 * @param {object} settings
 * @param {object} mappings
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.ensureIndex = function(index, settings, mappings, callback) {

  return Promise.resolve([index, settings, mappings])
    .bind(this)
    .spread(function(index, settings, mappings){

      if (!this.isValidIndexName(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if(!this.isValidSettings(settings)) {
        throw new errors.InvalidArgumentError('invalid-settings');
      }

      if(!this.isValidMappings(mappings)) {
        throw new errors.InvalidArgumentError('invalid-mappings');
      }

      return index;
    })
    .then(this.indexExists)
    .then(function(indexExists) {

      // Create index if it does not exist
      if (!indexExists) {
        return this.client.indices.create({
          index: index,
          body: {
            settings: settings,
            mappings: mappings
          }
        });
      }
    })
    .nodeify(callback);
};


/**
 * Checks if an index exists.
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.indexExists = function(index, callback) {

  return Promise.resolve(index)
    .bind(this)
    .then(this.validateIndexName)
    .then(function(index) {

      // Check if index exists
      return this.client.indices.exists({
        index: index
      });
    })
    .nodeify(callback);
};


/**
 * Gets the settings for an index.
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.getIndexSettings = function(index, callback) {

  return Promise.resolve(index)
    .bind(this)
    .then(this.validateIndexName)
    .then(function(index) {

      return this.client.indices.getSettings({
        index: index
      });
    })
    .nodeify(callback)
};


/**
 * Gets the mappings for an index.
 * @param {string} index
 * @param {string} type
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.getIndexMapping = function(index, type, callback) {

  return Promise.resolve([index, type])
    .bind(this)
    .spread(function(index, type) {

      if (!this.isValidIndexName(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if (type && !this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      return this.client.indices.getMapping({
        index: index,
        type: type
      });
    })
    .nodeify(callback)
};


/**
 * Deletes an Elasticsearch index
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.deleteIndex = function(index, callback) {

  return Promise.resolve(index)
    .bind(this)
    .then(this.validateIndexName)
    .then(function(index) {

      return this.client.indices.delete({
        index: index
      });
    })
    .nodeify(callback);
};


/**
 * Checks if a supplied index name is valid.
 * @param {string} index
 * @returns {boolean}
 */
ElasticsearchProvider.prototype.isValidIndexName = function(index) {

  return _.isString(index) &&
    index.toLowerCase() === index;
};


/**
 * Checks if supplied settings is valid.
 * @param {object} settings
 * @returns {boolean}
 */
ElasticsearchProvider.prototype.isValidSettings = function(settings) {
  return _.isPlainObject(settings);
};


/**
 * Checks if supplied settings is valid.
 * @param {object} mappings
 * @returns {boolean}
 */
ElasticsearchProvider.prototype.isValidMappings = function(mappings) {
  return _.isPlainObject(mappings);
};


/**
 * Checks if supplied type is valid.
 * @param {object} type
 * @returns {boolean}
 */
ElasticsearchProvider.prototype.isValidType = function(type) {
  return _.isString(type);
};


/**
 * Validates a supplied index name. Throws InvalidArgumentError.
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.validateIndexName = function(index, callback) {

  return Promise.resolve(index)
    .bind(this)
    .then(function(index) {

      if (!this.isValidIndexName(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return index;
    })
    .nodeify(callback);
};

module.exports = new ElasticsearchProvider();