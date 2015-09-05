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
 * @param {string} id
 * @param {object} doc
 * @param {string} type
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.indexDoc = function(id, doc, type, index, callback) {

  return Promise.resolve([id, doc, type, index])
    .bind(this)
    .spread(function(id, doc, type, index) {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.index({
        index: index,
        type: type,
        id: id,
        body: doc,
        refresh: true
      });
    })
    .nodeify(callback);
};


/**
 * Removes a document from Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {string} id
 * @param {string} type
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.deleteDoc = function(id, type, index, callback) {

  return Promise.resolve([id, type, index])
    .bind(this)
    .spread(function(id, type, index) {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.delete({
        index: index,
        type: type,
        id: id
      });
    })
    .nodeify(callback);
};


/**
 * Gets a document from Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {string} id
 * @param {string} type
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.getDoc = function(id, type, index, callback) {

  return Promise.resolve([id, type, index])
    .bind(this)
    .spread(function(id, type, index) {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.get({
        index: index,
        type: type,
        id: id
      });
    })
    .nodeify(callback);
};


/**
 * Check if a document with given id and type exists in index
 * Returns a promise or calls callback (if provided)
 * @param {string} id
 * @param {string} type
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.docExists = function(id, type, index, callback) {

  return Promise.resolve([id, type, index])
    .bind(this)
    .spread(function(id, type, index) {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.exists({
        index: index,
        type: type,
        id: id
      });
    })
    .catch(function(err){
      console.log(err);
      throw err;
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
ElasticsearchProvider.prototype.search = function(query, callback) {

  return Promise.resolve(query)
    .bind(this)
    .then(function(query) {
      return this.client.search(query);
    })
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

      if (!this.isValidIndex(index)) {
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
 * Deletes an Elasticsearch index
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.deleteIndex = function(index, callback) {

  return Promise.resolve(index)
    .bind(this)
    .then(function(index) {

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.indices.delete({
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
    .then(function(index) {

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

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

      if (!this.isValidIndex(index)) {
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
 * Checks if an index exists.
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.indexExists = function(index, callback) {

  return Promise.resolve(index)
    .bind(this)
    .then(function(index) {

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.indices.exists({
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
ElasticsearchProvider.prototype.isValidIndex = function(index) {

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
 * Checks if supplied document id is valid.
 * @param {string} id
 * @returns {boolean}
 */
ElasticsearchProvider.prototype.isValidId = function(id) {
  return _.isString(id);
};


module.exports = new ElasticsearchProvider();