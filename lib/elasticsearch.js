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
ElasticsearchProvider.prototype.index = function(doc, index, callback) {

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
ElasticsearchProvider.prototype.remove = function(doc, index, callback) {

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
ElasticsearchProvider.prototype.search = function(query, callback) {

  return Promise.resolve(query)
    .bind(this)
    .then(this.ensureElasticsearchQuery)
    .then(this.es.search)
    .nodeify(callback);
};


/**
 * Validates and ensures an Elasticsearch query
 * @param {object} query
 * @returns {object}
 */
ElasticsearchProvider.prototype.ensureElasticsearchQuery = function(query) {

  if(!_.isPlainObject(query)) {
    throw new this.InvalidArgumentError('invalid-query');
  }

  return query;
};


/**
 * Ensures that an index with given name exists in Elasticsearch.
 * If the index does not exist, it will be created.
 * Optionally, index settings can be supplied which
 * will be passed to Elasticsearch on index creation.
 * @param {string} index
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.ensureIndex = function(index, callback) {

  return Promise.resolve(index)
    .bind(this)
    .then(this.validateIndexName)
    .then(this.indexExists)
    .then(function(indexExists) {

      // Create index if it does not exist
      if (!indexExists) {
        return this.client.indices.create({
          index: index
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
    .then(function(index) {

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
ElasticsearchProvider.prototype.isValidIndexName = function(index) {

  return _.isString(index) &&
    index.toLowerCase() === index;
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

      // Validate the index name
      if (!this.isValidIndexName(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return index;
    })
    .nodeify(callback);
};

/**
 * Ensures that a mapping exists in an Elasticsearch index for a given type.
 * If the mapping does not exist, it will be created, otherwise it will be
 * attempted to merged it with the existing one.
 * @param {string} index
 * @param {string} type
 * @param {object} mapping
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.ensureMapping = function(index, type, mapping, callback) {

  return Promise.resolve([index, type, mapping])
    .bind(this)
    .spread(function(index, type, mapping) {

      if(!_.isString(index)) {
        throw new this.InvalidArgumentError('Invalid model name');
      }

      if(!_.isString(type)) {
        throw new this.InvalidArgumentError('Invalid model name');
      }

      if(!_.isPlainObject(mapping)) {
        throw new this.InvalidArgumentError('Invalid model name');
      }
    })
    .return({
      index: index,
      type: type,
      body: mapping
    })
    .then(this.es.indices.exists)
    .then(function(indexExists) {

      // If the index does not exists,
      // create it with rendered mapping and supplied settings
      if(!indexExists) {

        return this.es.indices.create({
          index: indexName,
          body: {
            settings: settings,
            mappings: mappings
          }});
      }

      // If the index exists already,
      // ensure the rendered mapping and supplied index settings
      return this.es.indices.putSettings({
        index: indexName,
        body: settings
      })
        .then(function() {
          return
        })
    })


    .nodeify(callback)
};


/**
 * Ensures that an index with given name exists in Elasticsearch
 * If the index does not exist, it will be created.
 * Optionally, index settings can be supplied which
 * will be passed to Elasticsearch on index creation.
 * @type {Mongoolastic}
 */
ElasticsearchProvider.prototype.ensureSettings = function(options, callback) {

  return Promise.resolve(options)
    .bind(this)
    .return({
      index: options.index,
      settings: _.isPlainObject(options.settings) ? options.settings : {},
      mappings: {}
    })
    .then(this.es.indices.exists)
    .then(function(indexExists) {

      // If the index does not exists,
      // create it with rendered mapping and supplied settings
      if(!indexExists) {

        return this.es.indices.create({
          index: indexName,
          body: {
            settings: settings,
            mappings: mappings
          }});
      }

      // If the index exists already,
      // ensure the rendered mapping and supplied index settings
      return this.es.indices.putSettings({
        index: indexName,
        body: settings
      })
        .then(function() {
          return
        })
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

      // Delete the index
      return this.client.indices.delete({
        index: index
      });
    })
    .nodeify(callback);
};

module.exports = new ElasticsearchProvider();