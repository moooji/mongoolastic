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
 * @param {string|array} hosts
 * @param {function} [callback]
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.connect = function(hosts, callback) {

  var self = this;

  return Promise.resolve()
    .then(function() {

      if(!self.client) {
        self.client = new elasticsearch.Client({
          hosts: hosts
        });
      }

      return self.client.ping({
        requestTimeout: self.requestTimeout,
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
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.indexDoc = function(id, doc, type, index, callback) {

  var self = this;

  return Promise.resolve([id, doc, type, index])
    .spread(function(id, doc, type, index) {

      if (!self.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!self.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!self.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return self.client.index({
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
 * Returns a promise or optionally calls callback (if provided)
 * @param {string} id
 * @param {string} type
 * @param {string} index
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @throws {DocumentNotFoundError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.deleteDoc = function(id, type, index, callback) {

  var self = this;

  return Promise.resolve([id, type, index])
    .spread(function(id, type, index) {

      if (!self.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!self.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!self.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return self.client.delete({
        index: index,
        type: type,
        id: id
      });
    })
    .catch(function(err) {

      if (err.status === 404) {
        throw new errors.DocumentNotFoundError(err);
      }

      throw err;
    })
    .nodeify(callback);
};


/**
 * Gets a document from Elasticsearch
 * Returns a promise or optionally calls callback (if provided)
 * @param {string} id
 * @param {string} type
 * @param {string} index
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.getDoc = function(id, type, index, callback) {

  var self = this;

  return Promise.resolve([id, type, index])
    .spread(function(id, type, index) {

      if (!self.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!self.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!self.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return self.client.get({
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
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.docExists = function(id, type, index, callback) {

  var self = this;

  return Promise.resolve([id, type, index])
    .bind(this)
    .spread(function(id, type, index) {

      if (!self.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!self.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!self.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return self.client.exists({
        index: index,
        type: type,
        id: id
      });
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

  var self = this;

  return Promise.resolve(query)
    .then(function(query) {
      return self.client.search(query);
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
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.ensureIndex = function(index, settings, mappings, callback) {

  var self = this;

  return Promise.resolve([index, settings, mappings])
    .spread(function(index, settings, mappings) {

      if (!self.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if(!self.isValidSettings(settings)) {
        throw new errors.InvalidArgumentError('invalid-settings');
      }

      if(!self.isValidMappings(mappings)) {
        throw new errors.InvalidArgumentError('invalid-mappings');
      }

      return self.indexExists(index);
    })
    .then(function(indexExists) {

      if (!indexExists) {
        return self.client.indices.create({
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
 * @param {string|[string]} index
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @throws {IndexNotFoundError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.deleteIndex = function(index, callback) {

  var self = this;

  return Promise.resolve(index)
    .then(function(index) {

      if (!self.isValidIndex(index, true)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return self.client.indices.delete({
        index: index
      });
    })
    .catch(function(err) {

      if (err.status === 404) {
        throw new errors.IndexNotFoundError(err);
      }

      throw err;
    })
    .nodeify(callback);
};


/**
 * Ensures the deletion of one or several indices.
 * Only attempts deletions on actually existing indices,
 * thus does NOT throw IndexNotFoundError in case that
 * non existing indices have been supplied in arguments.
 * @param {string|[string]} indices
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.ensureDeleteIndex = function(indices, callback) {

  var self = this;

  return Promise.resolve(indices)
    .then(function(indices) {

      if (!self.isValidIndex(indices, true)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return _.isArray(indices) ? indices : [indices];
    })
    .map(function(index) {

      // Check existence for all supplied indices
      return self.indexExists(index)
        .then(function(indexExists) {
          return indexExists ? index : null;
        });
    })
    .then(function(res) {

      // Filter out non existing indices
      var indicesToDelete = _.reject(res, _.isNull);

      // Check if there are any indices to delete
      if(!indicesToDelete.length) {
        return indicesToDelete;
      }

      // Delete the indices
      return self.deleteIndex(indicesToDelete)
        .then(function(res) {
          console.log(res);
          return indicesToDelete;
        });
    })
    .tap(console.log)
    .nodeify(callback);
};


/**
 * Gets the settings for an index.
 * @param {string} index
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @throws {IndexNotFoundError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.getIndexSettings = function(index, callback) {

  var self = this;

  return Promise.resolve(index)
    .then(function(index) {

      if (!self.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return self.client.indices.getSettings({
        index: index
      });
    })
    .catch(function(err) {

      if (err.status === 404) {
        throw new errors.IndexNotFoundError(err);
      }

      throw err;
    })
    .nodeify(callback);
};


/**
 * Gets the mappings for an index.
 * @param {string} index
 * @param {string} type
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @throws {IndexNotFoundError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.getIndexMapping = function(index, type, callback) {

  var self = this;

  return Promise.resolve([index, type])
    .spread(function(index, type) {

      if (!self.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if (type && !self.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      return self.client.indices.getMapping({
        index: index,
        type: type
      });
    })
    .catch(function(err) {

      if (err.status === 404) {
        throw new errors.IndexNotFoundError(err);
      }

      throw err;
    })
    .nodeify(callback);
};


/**
 * Checks if an index exists.
 * @param {string|[string]} index
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */
ElasticsearchProvider.prototype.indexExists = function(index, callback) {

  var self = this;

  return Promise.resolve(index)
    .then(function(index) {

      if (!self.isValidIndex(index, true)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return self.client.indices.exists({
        index: index
      });
    })
    .nodeify(callback);
};


/**
 * Checks if a supplied index name is valid.
 * @param {string|[string]} index
 * @param {boolean} [allowList]
 * @returns {boolean}
 */
ElasticsearchProvider.prototype.isValidIndex = function(index, allowList) {

  function isValid(index) {
    return _.isString(index) &&
      index.toLowerCase() === index;
  }

  if (_.isArray(index) && allowList) {
    return _.every(index, isValid);
  }

  return isValid(index);
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