'use strict';

const util = require('util');
const EventEmitter = require('events').EventEmitter;
const Bluebird = require('bluebird');
const elasticsearch = require('elasticsearch');
const _ = require('lodash');
const errors = require('./errors');

util.inherits(ElasticsearchProvider, EventEmitter);

/**
 * Elasticsearch provider
 * @param {object} [options]
 * @constructor
 */

function ElasticsearchProvider(options) {

  if (options && !_.isPlainObject(options)) {
    throw new errors.InvalidArgumentError('invalid-options');
  }

  if (options && options.bulkSize && !_.isFinite(options.bulkSize)) {
    throw new errors.InvalidArgumentError('invalid-bulk-size');
  }

  if (options && options.bulkTimeout && !_.isFinite(options.bulkTimeout)) {
    throw new errors.InvalidArgumentError('invalid-bulk-timeout');
  }

  this.bulkBuffer = [];
  this.bulkTimer = null;
  this.bulkSize = options && options.bulkSize ? options.bulkSize : 100;
  this.bulkTimeout = options && options.bulkTimeout ? options.bulkTimeout : 10000;
  this.isFlushingBulkBuffer = false;
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

  return Bluebird.resolve()
    .then(() => {

      if (!this.client) {
        this.client = new elasticsearch.Client({hosts});
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
 * @param {boolean} useBulk
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */

ElasticsearchProvider.prototype.indexDoc = function(id, doc, type, index, useBulk, callback) {

  return Bluebird.resolve([id, doc, type, index])
    .spread((id, doc, type, index) => {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if (useBulk) {

        const action = {index: {_index: index, _type: type, _id: id}};
        return this.addToBulkBuffer(action, doc);

      } else {

        return this.client.index({
          index,
          type,
          id,
          body: doc,
          refresh: true
        });
      }
    })
    .nodeify(callback);
};

/**
 * Flushes the buffer and performs bulk insert/delete
 * Because of it's decoupled nature, errors are emitted as events
 */

ElasticsearchProvider.prototype.flushBulkBuffer = function() {

  if (this.bulkTimer) {
    clearTimeout(this.bulkTimer);
    this.bulkTimer = null;
  }

  if (this.isFlushingBulkBuffer) {
    return;
  }

  console.log('flushing %d', this.bulkBuffer.length);
  this.isFlushingBulkBuffer = true;

  const body = [];
  let operation = this.bulkBuffer.shift();
  let operationCount = 0;

  while (operation) {

    body.push(operation.action);
    body.push(operation.doc);
    operationCount++;

    if (operationCount < this.bulkSize) {
      operation = this.bulkBuffer.shift();
    } else {
      operation = null;
    }
  }

  // Perform elasticsearch bulk operation
  this.client.bulk({body, refresh: true}, (err) => {

    if (err) {
      this.emit(err);
    }

    this.isFlushingBulkBuffer = false;
    this.tryFlushBulkBuffer();
  });
};

ElasticsearchProvider.prototype.tryFlushBulkBuffer = function() {

  if (!this.bulkBuffer.length) {
    return;
  }

  if (this.bulkBuffer.length >= this.bulkSize) {
    this.flushBulkBuffer();
  } else if (!this.bulkTimer) {
    this.bulkTimer = setTimeout(this.flushBulkBuffer.bind(this), this.bulkTimeout);
  }
};

/**
 * Adds an action and document to the bulk buffer
 * @param {object} action
 * @param {object} doc
 * @param {function} [callback]
 */

ElasticsearchProvider.prototype.addToBulkBuffer = function(action, doc, callback) {

  return Bluebird.resolve([action, doc])
    .spread((action, doc) => {

      this.bulkBuffer.push({action, doc});
      this.tryFlushBulkBuffer();
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

  return Bluebird.resolve([id, type, index])
    .spread((id, type, index) => {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.delete({index, type, id});
    })
    .catch((err) => {

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
 * @throws {DocumentNotFoundError}
 * @returns {Promise}
 */

ElasticsearchProvider.prototype.getDoc = function(id, type, index, callback) {

  return Bluebird.resolve([id, type, index])
    .spread((id, type, index) => {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.get({index, type, id});
    })
    .catch((err) => {

      if (err.status === 404) {
        throw new errors.DocumentNotFoundError(err);
      }

      throw err;
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

  return Bluebird.resolve([id, type, index])
    .spread((id, type, index) => {

      if (!this.isValidId(id)) {
        throw new errors.InvalidArgumentError('invalid-id');
      }

      if (!this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.exists({index, type, id});
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

  return Bluebird.resolve(query)
    .then((query) => {
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
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */

ElasticsearchProvider.prototype.ensureIndex = function(index, settings, mappings, callback) {

  return Bluebird.resolve([index, settings, mappings])
    .spread((index, settings, mappings) => {

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if (!this.isValidSettings(settings)) {
        throw new errors.InvalidArgumentError('invalid-settings');
      }

      if (!this.isValidMapping(mappings)) {
        throw new errors.InvalidArgumentError('invalid-mapping');
      }

      return this.indexExists(index);
    })
    .then((indexExists) => {

      if (!indexExists) {
        const body = {settings, mappings};
        return this.client.indices.create({index, body});
      }
    })
    .nodeify(callback);
};

/**
 * Deletes an Elasticsearch index
 * @param {string|Array<string>} index
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @throws {IndexNotFoundError}
 * @throws {IndexOperationError}
 * @returns {Promise}
 */

ElasticsearchProvider.prototype.deleteIndex = function(index, callback) {

  return Bluebird.resolve(index)
    .then((index) => {

      if (!this.isValidIndex(index, true)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.indices.delete({index})
        .then((res) => {

          if (res.acknowledge === false) {
            throw new errors.IndexOperationError('delete-not-acknowledged', res);
          }
        });
    })
    .catch((err) => {

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
 * @param {string|Array<string>} indices
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */

ElasticsearchProvider.prototype.ensureDeleteIndex = function(indices, callback) {

  return Bluebird.resolve(indices)
    .then((indices) => {

      if (!this.isValidIndex(indices, true)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return _.isArray(indices) ? indices : [indices];
    })
    .map((index) => {

      // Check existence for all supplied indices
      return this.indexExists(index)
        .then((indexExists) => {
          return indexExists ? index : null;
        });
    })
    .then((res) => {

      // Filter out non existing indices
      const indicesToDelete = _.reject(res, _.isNull);

      // Check if there are any indices to delete
      if (!indicesToDelete.length) {
        return indicesToDelete;
      }

      // Delete the indices
      return this.deleteIndex(indicesToDelete)
        .return(indicesToDelete);
    })
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

  return Bluebird.resolve(index)
    .then((index) => {

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.indices.getSettings({index});
    })
    .catch((err) => {

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

  return Bluebird.resolve([index, type])
    .spread((index, type) => {

      if (!this.isValidIndex(index)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      if (type && !this.isValidType(type)) {
        throw new errors.InvalidArgumentError('invalid-type');
      }

      return this.client.indices.getMapping({index, type});
    })
    .catch((err) => {

      if (err.status === 404) {
        throw new errors.IndexNotFoundError(err);
      }

      throw err;
    })
    .nodeify(callback);
};

/**
 * Checks if an index exists.
 * @param {string|Array<string>} index
 * @param {function} [callback]
 * @throws {InvalidArgumentError}
 * @returns {Promise}
 */

ElasticsearchProvider.prototype.indexExists = function(index, callback) {

  return Bluebird.resolve(index)
    .then((index) => {

      if (!this.isValidIndex(index, true)) {
        throw new errors.InvalidArgumentError('invalid-index-name');
      }

      return this.client.indices.exists({index});
    })
    .nodeify(callback);
};

/**
 * Checks if a supplied index name is valid.
 * @param {string|Array<string>} index
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

ElasticsearchProvider.prototype.isValidSettings = (settings) => {
  return _.isPlainObject(settings);
};

/**
 * Checks if supplied mapping is valid.
 * @param {object} mapping
 * @returns {boolean}
 */

ElasticsearchProvider.prototype.isValidMapping = (mapping) => {
  return _.isPlainObject(mapping);
};

/**
 * Checks if supplied type is valid.
 * @param {object} type
 * @returns {boolean}
 */

ElasticsearchProvider.prototype.isValidType = (type) => {
  return _.isString(type);
};

/**
 * Checks if supplied document id is valid.
 * @param {string} id
 * @returns {boolean}
 */

ElasticsearchProvider.prototype.isValidId = (id) => {
  return _.isString(id);
};

/**
 * Factory function that returns new ElasticsearchProvider
 * @returns {ElasticsearchProvider}
 */

function create(options) {
  return new ElasticsearchProvider(options);
}

module.exports.create = create;
