"use strict";

var util = require('util');
var events = require('events');
var Promise = require('bluebird');
var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var indices = require('./indices');

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


/**
 * Connects to Elasticsearch and tests the connection with a ping
 * Returns promise or calls callback (if provided)
 * @param {string} prefix
 * @param {object} options
 * @param {function} [callback]
 * @returns {Promise}
 */
Mongoolastic.prototype.connect = function(prefix, options, callback) {

  return Promise.resolve()
    .bind(this)
    .then(function() {

      if(!this.es) {
        this.es = new elasticsearch.Client(options);
      }

      if(!this.indices) {
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

  if(!(options && _.isString(options.index))) {
    throw new Error("Invalid index name");
  }

  // Register mongoose hooks
  schema.post('remove', function onRemove(doc){
    this.remove(doc);
  });

  schema.post('save', function onSave(doc) {
    this.index(doc, options.index);
  });


  /**
   * Search on current model with predefined index
   * @param query
   * @param cb
   */
  schema.methods.search = function(query, cb) {
    query.index = elastic.getIndexName(options.modelName);
    elastic.search(query, cb);
  };

  schema.statics.sync = function(callback) {
    return elastic.sync(this, options.modelName, callback);
  };

  schema.statics.syncById = function(id, callback) {
    return elastic.syncById(this, options.modelName, id, callback);
  };
};

/**
 * Indexes a mongoose document in Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {string} [indexName]
 * @returns {Promise}
 */
Mongoolastic.prototype.index = function(doc, indexName) {

  return Promise.resolve(doc)
    .bind(this)
    .then(this.populate)
    .then(function(doc) {

      if(!_.isString(doc.id)) {
        throw new Error('Invalid document id');
      }

      // If optional index name has been provided,
      // check that it is valid
      if(indexName && !_.isString(indexName)) {
        throw new Error('Invalid index name');
      }

      // Resolve model name from document
      var modelName = doc.constructor.modelName;

      if(!_.isString(modelName)) {
        throw new Error('Invalid model name');
      }

      // If no (optional) index name has been provided,
      // use the model name as elasticsearch index name
      indexName = indexName ? indexName : modelName;

      return {
        index: indexName,
        type: modelName,
        id: doc.id,
        body: doc.toObject(),
        refresh: true
      }
    })
    .then(this.es.index)
    .catch(this.emit);
};

/**
 * Removes a mongoose object from Elasticsearch
 * Returns a promise or calls callback (if provided)
 * @param {object} doc
 * @param {object} options
 * @returns {Promise}
 */
Mongoolastic.prototype.remove = function(doc, options) {

  elastic.delete(options.modelName, this.id, function(err) {
    if(err) {
      // TODO: Emit error event
      console.error(err);
    }
  });
};

module.exports = Mongoolastic;