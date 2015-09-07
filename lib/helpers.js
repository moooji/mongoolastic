'use strict';

const _ = require('lodash');
const Bluebird = require('bluebird');
const mongoose = require('mongoose');
const errors = require('./errors');


/**
 * Renders Elasticsearch mapping for a mongoose schema
 * @param {Schema} schema
 * @param {Map<Model>} populationModels
 */
function renderMapping(schema, populationModels) {

  // Validation
  if(!(schema instanceof mongoose.Schema)) {
    throw new errors.InvalidArgumentError('invalid-schema');
  }

  let result = {};

  // Get paths in schema which have Elasticsearch mapping defined
  // and merge all their mappings
  _.forOwn(schema.tree, (pathValue, pathKey) => {

    if(hasMapping(pathValue)) {

      // If path has Elasticsearch mappings set,
      // merge the mappings
      const pathMapping = pathValue.elasticsearch.mapping;
      result = _.merge(result, {[pathKey]: pathMapping});
    }
    else if(isSubDoc(pathValue)) {

      // If path is an embedded sub document schema,
      // merge the mappings
      const subDocMapping = renderMapping(_.first(pathValue), populationModels);
      result = _.merge(result, {[pathKey]: subDocMapping});
    }
    else {

      // If path holds reference to other model,
      // check if it is exists in list of population models
      // and merge the mappings
      let ref = pathValue.ref ? pathValue.ref : null;

      if(!ref &&
        _.isArray(pathValue) &&
        _.has(_.first(pathValue), 'ref')) {
        ref = _.get(_.first(pathValue), 'ref');
      }

      if(ref && populationModels.has(ref)) {

        const model = populationModels.get(ref);
        const populationMapping = renderMapping(model.schema, populationModels);
        result = _.merge(result, {[pathKey]: populationMapping});
      }
    }
  });

  result = !_.isEmpty(result) ? {properties: result} : null;
  return result;
}


/**
 * Renders and populates a mongoose object
 * to be indexed by Elasticsearch
 * @param {object} doc
 * @param {[string]} indexFields
 * @param {[string]} populationFields
 * @param {function} [callback]
 * @returns {object}
 */
function renderDoc(doc, indexFields, populationFields, callback) {

  return Bluebird.resolve(doc)
    .then((doc) => {

      return doc.populate('candy').execPopulate()
        .then((res) => {

          const subDoc = res.get('candy');
          console.log(subDoc);
          return res['candy'].populate('wrappingColor').execPopulate()
            .then((res) => {

              console.log(res);
              console.log(doc);
              return doc.toObject();
            });
        });
    })
    .nodeify(callback);
}

/*

 _.forOwn(doc.schema.tree, (value, key) => {

 if (_.has(value, 'elasticsearch.populate') &&
 _.get(value, 'elasticsearch.populate') === true) {

 if(_.get(value, 'elasticsearch.populate')) {
 console.log(_.set({}, key, value));
 }
 }
 });
 */

/**
 * Check if path value is a sub document
 * with nested schema.
 * @param {*} value
 * @returns {boolean}
 */
function isSubDoc(value) {

  return _.isArray(value) &&
    _.first(value) instanceof mongoose.Schema;
}

/**
 * Checks if a path value has an Elasticsearch
 * mapping defined.
 * @param {*} value
 * @returns {boolean}
 */
function hasMapping(value) {
  return _.has(value, 'elasticsearch.mapping');
}


module.exports.renderMapping = renderMapping;
module.exports.renderDoc = renderDoc;