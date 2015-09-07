'use strict';

const _ = require('lodash');
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

function populateDoc(doc) {
  return doc;
}

module.exports.renderMapping = renderMapping;
module.exports.populateDoc = populateDoc;