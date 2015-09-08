'use strict';

const _ = require('lodash');
const Bluebird = require('bluebird');
const mongoose = require('mongoose');
const errors = require('./errors');


/**
 * Parses a mongoose schema to be indexed in Elasticsearch
 * @param {Schema} schema
 * @param {Map<Model>} populationModels
 */
function parseSchema(schema, populationModels) {

  let populationTree = {};

  // Validation
  if(!(schema instanceof mongoose.Schema)) {
    throw new errors.InvalidArgumentError('invalid-schema');
  }

  let mapping = {};

  // Get paths in schema which have Elasticsearch mapping defined
  // and merge all their mappings
  _.forOwn(schema.tree, (pathValue, pathKey) => {

    if(hasMapping(pathValue)) {

      // If path has Elasticsearch mappings set,
      // merge the mappings
      const pathMapping = pathValue.elasticsearch.mapping;
      mapping = _.merge(mapping, {[pathKey]: pathMapping});
    }
    else if(isSubDoc(pathValue)) {

      // If path is an embedded sub document schema,
      // merge the mappings
      const subDoc = parseSchema(_.first(pathValue), populationModels);
      mapping = _.merge(mapping, {[pathKey]: subDoc.mapping});
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
        const population = parseSchema(model.schema, populationModels);

        populationTree[pathKey] = {
          fields: _.keys(population.mapping.properties),
        };

        if(!_.isEmpty(population.populationTree)) {
          populationTree[pathKey].paths = population.populationTree;
        }

        mapping = _.merge(mapping, {[pathKey]: population.mapping});
      }
    }
  });

  mapping = !_.isEmpty(mapping) ? {properties: mapping} : null;
  populationTree = !_.isEmpty(populationTree) ? populationTree : null;

  console.log(JSON.stringify(populationTree, null, 2));
  return {mapping, populationTree};
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


module.exports.parseSchema = parseSchema;
module.exports.renderDoc = renderDoc;