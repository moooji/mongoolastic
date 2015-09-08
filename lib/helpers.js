'use strict';

const _ = require('lodash');
const Bluebird = require('bluebird');
const mongoose = require('mongoose');
const errors = require('./errors');


/**
 * Parses a mongoose schema to be indexed in Elasticsearch
 * @param {mongoose.Schema} schema
 * @param {Map<mongoose.Model>} populationModels
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

      // Check if referenced model has been
      // registered to be populated
      if(ref && populationModels.has(ref)) {

        const model = populationModels.get(ref);
        const populationMapping = renderMapping(model.schema, populationModels);
        result = _.merge(result, {[pathKey]: populationMapping});
      }
    }
  });

  result = !_.isEmpty(result) ? {properties: result} : {};
  return result;
}


/**
 * Render the population tree for a schema
 * @param {mongoose.Schema} schema
 * @param {Map<mongoose.Model>} populationModels
 */
function renderPopulationTree(schema, populationModels) {

  // Validation
  if(!(schema instanceof mongoose.Schema)) {
    throw new errors.InvalidArgumentError('invalid-schema');
  }

  let tree = {};

  // Get paths in schema which have Elasticsearch mapping defined
  // and merge all their mappings
  _.forOwn(schema.tree, (pathValue, pathKey) => {

    if(isSubDoc(pathValue)) {

      // If path is an embedded sub document schema,
      // merge the mappings
      const subDocTree = renderPopulationTree(_.first(pathValue), populationModels);
      //tree = _.merge(tree, {[pathKey]: subDocTree});
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

      // Check if referenced model has been
      // registered to be populated
      if(ref && populationModels.has(ref)) {

        const model = populationModels.get(ref);
        const populationTree = renderPopulationTree(model.schema, populationModels);

        tree[pathKey] = {
          fields: _.keys(populationTree)
        };

        if(!_.isEmpty(populationTree)) {
          tree[pathKey].paths = populationTree;
        }

        tree = _.merge(tree, {[pathKey]: populationTree});
      }
    }
  });

  tree = !_.isEmpty(tree) ? tree : {};
  return tree;
}


/**
 * Renders list of paths to be indexed
 * @param {mongoose.Schema} schema
 * @param {Map<mongoose.Model>} populationModels
 */
function renderIndexPaths(schema, populationModels) {

  // Validation
  if(!(schema instanceof mongoose.Schema)) {
    throw new errors.InvalidArgumentError('invalid-schema');
  }

  let result = [];

  _.forOwn(schema.tree, (pathValue, pathKey) => {

    if(hasMapping(pathValue)) {

      // If path has Elasticsearch mappings set,
      // add it to the list of paths
      result.push(pathKey);
    }
    else if(isSubDoc(pathValue)) {

      // If path is an embedded sub document schema,
      // merge the paths
      const subDoc = _.first(pathValue);
      const subDocPaths = renderIndexPaths(subDoc, populationModels);

      subDocPaths.forEach((subDocPath) => {
        result.push(pathKey + '.' + subDocPath);
      });
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

      // Check if referenced model has been
      // registered to be populated
      if(ref && populationModels.has(ref)) {

        const schema = populationModels.get(ref).schema;
        const populationPaths = renderIndexPaths(schema, populationModels);

        populationPaths.forEach((populationPath) => {
          result.push(pathKey + '.' + populationPath);
        });
      }
    }
  });

  result = !_.isEmpty(result) ? result : [];
  return result;
}


/**
 * Renders and populates a mongoose object
 * to be indexed by Elasticsearch
 * @param {object} doc
 * @param {[string]} indexPaths
 * @param {object} populationTree
 * @param {function} [callback]
 * @returns {object}
 */
function renderDoc(doc, indexPaths, populationTree, callback) {

  return Bluebird.resolve(doc)
    .then((doc) => {

      const result = {};
      const plainDoc = doc.toObject();

      indexPaths.forEach((indexPath) => {

        const pathValue = _.get(plainDoc, indexPath);

        if (pathValue) {
          _.set(result, indexPath, pathValue);
        }
      });

      return result;
      /*
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
        */
    })
    .nodeify(callback);
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

module.exports.renderMapping = renderMapping;
module.exports.renderPopulationTree = renderPopulationTree;
module.exports.renderIndexPaths = renderIndexPaths;
module.exports.renderDoc = renderDoc;