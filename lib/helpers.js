'use strict';

const _ = require('lodash');
const mongoose = require('mongoose');
const errors = require('./errors');


/**
 * Renders Elasticsearch mapping for a supplied mongoose model
 * @param {Model} model
 */
function getModelMapping(model) {

  // Validation
  if(!(model.schema instanceof mongoose.Schema)) {
    throw new errors.InvalidArgumentError('invalid-model');
  }

  let mapping = {properties: {}};

  const schema = model.schema;
  const modelName = model.modelName;
  const paths = {properties: {}};

  console.log(schema.tree);
  // Get paths in schema which have Elasticsearch mapping options set
  // and merge all their mappings
  _.forOwn(schema.tree, (value, key) => {

    // If path is a sub document, add it to the mapping
    if(_.isArray(value) && _.first(value) instanceof mongoose.Schema) {

      // TODO Add function for recursion of sub docs schemas
      console.log (_.first(value));
      console.log('SCHEMA!!');
    }

    // Check if path has Elasticsearch options set,
    // otherwise it will be ignored
    if(!_.isPlainObject(value.elasticsearch)) {
      return;
    }

    const options = value.elasticsearch;

    // Check Elasticsearch mapping for path
    if (_.isPlainObject(options.mapping)) {
      paths.properties[key] = options.mapping;
    }

    // If path is a reference and the population
    // option is set, resolve the referenced schemas and mappings
    if(options.populate === true && _.isString(value.ref)) {

      const refModelName = value.ref;
      console.log(refModelName);
    }
  });

  mapping = _.merge(mapping, paths);
  return {[modelName]: mapping};
}


function populateDoc(doc) {
  return doc;
}

module.exports.getModelMapping = getModelMapping;
module.exports.populateDoc = populateDoc;


// Elasticsearch requires all nested properties 'sub.subSub'
// to be wrapped as {sub: {properties: {subSub: {properties: ...}}}}
/*

 // Helper function that wraps a key / value pair as object
 const wrapValue = (key, value) => {
 return _.set({}, key, value);
 };


 let nestedMapping = {};

 _.forOwn(mapping, (value, key) => {

 const nestedKeys = key.split('.');
 let nestedValue = value;

 if(nestedKeys.length > 1) {
 console.debug(key);
 }

 // Top level
 nestedValue = wrapValue(nestedKeys.pop(), nestedValue);

 // Deeper levels need to be wrapped as 'properties'
 _.forEachRight(nestedKeys, (nestedKey) => {

 nestedValue = wrapValue(nestedKey, {
 properties: nestedValue
 });
 });

 nestedMapping = _.merge(nestedMapping, nestedValue);
 });
 */