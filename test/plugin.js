'use strict';

//var Promise = require('bluebird');
var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
var mongoose = require('mongoose');
var plugin = require('../lib/plugin');
var errors = require('../lib/errors');

var expect = chai.expect;
chai.use(chaiAsPromised);

var host = 'localhost:9200';

var catSchemaIndex = 'mongoolastic-cat-schema';
var CatSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  hobby: {
    type: String
  }
}, {_id: false});

var catSchemaSettings = {
  'index': {
    'analysis': {
      'filter': {
        'english_stop': {
          'type': 'stop',
          'stopwords': '_english_'
        }
      }
    }
  }
};

var dogSchemaIndex = 'mongoolastic-dog-schema';
var DogSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  color: {
    type: String,
    required: true
  }
}, {_id: false});

describe('Plugin - Register', function() {

  it('should register a schema with settings', function() {

    return expect(plugin.register(
      CatSchema,
      catSchemaIndex,
      catSchemaSettings))
      .to.eventually.be.fulfilled;
  });

  it('should register a schema without settings', function() {

    return expect(plugin.register(
      DogSchema,
      dogSchemaIndex))
      .to.eventually.be.fulfilled;
  });

  it('should throw an InvalidArgumentError if index is not valid', function() {

    return expect(plugin.register(
      CatSchema,
      123,
      catSchemaSettings))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw an InvalidArgumentError if settings are not valid', function() {

    return expect(plugin.register(
      CatSchema,
      catSchemaIndex,
      123))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw an InvalidArgumentError if schema is not valid', function() {

    return expect(plugin.register(
      123,
      catSchemaIndex,
      catSchemaSettings))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });
});

describe('Plugin - Connect', function() {

  it('should connect to Elasticsearch', function() {

    return expect(plugin.connect(host))
      .to.eventually.be.fulfilled;
  });
});