'use strict';

//var Promise = require('bluebird');
var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
var mongoose = require('mongoose');
var plugin = require('../lib/plugin');
//var errors = require('../lib/errors');

var expect = chai.expect;
chai.use(chaiAsPromised);

var host = 'localhost:9200';

var testSchemaIndex = 'mongoolastic-test-schema';
var TestSchema = new mongoose.Schema({
  url: {
    type: String,
    required: true
  },
  title: {
    type: String
  },
  mediaType: {
    type: String
  }
}, {_id: false});

var testSchemaSettings = {
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

describe('Plugin - Register', function() {

  it('should register a schema', function() {

    return expect(plugin.register(
      TestSchema,
      testSchemaIndex,
      testSchemaSettings))
      .to.eventually.be.fulfilled;
  });
});

describe('Plugin - Connect', function() {

  it('should connect to Elasticsearch', function() {

    return expect(plugin.connect(host))
      .to.eventually.be.fulfilled;
  });
});