"use strict";

var chai = require('chai');
var chaiAsPromised = require("chai-as-promised");
var Promise = require("bluebird");
var elasticsearch = require('../lib/elasticsearch');
var errors = require('../lib/errors');

var InvalidArgumentError = errors.InvalidArgumentError;

var expect = chai.expect;
var should = chai.should;
chai.use(chaiAsPromised);

var host = "localhost:9200";
var index = "mongoolastic-test-index";

var indexNameTests = [
  {index: null, isValid: false},
  {index: undefined, isValid: false},
  {index: {}, isValid: false},
  {index: [], isValid: false},
  {index: 123, isValid: false},
  {index: "ABC", isValid: false},
  {index: "abcDEF", isValid: false},
  {index: "abc", isValid: true},
  {index: "abc-def", isValid: true}
];

var settingsTests = [
  {index: "abc-def", settings: null, isValid: false},
  {index: "abc-def", settings: undefined, isValid: false},
  {index: "abc-def", settings: {}, isValid: true},
  {index: "abc-def", settings: [], isValid: false},
  {index: "abc-def", settings: 123, isValid: false},
  {index: "abc-def", settings: "abc", isValid: false},
  {index: "abc-def", settings: {}, isValid: true},
  {index: "abc-def", settings: {analyzers: {}}, isValid: true}
];

describe('Elasticsearch - Connection', function() {

  it('should create a connection', function() {

    return expect(elasticsearch.connect({host: host}))
      .to.eventually.be.fulfilled;
  });
});


describe('Elasticsearch - Validate index name', function() {

  it('should check if an index name is not valid', function() {

    indexNameTests.forEach(function(test) {
      expect(elasticsearch.isValidIndexName(test.index))
        .to.equal(test.isValid);
    });
  });

  indexNameTests.forEach(function(test) {

    if(test.isValid) {

      it('should succeed if index is ' + test.index, function() {
        return expect(elasticsearch.validateIndexName(test.index))
          .to.eventually.be.fulfilled
      });
    }
    else {
      it('should throw InvalidArgumentError if index is ' + test.index, function() {
        return expect(elasticsearch.validateIndexName(test.index))
          .to.be.rejectedWith(InvalidArgumentError);
      });
    }
  });
});


describe('Elasticsearch - Validate index settings', function() {

  settingsTests.forEach(function(test) {

    if(test.isValid) {

      it('should succeed if index is ' + test.index + ' and settings ' + test.settings, function() {
        return expect(elasticsearch.validateIndexSettings(test.index, test.settings))
          .to.eventually.be.fulfilled
      });
    }
    else {
      it('should throw InvalidArgumentError if index is ' + test.index +
        ' and settings ' + test.settings, function() {

        return expect(elasticsearch.validateIndexSettings(test.index, test.settings))
          .to.be.rejectedWith(InvalidArgumentError);
      });
    }
  });
});


describe('Elasticsearch - Ensure index', function() {

  it('should create an index if it does not exist', function() {

    return expect(elasticsearch.ensureIndex(index))
      .to.eventually.be.fulfilled
      .then(function() {

        return expect(elasticsearch.indexExists(index))
          .to.eventually.be.fulfilled
          .then(function(indexExists) {
            return expect(indexExists).to.be.true;
          });
      });
  });


});

describe('Elasticsearch - Delete index', function() {

  it('should delete an existing index', function() {

    return expect(elasticsearch.deleteIndex(index))
      .to.eventually.be.fulfilled
      .then(function() {

        return expect(elasticsearch.indexExists(index))
          .to.eventually.be.fulfilled
          .then(function(indexExists) {
            return expect(indexExists).to.be.false;
          });
      });
  });
});


describe('Elasticsearch - Ensure settings', function() {

  it('should create settings if they do not exist', function() {

  });
});

describe('Elasticsearch - Ensure mappings', function() {

  it('should create mapping if it does not exist', function() {

  });
});

describe('Elasticsearch - Index document', function() {

  it('should add a new document if it does not exist', function() {

  });
});

describe('Elasticsearch - Delete document', function() {

  it('should delete a document', function() {

  });
});