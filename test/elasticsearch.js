"use strict";

const chai = require('chai');
const chaiAsPromised = require("chai-as-promised");
const elasticsearch = require('../lib/elasticsearch');
const errors = require('../lib/errors');

const InvalidArgumentError = errors.InvalidArgumentError;

const expect = chai.expect;
const should = chai.should;
chai.use(chaiAsPromised);

const host = "localhost:9200";
const index = "mongoolastic-test-index";

describe('Elasticsearch', function() {

  it('should check if an index name is valid', function() {

    var tests = [
      { value: null, result: false },
      { value: undefined, result: false },
      { value: {}, result: false },
      { value: [], result: false },
      { value: 123, result: false },
      { value: "ABC", result: false },
      { value: "abcDEF", result: false },
      { value: "abc", result: true },
      { value: "abc-def", result: true }
    ];

    tests.forEach(function(test){
      expect(elasticsearch.isValidIndexName(test.value)).to.equal(test.result);

      if (test.result === true) {
        expect(elasticsearch.validateIndexName(test.value))
          .to.eventually.be.fulfilled
          .then(function(index){
            expect(index).to.equal(test.value);
          });
      }
      else {
        expect(elasticsearch.validateIndexName(test.value))
          .to.be.rejectedWith(errors.InvalidArgumentError);
      }
    });
  });

  it('should create a connection', function() {

    return expect(elasticsearch.connect({host: host}))
      .to.eventually.be.fulfilled;
  });

  it('should delete an index', function() {

    return expect(elasticsearch.deleteIndex(index))
      .to.eventually.be.fulfilled;
  });

  it('should check if an index does not exist', function() {

    return expect(elasticsearch.indexExists(index))
      .to.eventually.be.fulfilled
      .then(function(indexExists) {
        return expect(indexExists).to.be.false;
      });
  });

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