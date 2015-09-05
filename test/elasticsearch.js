"use strict";

var Promise = require("bluebird");
var chai = require('chai');
var chaiAsPromised = require("chai-as-promised");
var elasticsearch = require('../lib/elasticsearch');
var errors = require('../lib/errors');

var InvalidArgumentError = errors.InvalidArgumentError;

var expect = chai.expect;
var should = chai.should;
chai.use(chaiAsPromised);

var host = "localhost:9200";
var index = "mongoolastic-test-index";

var indexSettings = {
  'settings': {
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
  }
};

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
  {settings: null, isValid: false},
  {settings: undefined, isValid: false},
  {settings: {}, isValid: true},
  {settings: [], isValid: false},
  {settings: 123, isValid: false},
  {settings: "abc", isValid: false},
  {settings: {}, isValid: true},
  {settings: indexSettings, isValid: true}
];

var mappingsTests = [
  {mappings: null, isValid: false},
  {mappings: undefined, isValid: false},
  {mappings: [], isValid: false},
  {mappings: 123, isValid: false},
  {mappings: "abc", isValid: false},
  {mappings: {}, isValid: true},
  {mappings: {"Cows": {}}, isValid: true}
];

var typesTests = [
  {type: null, isValid: false},
  {type: undefined, isValid: false},
  {type: {}, isValid: false},
  {type: [], isValid: false},
  {type: 123, isValid: false},
  {type: {}, isValid: false},
  {type: {"Cows":{}}, isValid: false},
  {type: "abc", isValid: true},
  {type: "ABC", isValid: true}
];

describe('Elasticsearch - Connection', function() {

  it('should create a connection', function() {

    return expect(elasticsearch.connect({host: host}))
      .to.eventually.be.fulfilled;
  });
});


describe('Elasticsearch - Validation', function() {

  it('should check if index name is valid', function() {

    indexNameTests.forEach(function(test) {
      expect(elasticsearch.isValidIndexName(test.index))
        .to.equal(test.isValid);
    });
  });

  it('should check if settings are valid', function() {

    settingsTests.forEach(function(test) {
      expect(elasticsearch.isValidSettings(test.settings))
        .to.equal(test.isValid);
    });
  });

  it('should check if mappings are valid', function() {

    mappingsTests.forEach(function(test) {
      expect(elasticsearch.isValidMappings(test.mappings))
        .to.equal(test.isValid);
    });
  });

  it('should check if type is valid', function() {

    typesTests.forEach(function(test) {
      expect(elasticsearch.isValidType(test.type))
        .to.equal(test.isValid);
    });
  });
});

describe('Elasticsearch - Errors', function() {

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
})
/*
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
*/

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

/*
describe('Elasticsearch - Ensure index settings', function() {

  it('should throw Error if index does not exist', function() {

    return expect(elasticsearch.ensureIndexSettings(index, indexSettings))
      .to.be.rejectedWith(Error);
  });

  it('should update the index settings', function() {

    return expect(elasticsearch.ensureIndex(index))
      .to.eventually.be.fulfilled
      .then(function() {

        return expect(elasticsearch.getIndexSettings(index))
          .to.eventually.be.fulfilled
          .then(function(oldSettings) {

            return expect(elasticsearch.ensureIndexSettings(index, indexSettings))
              .to.eventually.be.fulfilled
              .then(function(res) {

                console.log(res);

                return expect(elasticsearch.getIndexSettings(index))
                  .to.eventually.be.fulfilled
                  .then(function(newSettings) {

                    console.log(oldSettings);
                    console.log(newSettings);

                    return expect(newSettings).to.deepEqual(indexSettings);
                  });
              });
          });
      });
  });
});
*/

describe('Elasticsearch - Index document', function() {

  it('should add a new document if it does not exist', function() {

  });
});

describe('Elasticsearch - Delete document', function() {

  it('should delete a document', function() {

  });
});