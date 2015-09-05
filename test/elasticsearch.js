'use strict';

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
var elasticsearch = require('../lib/elasticsearch');

var expect = chai.expect;
chai.use(chaiAsPromised);

var host = 'localhost:9200';
var index = 'mongoolastic-test-index';

var id = '123';
var type = 'Animal';
var doc = {name: 'Bob', hobby: 'Mooo'};

var query = {
  'query_string': {
    'query': 'Bob'
  }
};

var mappings = {
  'Animal': {
    properties: {
      name: {
        type: 'string'
      }
    }
  }
};

var indexSettings = {
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

var indexTests = [
  {index: null, isValid: false},
  {index: undefined, isValid: false},
  {index: {}, isValid: false},
  {index: [], isValid: false},
  {index: 123, isValid: false},
  {index: 'ABC', isValid: false},
  {index: 'abcDEF', isValid: false},
  {index: 'abc', isValid: true},
  {index: 'abc-def', isValid: true}
];

var settingsTests = [
  {settings: null, isValid: false},
  {settings: undefined, isValid: false},
  {settings: {}, isValid: true},
  {settings: [], isValid: false},
  {settings: 123, isValid: false},
  {settings: 'abc', isValid: false},
  {settings: {}, isValid: true},
  {settings: indexSettings, isValid: true}
];

var mappingsTests = [
  {mappings: null, isValid: false},
  {mappings: undefined, isValid: false},
  {mappings: [], isValid: false},
  {mappings: 123, isValid: false},
  {mappings: 'abc', isValid: false},
  {mappings: {}, isValid: true},
  {mappings: {'Cows': {}}, isValid: true}
];

var typesTests = [
  {type: null, isValid: false},
  {type: undefined, isValid: false},
  {type: {}, isValid: false},
  {type: [], isValid: false},
  {type: 123, isValid: false},
  {type: {}, isValid: false},
  {type: {'Cows': {}}, isValid: false},
  {type: 'abc', isValid: true},
  {type: 'ABC', isValid: true}
];

var idTests = [
  {id: null, isValid: false},
  {id: undefined, isValid: false},
  {id: {}, isValid: false},
  {id: [], isValid: false},
  {id: 123, isValid: false},
  {id: {}, isValid: false},
  {id: {'Cows': {}}, isValid: false},
  {id: 'abc', isValid: true},
  {id: 'ABC', isValid: true}
];

describe('Elasticsearch - Connection', function() {

  it('should create a connection', function() {

    return expect(elasticsearch.connect(host))
      .to.eventually.be.fulfilled;
  });
});


describe('Elasticsearch - Validation', function() {

  it('should check if index name is valid', function() {

    indexTests.forEach(function(test) {
      expect(elasticsearch.isValidIndex(test.index))
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

  it('should check if id is valid', function() {

    idTests.forEach(function(test) {
      expect(elasticsearch.isValidId(test.id))
        .to.equal(test.isValid);
    });
  });
});


describe('Elasticsearch - Ensure index', function() {

  it('should create an index if it does not exist', function() {

    // Ensure the index
    return expect(elasticsearch.ensureIndex(index, indexSettings, mappings))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res.acknowledged).to.deep.equal(true);

        // Check that index exists
        return expect(elasticsearch.indexExists(index))
          .to.eventually.be.fulfilled
          .then(function(indexExists) {

            expect(indexExists).to.deep.equal(true);

            // Check index settings
            return expect(elasticsearch.getIndexSettings(index))
              .to.eventually.be.fulfilled
              .then(function(res) {

                expect(res).to.have.property(index);
                expect(res[index]).to.have.property('settings');
                expect(res[index].settings.index.analysis)
                  .to.deep.equal(indexSettings.index.analysis);

                // Check index mapping
                return expect(elasticsearch.getIndexMapping(index))
                  .to.eventually.be.fulfilled
                  .then(function(res) {

                    expect(res).to.have.property(index);
                    expect(res[index]).to.have.property('mappings');
                    expect(res[index].mappings)
                      .to.deep.equal(mappings);
                  });
              });
          });
      });
  });
});


describe('Elasticsearch - Index document', function() {

  it('should add a new document if it does not exist', function() {

    // Check that document does not exist yet
    return expect(elasticsearch.docExists(id, type, index))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res).to.deep.equal(false);

        // Index the document
        return expect(elasticsearch.indexDoc(id, doc, type, index))
          .to.eventually.be.fulfilled
          .then(function(res) {

            expect(res.created).to.deep.equal(true);

            // Get the document and compare to source
            return expect(elasticsearch.getDoc(id, type, index))
              .to.eventually.be.fulfilled
              .then(function(res) {

                expect(res.found).to.deep.equal(true);
                expect(res._index).to.deep.equal(index);
                expect(res._type).to.deep.equal(type);
                expect(res._id).to.deep.equal(id);
                expect(res._source).to.deep.equal(doc);
              });
          });
      });
  });
});


describe('Elasticsearch - Search', function() {

  it('should search the index', function() {

    return expect(elasticsearch.search(query))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res).to.have.property('hits');
        expect(res.hits.hits).to.have.length(1);

        var hit = res.hits.hits[0];

        expect(hit._id).to.equal(id);
        expect(hit._type).to.equal(type);
        expect(hit._source).to.deep.equal(doc);
      });
  });
});

describe('Elasticsearch - Delete document', function() {

  it('should delete a document', function() {

    // Delete the document
    return expect(elasticsearch.deleteDoc(id, type, index))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res.found).to.deep.equal(true);

        // Check that document does not exist anymore
        return expect(elasticsearch.docExists(id, type, index))
          .to.eventually.be.fulfilled
          .then(function(res) {

            expect(res).to.deep.equal(false);
          });
      });
  });
});

describe('Elasticsearch - Delete index', function() {

  it('should delete an existing index', function() {

    // Delete the index
    return expect(elasticsearch.deleteIndex(index))
      .to.eventually.be.fulfilled
      .then(function() {

        // Check that the index does not exist anymore
        return expect(elasticsearch.indexExists(index))
          .to.eventually.be.fulfilled
          .then(function(indexExists) {
            return expect(indexExists).to.be.false;
          });
      });
  });
});