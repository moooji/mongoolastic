'use strict';

var _ = require('lodash');
var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
var elasticsearch = require('../lib/elasticsearch');
var errors = require('../lib/errors');

var expect = chai.expect;
chai.use(chaiAsPromised);

var host = 'localhost:9200';

var notExistingId = 'mongoolastic-test-not-existing-id';
var notExistingIndex = 'mongoolastic-test-not-existing-index';
var type = 'Animal';

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
  {index: 'abc-def', isValid: true},
  {index: ['abc-def'], isValid: false},
  {index: ['abc-def', 123], allowList: true, isValid: false},
  {index: ['abc-def', 'ghi'], allowList: true, isValid: true},
  {index: 'abc-def', allowList: true, isValid: true}
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
      expect(elasticsearch.isValidIndex(test.index, test.allowList))
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

  var testIndex = 'mongoolastic-test-ensure-index';

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        return done();
      })
      .catch(done);
  });

  it('should create an index if it does not exist', function() {

    // Ensure the index
    return expect(elasticsearch.ensureIndex(testIndex, indexSettings, mappings))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res.acknowledged).to.deep.equal(true);

        // Check that index exists
        return expect(elasticsearch.indexExists(testIndex))
          .to.eventually.be.fulfilled
          .then(function(indexExists) {

            expect(indexExists).to.deep.equal(true);

            // Check index settings
            return expect(elasticsearch.getIndexSettings(testIndex))
              .to.eventually.be.fulfilled
              .then(function(res) {

                expect(res).to.have.property(testIndex);
                expect(res[testIndex]).to.have.property('settings');
                expect(res[testIndex].settings.index.analysis)
                  .to.deep.equal(indexSettings.index.analysis);

                // Check index mapping
                return expect(elasticsearch.getIndexMapping(testIndex, type))
                  .to.eventually.be.fulfilled
                  .then(function(res) {

                    expect(res).to.have.property(testIndex);
                    expect(res[testIndex]).to.have.property('mappings');
                    expect(res[testIndex].mappings)
                      .to.deep.equal(mappings);
                  });
              });
          });
      });
  });

  after(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        return done();
      })
      .catch(done);
  });
});

describe('Elasticsearch - Delete index', function() {

  var testIndices = [
    'mongoolastic-test-delete-1',
    'mongoolastic-test-delete-2',
    'mongoolastic-test-delete-3'
  ];

  before(function(done) {

    elasticsearch.ensureIndex(testIndices[0], {}, {})
      .then(function(){
        return elasticsearch.ensureIndex(testIndices[1], {}, {})
          .then(function(){
            return elasticsearch.ensureIndex(testIndices[2], {}, {})
              .then(function(){
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should throw IndexNotFoundError if index does not exist', function() {

    // Delete the index
    return expect(elasticsearch.deleteIndex(notExistingIndex))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should throw IndexOperationError if delete operation is not acknowledged', function() {

    // TODO: Implement this test

  });

  it('should delete an existing index (string)', function() {

    var testIndex = testIndices.pop();

    // Delete the index
    return expect(elasticsearch.deleteIndex(testIndex))
      .to.eventually.be.fulfilled
      .then(function() {

        // Check that the index does not exist anymore
        return expect(elasticsearch.indexExists(testIndex))
          .to.eventually.be.fulfilled
          .then(function(indexExists) {
            return expect(indexExists).to.be.false;
          });
      });
  });

  it('should delete existing indices (list)', function() {

    // Delete the index
    return expect(elasticsearch.deleteIndex(testIndices))
      .to.eventually.be.fulfilled
      .then(function(){
        return testIndices;
      })
      .map(function(index) {

        // Check existence for all supplied indices
        return elasticsearch.indexExists(index)
          .then(function(indexExists) {
            return indexExists ? index : null;
          });
      })
      .then(function(res) {
        return expect(_.every(res, false));
      });
  });
});


/**
 * Ensure delete index
 *
 */
describe('Elasticsearch - Ensure delete index', function() {

  var testIndices = [
    'mongoolastic-test-ensure-delete-1',
    'mongoolastic-test-ensure-delete-2',
    'mongoolastic-test-ensure-delete-3'
  ];

  before(function(done) {

    elasticsearch.ensureIndex(testIndices[0], {}, {})
      .then(function(){
        return elasticsearch.ensureIndex(testIndices[1], {}, {})
          .then(function(){
            return elasticsearch.ensureIndex(testIndices[2], {}, {})
              .then(function(){
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should not throw an IndexNotFoundError if index does not exist', function() {

    return expect(elasticsearch.ensureDeleteIndex(notExistingIndex))
      .to.eventually.be.fulfilled
      .then(function(res) {
        return expect(res).to.deep.equal([]);
      });
  });

  it('should delete the supplied index (string) and return list of deletions', function() {

    var testIndex = testIndices.pop();

    return expect(elasticsearch.ensureDeleteIndex(testIndex))
      .to.eventually.be.fulfilled
      .then(function(res) {
        return expect(res).to.deep.equal([testIndex]);
      });
  });

  it('should delete the supplied indices (list) and return list of deletions', function() {

    return expect(elasticsearch.ensureDeleteIndex(testIndices))
      .to.eventually.be.fulfilled
      .then(function(res) {
        return expect(res).to.deep.equal(testIndices);
      });
  });
});

/**
 * Get index mapping
 *
 */
describe('Elasticsearch - Get index mapping', function() {

  var testIndex = 'mongoolastic-test-get-index-mapping';

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(function() {
            return done();
          });
      })
      .catch(done);
  });

  it('should throw an IndexNotFoundError if index does not exist', function() {

    return expect(elasticsearch.getIndexMapping(notExistingIndex, type))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should return the mapping for an index and type', function() {

    return expect(elasticsearch.getIndexMapping(testIndex, type))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res).to.have.property(testIndex);
        expect(res[testIndex]).to.have.property('mappings');
        expect(res[testIndex].mappings)
          .to.deep.equal(mappings);
      });
  });

  after(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        return done();
      })
      .catch(done);
  });
});


/**
 * Get index settings
 *
 */
describe('Elasticsearch - Get index settings', function() {

  var testIndex = 'mongoolastic-test-get-index-settings';

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(function() {
            return done();
          });
      })
      .catch(done);
  });

  it('should throw an IndexNotFoundError if index does not exist', function() {

    return expect(elasticsearch.getIndexSettings(notExistingIndex, type))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should return the setting for an index', function() {

    return expect(elasticsearch.getIndexSettings(testIndex))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res).to.have.property(testIndex);
        expect(res[testIndex]).to.have.property('settings');
        expect(res[testIndex].settings.index.analysis)
          .to.deep.equal(indexSettings.index.analysis);
      });
  });

  after(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        return done();
      })
      .catch(done);
  });
});


describe('Elasticsearch - Index document', function() {

  var testIndex = 'mongoolastic-test-index-document';
  var doc = {name: 'Bob', hobby: 'Mooo'};
  var docUpdated = {name: 'Bob', hobby: 'Wooof'};
  var id = '1234567890';

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(function() {
            return done();
          });
      })
      .catch(done);
  });

  it('should add a new document if it does not exist', function() {

    return expect(elasticsearch.indexDoc(id, doc, type, testIndex))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res.created).to.deep.equal(true);

        // Get the document and compare to source
        return expect(elasticsearch.getDoc(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then(function(res) {

            expect(res.found).to.deep.equal(true);
            expect(res._index).to.deep.equal(testIndex);
            expect(res._type).to.deep.equal(type);
            expect(res._id).to.deep.equal(id);
            expect(res._source).to.deep.equal(doc);
          });
      });
  });

  it('should update an existing document', function() {

    return expect(elasticsearch.indexDoc(id, docUpdated, type, testIndex))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res.created).to.deep.equal(false);

        // Get the document and compare to source
        return expect(elasticsearch.getDoc(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then(function(res) {

            expect(res.found).to.deep.equal(true);
            expect(res._index).to.deep.equal(testIndex);
            expect(res._type).to.deep.equal(type);
            expect(res._id).to.deep.equal(id);
            expect(res._source).to.deep.equal(docUpdated);
          });
      });
  });

  after(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        return done();
      })
      .catch(done);
  });
});

/**
 * Delete document
 *
 */
describe('Elasticsearch - Delete document', function() {

  var testIndex = 'mongoolastic-test-delete-document';
  var doc = {name: 'Bob', hobby: 'Mooo'};
  var id = '1234567890';

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(function() {
            elasticsearch.indexDoc(id, doc, type, testIndex)
              .then(function() {
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should throw DocumentNotFoundError if document does not exist', function() {

    // Delete the index
    return expect(elasticsearch.deleteDoc(notExistingId, type, testIndex))
      .to.be.rejectedWith(errors.DocumentNotFoundError);
  });

  it('should delete a document', function() {

    // Delete the document
    return expect(elasticsearch.deleteDoc(id, type, testIndex))
      .to.eventually.be.fulfilled
      .then(function(res) {

        expect(res.found).to.deep.equal(true);

        // Check that document does not exist anymore
        return expect(elasticsearch.docExists(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then(function(res) {

            expect(res).to.deep.equal(false);
          });
      });
  });

  after(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        return done();
      })
      .catch(done);
  });
});

describe('Elasticsearch - Search', function() {

  var testIndex = 'mongoolastic-test-search';
  var doc = {name: 'Bob', hobby: 'Mooo'};
  var id = '1234567890';
  var query = {
    'index': testIndex,
    'query_string': {
      'query': 'Bob'
    }
  };

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(function() {
            elasticsearch.indexDoc(id, doc, type, testIndex)
              .then(function() {
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should search the index and find a document', function() {

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

  after(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(function() {
        return done();
      })
      .catch(done);
  });
});