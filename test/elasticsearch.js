'use strict';

const _ = require('lodash');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const elasticsearch = require('../lib/elasticsearch');
const errors = require('../lib/errors');

const expect = chai.expect;
chai.use(chaiAsPromised);

const host = 'localhost:9200';

const notExistingId = 'mongoolastic-test-not-existing-id';
const notExistingIndex = 'mongoolastic-test-not-existing-index';
const type = 'Animal';

const mappings = {
  'Animal': {
    properties: {
      name: {
        type: 'string'
      }
    }
  }
};

const indexSettings = {
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

const indexTests = [
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

const settingsTests = [
  {settings: null, isValid: false},
  {settings: undefined, isValid: false},
  {settings: {}, isValid: true},
  {settings: [], isValid: false},
  {settings: 123, isValid: false},
  {settings: 'abc', isValid: false},
  {settings: {}, isValid: true},
  {settings: indexSettings, isValid: true}
];

const mappingsTests = [
  {mappings: null, isValid: false},
  {mappings: undefined, isValid: false},
  {mappings: [], isValid: false},
  {mappings: 123, isValid: false},
  {mappings: 'abc', isValid: false},
  {mappings: {}, isValid: true},
  {mappings: {'Cows': {}}, isValid: true}
];

const typesTests = [
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

const idTests = [
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

describe('Elasticsearch - Connection', () => {

  it('should create a connection', () => {
    
    return expect(elasticsearch.connect(host))
      .to.eventually.be.fulfilled;
  });
});


describe('Elasticsearch - Validation', () => {

  it('should check if index name is valid', () => {

    indexTests.forEach((test) => {
      expect(elasticsearch.isValidIndex(test.index, test.allowList))
        .to.equal(test.isValid);
    });
  });

  it('should check if settings are valid', () => {

    settingsTests.forEach((test) => {
      expect(elasticsearch.isValidSettings(test.settings))
        .to.equal(test.isValid);
    });
  });

  it('should check if mappings are valid', () => {

    mappingsTests.forEach((test) => {
      expect(elasticsearch.isValidMappings(test.mappings))
        .to.equal(test.isValid);
    });
  });

  it('should check if type is valid', () => {

    typesTests.forEach((test) => {
      expect(elasticsearch.isValidType(test.type))
        .to.equal(test.isValid);
    });
  });

  it('should check if id is valid', () => {

    idTests.forEach((test) => {
      expect(elasticsearch.isValidId(test.id))
        .to.equal(test.isValid);
    });
  });
});


describe('Elasticsearch - Ensure index', () => {

  const testIndex = 'mongoolastic-test-ensure-index';

  before((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });

  it('should create an index if it does not exist', () => {

    // Ensure the index
    return expect(elasticsearch.ensureIndex(testIndex, indexSettings, mappings))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.acknowledged).to.deep.equal(true);

        // Check that index exists
        return expect(elasticsearch.indexExists(testIndex))
          .to.eventually.be.fulfilled
          .then((indexExists) => {

            expect(indexExists).to.deep.equal(true);

            // Check index settings
            return expect(elasticsearch.getIndexSettings(testIndex))
              .to.eventually.be.fulfilled
              .then((res) => {

                expect(res).to.have.property(testIndex);
                expect(res[testIndex]).to.have.property('settings');
                expect(res[testIndex].settings.index.analysis)
                  .to.deep.equal(indexSettings.index.analysis);

                // Check index mapping
                return expect(elasticsearch.getIndexMapping(testIndex, type))
                  .to.eventually.be.fulfilled
                  .then((res) => {

                    expect(res).to.have.property(testIndex);
                    expect(res[testIndex]).to.have.property('mappings');
                    expect(res[testIndex].mappings)
                      .to.deep.equal(mappings);
                  });
              });
          });
      });
  });

  after((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

describe('Elasticsearch - Delete index', () => {

  const testIndices = [
    'mongoolastic-test-delete-1',
    'mongoolastic-test-delete-2',
    'mongoolastic-test-delete-3'
  ];

  before((done) => {

    elasticsearch.ensureIndex(testIndices[0], {}, {})
      .then(() => {
        return elasticsearch.ensureIndex(testIndices[1], {}, {})
          .then(() => {
            return elasticsearch.ensureIndex(testIndices[2], {}, {})
              .then(() => {
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should throw IndexNotFoundError if index does not exist', () => {

    // Delete the index
    return expect(elasticsearch.deleteIndex(notExistingIndex))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should throw IndexOperationError if delete operation is not acknowledged', () => {

    // TODO: Implement this test

  });

  it('should delete an existing index (string)', () => {

    const testIndex = testIndices.pop();

    // Delete the index
    return expect(elasticsearch.deleteIndex(testIndex))
      .to.eventually.be.fulfilled
      .then(() => {

        // Check that the index does not exist anymore
        return expect(elasticsearch.indexExists(testIndex))
          .to.eventually.be.fulfilled
          .then((indexExists) => {
            return expect(indexExists).to.be.false;
          });
      });
  });

  it('should delete existing indices (list)', () => {

    // Delete the index
    return expect(elasticsearch.deleteIndex(testIndices))
      .to.eventually.be.fulfilled
      .then(() =>{
        return testIndices;
      })
      .map(function(index) {

        // Check existence for all supplied indices
        return elasticsearch.indexExists(index)
          .then((indexExists) => {
            return indexExists ? index : null;
          });
      })
      .then((res) => {
        return expect(_.every(res, false));
      });
  });
});


/**
 * Ensure delete index
 *
 */
describe('Elasticsearch - Ensure delete index', () => {

  let testIndices = [
    'mongoolastic-test-ensure-delete-1',
    'mongoolastic-test-ensure-delete-2',
    'mongoolastic-test-ensure-delete-3'
  ];

  before((done) => {

    elasticsearch.ensureIndex(testIndices[0], {}, {})
      .then(() => {
        return elasticsearch.ensureIndex(testIndices[1], {}, {})
          .then(() => {
            return elasticsearch.ensureIndex(testIndices[2], {}, {})
              .then(() => {
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should not throw an IndexNotFoundError if index does not exist', () => {

    return expect(elasticsearch.ensureDeleteIndex(notExistingIndex))
      .to.eventually.be.fulfilled
      .then((res) => {
        return expect(res).to.deep.equal([]);
      });
  });

  it('should delete the supplied index (string) and return list of deletions', () => {

    const testIndex = testIndices.pop();

    return expect(elasticsearch.ensureDeleteIndex(testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {
        return expect(res).to.deep.equal([testIndex]);
      });
  });

  it('should delete the supplied indices (list) and return list of deletions', () => {

    return expect(elasticsearch.ensureDeleteIndex(testIndices))
      .to.eventually.be.fulfilled
      .then((res) => {
        return expect(res).to.deep.equal(testIndices);
      });
  });
});

/**
 * Get index mapping
 *
 */
describe('Elasticsearch - Get index mapping', () => {

  const testIndex = 'mongoolastic-test-get-index-mapping';

  before((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => done());
      })
      .catch(done);
  });

  it('should throw an IndexNotFoundError if index does not exist', () => {

    return expect(elasticsearch.getIndexMapping(notExistingIndex, type))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should return the mapping for an index and type', () => {

    return expect(elasticsearch.getIndexMapping(testIndex, type))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res).to.have.property(testIndex);
        expect(res[testIndex]).to.have.property('mappings');
        expect(res[testIndex].mappings)
          .to.deep.equal(mappings);
      });
  });

  after((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});


/**
 * Get index settings
 *
 */
describe('Elasticsearch - Get index settings', () => {

  const testIndex = 'mongoolastic-test-get-index-settings';

  before((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => done());
      })
      .catch(done);
  });

  it('should throw an IndexNotFoundError if index does not exist', () => {

    return expect(elasticsearch.getIndexSettings(notExistingIndex, type))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should return the setting for an index', () => {

    return expect(elasticsearch.getIndexSettings(testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res).to.have.property(testIndex);
        expect(res[testIndex]).to.have.property('settings');
        expect(res[testIndex].settings.index.analysis)
          .to.deep.equal(indexSettings.index.analysis);
      });
  });

  after((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});


describe('Elasticsearch - Index document', () => {

  const testIndex = 'mongoolastic-test-index-document';
  const doc = {name: 'Bob', hobby: 'Mooo'};
  const docUpdated = {name: 'Bob', hobby: 'Wooof'};
  const id = '1234567890';

  before((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => done());
      })
      .catch(done);
  });

  it('should add a new document if it does not exist', () => {

    return expect(elasticsearch.indexDoc(id, doc, type, testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.created).to.deep.equal(true);

        // Get the document and compare to source
        return expect(elasticsearch.getDoc(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res.found).to.deep.equal(true);
            expect(res._index).to.deep.equal(testIndex);
            expect(res._type).to.deep.equal(type);
            expect(res._id).to.deep.equal(id);
            expect(res._source).to.deep.equal(doc);
          });
      });
  });

  it('should update an existing document', () => {

    return expect(elasticsearch.indexDoc(id, docUpdated, type, testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.created).to.deep.equal(false);

        // Get the document and compare to source
        return expect(elasticsearch.getDoc(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res.found).to.deep.equal(true);
            expect(res._index).to.deep.equal(testIndex);
            expect(res._type).to.deep.equal(type);
            expect(res._id).to.deep.equal(id);
            expect(res._source).to.deep.equal(docUpdated);
          });
      });
  });

  after((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

/**
 * Delete document
 *
 */
describe('Elasticsearch - Delete document', () => {

  const testIndex = 'mongoolastic-test-delete-document';
  const doc = {name: 'Bob', hobby: 'Mooo'};
  const id = '1234567890';

  before((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => {
            elasticsearch.indexDoc(id, doc, type, testIndex)
              .then(() => done());
          });
      })
      .catch(done);
  });

  it('should throw DocumentNotFoundError if document does not exist', () => {

    // Delete the index
    return expect(elasticsearch.deleteDoc(notExistingId, type, testIndex))
      .to.be.rejectedWith(errors.DocumentNotFoundError);
  });

  it('should delete a document', () => {

    // Delete the document
    return expect(elasticsearch.deleteDoc(id, type, testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.found).to.deep.equal(true);

        // Check that document does not exist anymore
        return expect(elasticsearch.docExists(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res).to.deep.equal(false);
          });
      });
  });

  after((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

describe('Elasticsearch - Search', () => {

  const testIndex = 'mongoolastic-test-search';
  const doc = {name: 'Bob', hobby: 'Mooo'};
  const id = '1234567890';
  const query = {
    'index': testIndex,
    'query_string': {
      'query': 'Bob'
    }
  };

  before((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => {
        elasticsearch.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => {
            elasticsearch.indexDoc(id, doc, type, testIndex)
              .then(() => done());
          });
      })
      .catch(done);
  });

  it('should search the index and find a document', () => {

    return expect(elasticsearch.search(query))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res).to.have.property('hits');
        expect(res.hits.hits).to.have.length(1);

        const hit = res.hits.hits[0];

        expect(hit._id).to.equal(id);
        expect(hit._type).to.equal(type);
        expect(hit._source).to.deep.equal(doc);
      });
  });

  after((done) => {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});