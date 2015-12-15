'use strict';

const _ = require('lodash');
const Bluebird = require('bluebird');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const elasticsearch = require('../lib/elasticsearch');
const errors = require('../lib/errors');

const client = elasticsearch.create({bulkSize: 10, bulkTimeout: 50, bulkBufferSize: 20});

const expect = chai.expect;
chai.use(chaiAsPromised);

const host = 'localhost:9200';

const notExistingId = 'mongoolastic-test-not-existing-id';
const notExistingIndex = 'mongoolastic-test-not-existing-index';

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

describe('Elasticsearch - Create', () => {

  it('should throw InvalidArgumentError if options are invalid', () => {

    return expect(() => elasticsearch.create(123))
      .to.throw(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if bulkSize option is invalid', () => {

    return expect(() => elasticsearch.create({
      bulkSize: 'abc',
      bulkTimeout: 123,
      bulkBufferSize: 100
    })).to.throw(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if bulkTimeout option is invalid', () => {

    return expect(() => elasticsearch.create({
      bulkSize: 123,
      bulkTimeout: 'abc',
      bulkBufferSize: 100
    })).to.throw(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if bulkBufferSize option is invalid', () => {

    return expect(() => elasticsearch.create({
      bulkSize: 100,
      bulkTimeout: 100,
      bulkBufferSize: 'abc'
    })).to.throw(errors.InvalidArgumentError);
  });

  it('should create new client with options', () => {

    let newClient = null;

    expect(() => {
      newClient = elasticsearch.create({bulkSize: 123, bulkTimeout: 456});
    }).to.not.throw();

    expect(newClient.bulkSize).to.equal(123);
    expect(newClient.bulkTimeout).to.equal(456);
  });

  it('should create new client without options', () => {

    return expect(() => elasticsearch.create())
      .to.not.throw();
  });
});

describe('Elasticsearch - Connection', () => {

  it('should create a connection', () => {

    return expect(client.connect(host))
      .to.eventually.be.fulfilled;
  });
});

describe('Elasticsearch - Validation', () => {

  it('should check if index name is valid', () => {

    indexTests.forEach((test) => {
      expect(client.isValidIndex(test.index, test.allowList))
        .to.equal(test.isValid);
    });
  });

  it('should check if settings are valid', () => {

    settingsTests.forEach((test) => {
      expect(client.isValidSettings(test.settings))
        .to.equal(test.isValid);
    });
  });

  it('should check if mappings are valid', () => {

    mappingsTests.forEach((test) => {
      expect(client.isValidMapping(test.mappings))
        .to.equal(test.isValid);
    });
  });

  it('should check if type is valid', () => {

    typesTests.forEach((test) => {
      expect(client.isValidType(test.type))
        .to.equal(test.isValid);
    });
  });

  it('should check if id is valid', () => {

    idTests.forEach((test) => {
      expect(client.isValidId(test.id))
        .to.equal(test.isValid);
    });
  });
});

describe('Elasticsearch - Ensure index', () => {

  const testIndex = 'mongoolastic-test-ensure-index';
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });

  it('should throw InvalidArgumentError if mappings are not valid', () => {

    return expect(client.ensureIndex(testIndex, indexSettings, 123))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if settings are not valid', () => {

    return expect(client.ensureIndex(testIndex, 123, mappings))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if index name is not valid', () => {

    return expect(client.ensureIndex(123, indexSettings, mappings))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should create an index if it does not exist', () => {

    // Ensure the index
    return expect(client.ensureIndex(testIndex, indexSettings, mappings))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.acknowledged).to.deep.equal(true);

        // Check that index exists
        return expect(client.indexExists(testIndex))
          .to.eventually.be.fulfilled
          .then((indexExists) => {

            expect(indexExists).to.deep.equal(true);

            // Check index settings
            return expect(client.getIndexSettings(testIndex))
              .to.eventually.be.fulfilled
              .then((res) => {

                expect(res).to.have.property(testIndex);
                expect(res[testIndex]).to.have.property('settings');
                expect(res[testIndex].settings.index.analysis)
                  .to.deep.equal(indexSettings.index.analysis);

                // Check index mapping
                return expect(client.getIndexMapping(testIndex, type))
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

    client.ensureDeleteIndex(testIndex)
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

    client.ensureIndex(testIndices[0], {}, {})
      .then(() => {
        return client.ensureIndex(testIndices[1], {}, {})
          .then(() => {
            return client.ensureIndex(testIndices[2], {}, {})
              .then(() => {
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should throw IndexNotFoundError if index does not exist', () => {

    // Delete the index
    return expect(client.deleteIndex(notExistingIndex))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should throw IndexOperationError if delete operation is not acknowledged', () => {

    // TODO: Implement this test

  });

  it('should delete an existing index (string)', () => {

    const testIndex = testIndices.pop();

    // Delete the index
    return expect(client.deleteIndex(testIndex))
      .to.eventually.be.fulfilled
      .then(() => {

        // Check that the index does not exist anymore
        return expect(client.indexExists(testIndex))
          .to.eventually.be.fulfilled
          .then((indexExists) => {
            return expect(indexExists).to.be.false;
          });
      });
  });

  it('should delete existing indices (list)', () => {

    // Delete the index
    return expect(client.deleteIndex(testIndices))
      .to.eventually.be.fulfilled
      .then(() => {
        return testIndices;
      })
      .map(function(index) {

        // Check existence for all supplied indices
        return client.indexExists(index)
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

    client.ensureIndex(testIndices[0], {}, {})
      .then(() => {
        return client.ensureIndex(testIndices[1], {}, {})
          .then(() => {
            return client.ensureIndex(testIndices[2], {}, {})
              .then(() => {
                return done();
              });
          });
      })
      .catch(done);
  });

  it('should not throw an IndexNotFoundError if index does not exist', () => {

    return expect(client.ensureDeleteIndex(notExistingIndex))
      .to.eventually.be.fulfilled
      .then((res) => {
        return expect(res).to.deep.equal([]);
      });
  });

  it('should delete the supplied index (string) and return list of deletions', () => {

    const testIndex = testIndices.pop();

    return expect(client.ensureDeleteIndex(testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {
        return expect(res).to.deep.equal([testIndex]);
      });
  });

  it('should delete the supplied indices (list) and return list of deletions', () => {

    return expect(client.ensureDeleteIndex(testIndices))
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
  const type = 'Cat';
  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => {
        client.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => done());
      })
      .catch(done);
  });

  it('should throw an IndexNotFoundError if index does not exist', () => {

    return expect(client.getIndexMapping(notExistingIndex, type))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should return the mapping for an index and type', () => {

    return expect(client.getIndexMapping(testIndex, type))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res).to.have.property(testIndex);
        expect(res[testIndex]).to.have.property('mappings');
        expect(res[testIndex].mappings)
          .to.deep.equal(mappings);
      });
  });

  after((done) => {

    client.ensureDeleteIndex(testIndex)
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
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => {
        client.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => done());
      })
      .catch(done);
  });

  it('should throw an IndexNotFoundError if index does not exist', () => {

    return expect(client.getIndexSettings(notExistingIndex, type))
      .to.be.rejectedWith(errors.IndexNotFoundError);
  });

  it('should return the setting for an index', () => {

    return expect(client.getIndexSettings(testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res).to.have.property(testIndex);
        expect(res[testIndex]).to.have.property('settings');
        expect(res[testIndex].settings.index.analysis)
          .to.deep.equal(indexSettings.index.analysis);
      });
  });

  after((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

/**
 * Index document
 *
 *
 */

describe('Elasticsearch - Index document', () => {

  const testIndex = 'mongoolastic-test-index-document';
  const doc = {name: 'Bob', hobby: 'Mooo'};
  const docUpdated = {name: 'Bob', hobby: 'Wooof'};
  const id = '1234567890';
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => {
        client.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => done());
      })
      .catch(done);
  });

  it('should add a new document if it does not exist', () => {

    return expect(client.indexDoc(id, doc, type, testIndex, false))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.created).to.deep.equal(true);

        // Get the document and compare to source
        return expect(client.getDoc(id, type, testIndex))
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

  it('should index a document with _id field', () => {

    const id = '123';
    const doc = {_id: id, name: 'Bob', hobby: 'Wooof'};

    return expect(client.indexDoc(id, doc, type, testIndex, false))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.created).to.deep.equal(true);

        // Get the document and compare to source
        return expect(client.getDoc(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res.found).to.deep.equal(true);
            expect(res._index).to.deep.equal(testIndex);
            expect(res._type).to.deep.equal(type);
            expect(res._id).to.deep.equal(id);

            // Remove _id field for comparison
            delete doc._id;
            expect(res._source).to.deep.equal(doc);
          });
      });
  });

  it('should update an existing document', () => {

    return expect(client.indexDoc(id, docUpdated, type, testIndex, false))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.created).to.deep.equal(false);

        // Get the document and compare to source
        return expect(client.getDoc(id, type, testIndex))
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

    client.ensureDeleteIndex(testIndex)
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
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => {
        client.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => {
            client.indexDoc(id, doc, type, testIndex, false)
              .then(() => done());
          });
      })
      .catch(done);
  });

  it('should throw DocumentNotFoundError if document does not exist', () => {

    // Delete the index
    return expect(client.deleteDoc(notExistingId, type, testIndex))
      .to.be.rejectedWith(errors.DocumentNotFoundError);
  });

  it('should delete a document', () => {

    // Delete the document
    return expect(client.deleteDoc(id, type, testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res.found).to.deep.equal(true);

        // Check that document does not exist anymore
        return expect(client.docExists(id, type, testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res).to.deep.equal(false);
          });
      });
  });

  after((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

/**
 * Get document
 *
 */

describe('Elasticsearch - Get document', () => {

  const testIndex = 'mongoolastic-test-get-document';
  const doc = {name: 'Bob', hobby: 'Mooo'};
  const id = '1234567890';
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => {
        client.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => {
            client.indexDoc(id, doc, type, testIndex, false)
              .then(() => done());
          });
      })
      .catch(done);
  });

  it('should throw DocumentNotFoundError if document does not exist', () => {

    // Delete the index
    return expect(client.getDoc(notExistingId, type, testIndex))
      .to.be.rejectedWith(errors.DocumentNotFoundError);
  });

  it('should get a document', () => {

    // Delete the document
    return expect(client.getDoc(id, type, testIndex))
      .to.eventually.be.fulfilled
      .then((res) => {

        expect(res._id).to.deep.equal(id);
        expect(res._type).to.deep.equal(type);
        expect(res._index).to.deep.equal(testIndex);
        expect(res._source).to.deep.equal(doc);
      });
  });

  after((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

/**
 * Search
 *
 *
 */

describe('Elasticsearch - Search', () => {

  const testIndex = 'mongoolastic-test-search';
  const doc = {name: 'Bob', hobby: 'Mooo'};
  const id = '1234567890';
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  const query = {
    'index': testIndex,
    'query_string': {
      'query': 'Bob'
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => {
        client.ensureIndex(testIndex, indexSettings, mappings)
          .then(() => {
            client.indexDoc(id, doc, type, testIndex, false)
              .then(() => done());
          });
      })
      .catch(done);
  });

  it('should search the index and find a document', () => {

    return expect(client.search(query))
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

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

/**
 * Bulk index
 *
 *
 */

describe('Elasticsearch - Bulk index', function() {

  this.timeout(5000);

  const testDelay = 1000;
  const testIndex = 'mongoolastic-test-search';
  const doc = {name: 'Bob', hobby: 'Mooo'};
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => {
        return client.ensureIndex(testIndex, indexSettings, mappings);
      })
      .then(() => done())
      .catch(done);
  });

  it('should add documents to buffer and flush if buffer is full or timed out', (done) => {

    const docs = [];

    for (let i = 0; i < 50; i++) {
      doc.id = 'abcdefg' + i.toString();
      docs.push(doc);
    }

    return Bluebird.resolve(docs)
      .map((doc) => {
        return client.indexDoc(doc.id, doc, type, testIndex, true);
      })
      .then(() => {

        setTimeout(() => {

          Bluebird.resolve(docs)
            .map((doc) => {
              return client.getDoc(doc.id, type, testIndex);
            })
            .then(() => done())
            .catch(done);
        }, testDelay);
      });
  });

  after((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});

/*
describe('Elasticsearch - Bulk errors', () => {

  const newClient = elasticsearch.create({bulkBufferSize: 1, bulkTimeout: 10000});
  const testIndex = 'mongoolastic-test-search';
  const doc = {id: 'abcdef123123', name: 'Bob', hobby: 'Mooo'};
  const type = 'Cat';

  const mappings = {
    'Cat': {
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  };

  before((done) => {

    newClient.connect(host)
      .then(() => {

        newClient.ensureDeleteIndex(testIndex)
          .then(() => {
            return newClient.ensureIndex(testIndex, indexSettings, mappings);
          })
          .then(() => done())
          .catch(done);
      });
  });

  it('should throw BufferFullError when trying to add document to full buffer', () => {

    return expect(newClient.indexDoc(doc.id, doc, type, testIndex, true))
      .to.be.eventually.fulfilled
      .then(() => {

        return expect(newClient.indexDoc(doc.id, doc, type, testIndex, true))
          .to.be.rejectedWith(errors.BufferOverflowError);
      });
  });

  after((done) => {

    newClient.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });
});
*/
