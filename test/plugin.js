'use strict';

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const mongoose = require('mongoose');
const elasticsearch = require('../lib/elasticsearch');
const plugin = require('../lib/plugin');
const errors = require('../lib/errors');

const client = elasticsearch.create();
const clientTimeout = 100;

const expect = chai.expect;
chai.use(chaiAsPromised);

const host = 'localhost:9200';

/**
 * Test data
 *
 */

const CatSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  color: {
    type: String
  }
});

const HobbySchema = new mongoose.Schema({
  likes: {
    type: Number,
    required: true
  },
  activity: {
    type: String
  }
});

const DogSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  color: {
    type: String
  },
  hobbies: [HobbySchema],
  candy: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Candy'
  }
});

const ColorSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  lightness: {
    type: Number
  }
});

const CandySchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  sugarAmount: {
    type: Number
  },
  wrappingColor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Color'
  }
});

const DogModel = mongoose.model('Dog', DogSchema);
const CandyModel = mongoose.model('Candy', CandySchema);
const ColorModel = mongoose.model('Color', ColorSchema);
const CatModel = mongoose.model('Cat', CatSchema);
const SuperCatModel = mongoose.model('SuperCat', CatSchema);

const testIndex = 'mongoolastic-test-plugin';
const testIndexSettings = {
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

/**
 * MongoDB
 *
 *
 *
 */

const connectionString = 'mongodb://localhost:27017/mongoolastic-test';
const connectionOptions = {server: {auto_reconnect: true}};

function onConnectionError(err) {
  throw err;
}

function onConnectionOpen() {
  //...
}

mongoose.connection.on('error', onConnectionError);
mongoose.connection.once('open', onConnectionOpen);
mongoose.connect(connectionString, connectionOptions);

/**
 * Register plugin
 *
 *
 */

describe('Plugin - Register model', function() {

  // Errors
  it('should throw InvalidArgumentError if transform function is not valid', () => {

    const transform = 123;
    return expect(plugin.registerModel(CatModel, {transform}))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if mapping is not valid', () => {

    const mapping = 123;
    return expect(plugin.registerModel(CatModel, {mapping}))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if model is not valid', () => {

    const model = 123;
    return expect(plugin.registerModel(model))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should register a model without mapping and transform function', () => {

    const expectedMappings = {};

    return expect(plugin.registerModel(CatModel))
      .to.eventually.be.fulfilled
      .then(() => {

        return expect(plugin.getMappings())
          .to.deep.equal(expectedMappings);
      });
  });

  it('should register a model with mapping and transform function', () => {

    const transform = (doc, done) => {

      doc.populate('candy', (err, doc) => {

        if (err) {
          return done(err);
        }

        doc.candy.populate('wrappingColor', (err) => {
          return done(err, doc);
        });
      });
    };

    const mapping = {
      name: {
        type: 'string',
        index: 'not_analyzed'
      },
      hobbies: {
        properties: {
          likes: {
            type: 'long'
          }
        }
      },
      candy: {
        properties: {
          name: {
            index: 'not_analyzed',
            type: 'string'
          },
          wrappingColor: {
            properties: {
              lightness: {
                type: 'integer'
              }
            }
          }
        }
      }
    };

    const expectedMappings = {
      Dog: {
        properties: {
          name: {
            type: 'string',
            index: 'not_analyzed'
          },
          hobbies: {
            properties: {
              likes: {
                type: 'long'
              }
            }
          },
          candy: {
            properties: {
              name: {
                index: 'not_analyzed',
                type: 'string'
              },
              wrappingColor: {
                properties: {
                  lightness: {
                    type: 'integer'
                  }
                }
              }
            }
          }
        }
      }
    };

    return expect(plugin.registerModel(DogModel, {transform, mapping}))
      .to.eventually.be.fulfilled
      .then(() => {

        return expect(plugin.getMappings())
          .to.deep.equal(expectedMappings);
      });
  });
});

/**
 * Connect
 *
 *
 */

describe('Plugin - Connect', function() {

  before((done) => {

    client.connect(host)
      .then(() => {

        client.ensureDeleteIndex(testIndex)
          .then(() => done())
          .catch(done);
      });
  });

  it('should throw InvalidArgumentError if index is not valid', () => {

    return expect(plugin.connect(host, 123))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw InvalidArgumentError if options are not valid', () => {

    return expect(plugin.connect(host, testIndex, 123))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should connect to Elasticsearch', function() {

    const expectedMappings = {
      Dog: {
        properties: {
          name: {
            type: 'string',
            index: 'not_analyzed'
          },
          hobbies: {
            properties: {
              likes: {
                type: 'long'
              }
            }
          },
          candy: {
            properties: {
              name: {
                index: 'not_analyzed',
                type: 'string'
              },
              wrappingColor: {
                properties: {
                  lightness: {
                    type: 'integer'
                  }
                }
              }
            }
          }
        }
      }
    };

    return expect(plugin.connect(host, testIndex, {settings: testIndexSettings}))
      .to.eventually.be.fulfilled
      .then(() => {

        return expect(client.getIndexSettings(testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res).to.have.property(testIndex);
            expect(res[testIndex]).to.have.property('settings');
            expect(res[testIndex].settings.index.analysis)
              .to.deep.equal(testIndexSettings.index.analysis);

            return expect(client.getIndexMapping(testIndex, DogModel.modelName))
              .to.eventually.be.fulfilled
              .then((res) => {

                return expect(res[testIndex].mappings)
                  .to.deep.equal(expectedMappings);
              });
          });
      });
  });
});

/**
 * Index document
 *
 *
 */

describe('Plugin - Index document', function() {

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });

  it('should index a mongoose document when it has been saved', (done) => {

    const newCat = new CatModel({name: 'Bingo'});

    newCat.save((err, doc) => {

      if (err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(client.getDoc(doc.id, type, testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res._index).to.deep.equal(testIndex);
            expect(res._id).to.deep.equal(newCat.id);
            expect(res._type).to.deep.equal(newCat.constructor.modelName);
            expect(res._source.name).to.deep.equal(newCat.name);

            return done();
          })
          .catch(done);

      }, clientTimeout);
    });
  });

  it('should index a populated mongoose document when it has been saved', (done) => {

    const newColor = new ColorModel({name: 'blue', lightness: 128});
    const newCandy = new CandyModel({
      name: 'Chocolate ball',
      sugarAmount: 123,
      wrappingColor: newColor._id
    });

    const newDog = new DogModel({
      name: 'Bob',
      color: 'black',
      hobbies: [{
        likes: 12,
        activity: 'jumping'
      }],
      candy: newCandy._id
    });

    newColor.save((err) => {

      if (err) {
        return done(err, null);
      }

      newCandy.save((err) => {

        if (err) {
          return done(err, null);
        }

        newDog.save((err, doc) => {

          if (err) {
            return done(err, null);
          }

          setTimeout(() => {

            const type = doc.constructor.modelName;
            return expect(client.getDoc(doc.id, type, testIndex))
              .to.eventually.be.fulfilled
              .then((res) => {

                expect(res._index).to.deep.equal(testIndex);
                expect(res._id).to.deep.equal(newDog.id);
                expect(res._type).to.deep.equal(newDog.constructor.modelName);
                expect(res._source.name).to.deep.equal(newDog.name);
                expect(res._source.color).to.deep.equal('black');
                expect(res._source.hobbies[0].likes).to.deep.equal(12);
                expect(res._source.hobbies[0].activity).to.deep.equal('jumping');
                expect(res._source.candy.name).to.deep.equal(newCandy.name);
                expect(res._source.candy.sugarAmount).to.deep.equal(123);
                expect(res._source.candy.wrappingColor.name).to.deep.equal('blue');
                expect(res._source.candy.wrappingColor.lightness).to.deep.equal(128);

                return done();
              })
              .catch(done);

          }, clientTimeout);
        });
      });
    });
  });

  it('should not index a document of same schema, if model is not registered', (done) => {

    const newSuperCat = new SuperCatModel({name: 'Jenny'});

    newSuperCat.save((err, doc) => {

      if (err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(client.getDoc(doc.id, type, testIndex))
          .to.be.rejectedWith(errors.DocumentNotFoundError)
          .then(() => done());

      }, clientTimeout);
    });
  });
});

/**
 * Remove document
 *
 *
 */

describe('Plugin - Remove document', () => {

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });

  it('should delete a mongoose document from Elasticsearch when it has been removed', (done) => {

    const newCat = new CatModel({name: 'Jeff', hobby: 'woof'});

    newCat.save((err, doc) => {

      if (err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(client.getDoc(doc.id, type, testIndex))
          .to.eventually.be.fulfilled
          .then(() => {

            newCat.remove((err) => {

              if (err) {
                return done(err, null);
              }

              setTimeout(() => {

                return expect(client.getDoc(doc.id, type, testIndex))
                  .to.be.rejectedWith(errors.DocumentNotFoundError)
                  .then(() => done());
              }, clientTimeout);
            });
          })
          .catch(done);
      }, clientTimeout);
    });
  });

  it('should not attempt to delete a mongoose document from unregistered models', (done) => {

    const newSuperCat = new SuperCatModel({name: 'Bing', hobby: 'swoosh'});

    newSuperCat.save((err, doc) => {

      if (err) {
        return done(err, null);
      }

      // TODO Add event to listen on

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(client.getDoc(doc.id, type, testIndex))
          .to.be.rejectedWith(errors.DocumentNotFoundError)
          .then(() => {

            newSuperCat.remove((err) => {

              if (err) {
                return done(err, null);
              }

              setTimeout(() => {

                return expect(client.getDoc(doc.id, type, testIndex))
                  .to.be.rejectedWith(errors.DocumentNotFoundError)
                  .then(() => done());
              }, clientTimeout);
            });
          })
          .catch(done);
      }, clientTimeout);
    });
  });
});

/**
 * Sync model documents
 *
 *
 */

describe('Plugin - Sync', function() {

  before((done) => {

    client.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });

  it('should throw InvalidArgumentError if model is invalid', () => {

    return expect(plugin.sync(123))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should sync all documents of a model', () => {

    return expect(plugin.sync(CatModel))
      .to.be.eventually.fulfilled
      .then(() => {

      });
  });
});

