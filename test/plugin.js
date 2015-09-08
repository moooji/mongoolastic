'use strict';

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const mongoose = require('mongoose');
const elasticsearch = require('../lib/elasticsearch');
const plugin = require('../lib/plugin');
const errors = require('../lib/errors');

const expect = chai.expect;
chai.use(chaiAsPromised);

const host = 'localhost:9200';
const elasticsearchTimeout = 100;

/**
 * Test data
 *
 */
const HobbySchema = new mongoose.Schema({
  likes: {
    type: Number,
    required: true,
    elasticsearch: {
      mapping: {
        type: 'long'
      }
    }
  },
  activity: {
    type: String
  }
});

const CatSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    elasticsearch: {
      mapping: {
        index: 'not_analyzed',
        type: 'string'
      }
    }
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
    type: Number,
    elasticsearch: {
      mapping: {
        type: 'integer'
      }
    }
  }
});

const CandySchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    elasticsearch: {
      mapping: {
        index: 'not_analyzed',
        type: 'string'
      }
    }
  },
  sugarAmount: {
    type: Number
  },
  wrappingColor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Color'
  }
});

const CatModel = mongoose.model('Cat', CatSchema);
const SuperCatModel = mongoose.model('SuperCat', CatSchema);
const CandyModel = mongoose.model('Candy', CandySchema);
const ColorModel = mongoose.model('Color', ColorSchema);

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
const connectionOptions = { server: { auto_reconnect: true }};


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

  it('should register a model and update mappings', function() {

    const expectedMappings = {
      Cat: {
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
          }
        }
      }
    };

    return expect(plugin.registerModel(CatModel))
      .to.eventually.be.fulfilled
      .then(() => {

        console.log(plugin.getMappings());
        return expect(plugin.getMappings())
          .to.deep.equal(expectedMappings);
      });
  });
});


/**
 * Register population
 *
 *
 */
describe('Plugin - Register population', function() {

  it('should register a population model and update mappings', function() {

    const expectedMappingsCandy = {
      Cat: {
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
              }
            }
          }
        }
      }
    };

    const expectedMappingsColor = {
      Cat: {
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

    return expect(plugin.registerPopulation(CandyModel))
      .to.eventually.be.fulfilled
      .then(() => {

        expect(plugin.getMappings())
          .to.deep.equal(expectedMappingsCandy);

        return expect(plugin.registerPopulation(ColorModel))
          .to.eventually.be.fulfilled
          .then(() => {

            return expect(plugin.getMappings())
              .to.deep.equal(expectedMappingsColor);
        });
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

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });

  it('should connect to Elasticsearch', function() {

    const expectedMappings = {
      Cat: {
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

    return expect(plugin.connect(host, testIndex, testIndexSettings))
      .to.eventually.be.fulfilled
      .then(() => {

        return expect(elasticsearch.getIndexSettings(testIndex))
          .to.eventually.be.fulfilled
          .then(() => {

            return expect(elasticsearch.getIndexMapping(testIndex, CatModel.modelName))
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

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });

  it('should index a mongoose document when it has been saved', (done) => {

    const newCat = new CatModel({name: 'Bingo'});

    newCat.save((err, doc) => {

      if(err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(elasticsearch.getDoc(doc.id, type, testIndex))
          .to.eventually.be.fulfilled
          .then((res) => {

            expect(res._index).to.deep.equal(testIndex);
            expect(res._id).to.deep.equal(newCat.id);
            expect(res._type).to.deep.equal(newCat.constructor.modelName);
            expect(res._source.name).to.deep.equal(newCat.name);
            expect(res._source.hobbies).to.deep.equal([]);

            return done();
          })
          .catch(done);

      }, elasticsearchTimeout);
    });
  });

  it('should index a populated mongoose document when it has been saved', (done) => {

    const newColor = new ColorModel({name: 'blue', lightness: 128});
    const newCandy = new CandyModel({
      name: 'Chocolate ball',
      sugarAmount: 123,
      wrappingColor: newColor._id
    });

    const newCat = new CatModel({
      name: 'Bob',
      color: 'black',
      hobbies: [{
        likes: 12,
        activity: 'jumping'
      }],
      candy: newCandy._id
    });

    newColor.save((err) => {

      if(err) {
        return done(err, null);
      }

      newCandy.save((err) => {

        if(err) {
          return done(err, null);
        }

        newCat.save((err, doc) => {

          if(err) {
            return done(err, null);
          }

          setTimeout(() => {

            const type = doc.constructor.modelName;
            return expect(elasticsearch.getDoc(doc.id, type, testIndex))
              .to.eventually.be.fulfilled
              .then((res) => {

                expect(res._index).to.deep.equal(testIndex);
                expect(res._id).to.deep.equal(newCat.id);
                expect(res._type).to.deep.equal(newCat.constructor.modelName);
                expect(res._source.name).to.deep.equal(newCat.name);
                expect(res._source.color).to.deep.equal(undefined);
                expect(res._source.hobbies[0].likes).to.deep.equal(12);
                expect(res._source.hobbies[0].activity).to.deep.equal(undefined);
                expect(res._source.candy.name).to.deep.equal(newCandy.name);
                expect(res._source.candy.sugarAmount).to.deep.equal(undefined);
                expect(res._source.candy.wrappingColor.name).to.deep.equal(undefined);
                expect(res._source.candy.wrappingColor.lightness).to.deep.equal(128);

                return done();
              })
              .catch(done);

          }, elasticsearchTimeout);
        });
      });
    });
  });

  it('should not index a document of same schema, if model has not been registered before', (done) => {

    const newSuperCat = new SuperCatModel({name: 'Jenny'});

    newSuperCat.save((err, doc) => {

      if (err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(elasticsearch.getDoc(doc.id, type, testIndex))
          .to.be.rejectedWith(errors.DocumentNotFoundError)
          .then(() => done());

      }, elasticsearchTimeout);
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

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });


  it('should delete a mongoose document from Elasticsearch when it has been removed', (done) => {

    const newCat = new CatModel({name: 'Jeff', hobby: 'woof'});

    newCat.save((err, doc) => {

      if(err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(elasticsearch.getDoc(doc.id, type, testIndex))
          .to.eventually.be.fulfilled
          .then(() => {

            newCat.remove((err) => {

              if(err) {
                return done(err, null);
              }

              setTimeout(() => {

                return expect(elasticsearch.getDoc(doc.id, type, testIndex))
                  .to.be.rejectedWith(errors.DocumentNotFoundError)
                  .then(() => done());
              }, elasticsearchTimeout);
            });
          })
          .catch(done);
      }, elasticsearchTimeout);
    });
  });

  it('should not attempt to delete a mongoose document from unregistered models', (done) => {

    const newSuperCat = new SuperCatModel({name: 'Bing', hobby: 'swoosh'});

    newSuperCat.save((err, doc) => {

      if(err) {
        return done(err, null);
      }

      // TODO Add event to listen on

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(elasticsearch.getDoc(doc.id, type, testIndex))
          .to.be.rejectedWith(errors.DocumentNotFoundError)
          .then(() => {

            newSuperCat.remove((err) => {

              if(err) {
                return done(err, null);
              }

              setTimeout(() => {

                return expect(elasticsearch.getDoc(doc.id, type, testIndex))
                  .to.be.rejectedWith(errors.DocumentNotFoundError)
                  .then(() => done());
              }, elasticsearchTimeout);
            });
          })
          .catch(done);
      }, elasticsearchTimeout);
    });
  });
});


