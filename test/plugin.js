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
const elasticsearchTimeout = 200;

/**
 * Test data
 *
 */
const CatSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    elasticsearch: {
      mapping: {
        index: 'not_analyzed',
        type: 'string'
      },
      populate: true
    }
  },
  hobby: {
    type: String
  },
  candy: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Candy'
  }
});

const DogSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  color: {
    type: String,
    required: true
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
  }
});

const CatModel = mongoose.model('Cat', CatSchema);
const SuperCatModel = mongoose.model('SuperCat', CatSchema);
const DogModel = mongoose.model('Dog', DogSchema);
const CandyModel = mongoose.model('Candy', CandySchema);


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

  it('should register the CatModel and update mappings', function() {

    return expect(plugin.registerModel(CatModel))
      .to.eventually.be.fulfilled
      .then(() => {

        console.log(plugin.getMappings());
      });
  });

  it('should register the DogModel', function() {

    return expect(plugin.registerModel(DogModel))
      .to.eventually.be.fulfilled;
  });
});


/**
 * Register population
 *
 *
 */
describe('Plugin - Register population', function() {

  it('should register the a model for population', function() {

    return expect(plugin.registerPopulation(CandyModel))
      .to.eventually.be.fulfilled
      .then(() => {

        console.log(plugin.getMappings());
      });
  });
});


/**
 * Connect
 *
 *
 */
describe('Plugin - Connect', function() {

  it('should connect to Elasticsearch', function() {

    return expect(plugin.connect(host, testIndex, testIndexSettings))
      .to.eventually.be.fulfilled;
  });
});


/**
 * Index document
 *
 *
 */
describe('Plugin - Index document', function() {

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });


  it('should index a mongoose document when it has been saved', function(done) {

    const newCat = new CatModel({name: 'Bob', hobby: 'woof'});

    newCat.save(function(err, doc) {

      if(err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(elasticsearch.getDoc(doc.id, type, testIndex))
          .to.eventually.be.fulfilled
          .then(() => done())
          .catch(done);

      }, elasticsearchTimeout);
    });
  });

  it('should index a mongoose document when it has been saved', function(done) {

    const newDog = new DogModel({name: 'Bob', color: 'black'});

    newDog.save((err, doc) => {

      if(err) {
        return done(err, null);
      }

      setTimeout(() => {

        const type = doc.constructor.modelName;
        return expect(elasticsearch.getDoc(doc.id, type, testIndex))
          .to.eventually.be.fulfilled
          .then(() => done())
          .catch(done);

      }, elasticsearchTimeout);
    });
  });

  it('should not index a document of same schema, if model has not been registered before', function(done) {

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

  before(function(done) {

    elasticsearch.ensureDeleteIndex(testIndex)
      .then(() => done())
      .catch(done);
  });


  it('should delete a mongoose document from Elasticsearch when it has been removed', function(done) {

    const newCat = new CatModel({name: 'Jeff', hobby: 'woof'});

    newCat.save(function(err, doc) {

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

  it('should not attempt to delete a mongoose document from unregistered models', function(done) {

    const newSuperCat = new SuperCatModel({name: 'Bing', hobby: 'swoosh'});

    newSuperCat.save(function(err, doc) {

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


