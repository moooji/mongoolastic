'use strict';

const chai = require('chai');
const Bluebird = require('bluebird');
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
      type: 'string'
    }
  },
  hobby: {
    type: String
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

const CatModel = mongoose.model('Cat', CatSchema);
const SuperCatModel = mongoose.model('SuperCat', CatSchema);
const DogModel = mongoose.model('Dog', DogSchema);

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


/**
 * Tests
 *
 *
 */
describe('Plugin - Register', function() {

  before(function(done){

    function onConnectionError(err) {
      throw err;
    }

    function onConnectionOpen() {
      return done();
    }

    mongoose.connection.on('error', onConnectionError);
    mongoose.connection.once('open', onConnectionOpen);
    mongoose.connect(connectionString, connectionOptions);

  });

  it('should register the CatModel', function() {

    return expect(plugin.registerModel(CatModel))
      .to.eventually.be.fulfilled;
  });

  it('should register the DogModel', function() {

    return expect(plugin.registerModel(DogModel))
      .to.eventually.be.fulfilled;
  });
});


describe('Plugin - Connect', function() {

  it('should connect to Elasticsearch', function() {

    return expect(plugin.connect(host, testIndex, testIndexSettings))
      .to.eventually.be.fulfilled;
  });
});


describe('Plugin - Index', function() {

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

      // TODO check MongoDB

      setTimeout(() => {

        return Bluebird.resolve(doc)
          .then((doc) => {

            const type = doc.constructor.modelName;

            return expect(elasticsearch.getDoc(doc.id, type, testIndex))
              .to.eventually.be.fulfilled
              .then(() => done())
              .catch(done);
          });
      }, elasticsearchTimeout);
    });
  });

  it('should index a mongoose document when it has been saved', function(done) {

    const newDog = new DogModel({name: 'Bob', color: 'black'});

    newDog.save((err, doc) => {

      if(err) {
        return done(err, null);
      }

      // TODO check MongoDB

      setTimeout(() => {

        return Bluebird.resolve(doc)
          .then((doc) => {

            const type = doc.constructor.modelName;

            return expect(elasticsearch.getDoc(doc.id, type, testIndex))
              .to.eventually.be.fulfilled
              .then(() => done())
              .catch(done);
          });
      }, elasticsearchTimeout);
    });
  });

  it('should not index a document of same schema, if model has not been registered before', function(done) {

    const newSuperCat = new SuperCatModel({name: 'Jenny'});

    newSuperCat.save((err, doc) => {

      if (err) {
        return done(err, null);
      }

      // TODO check MongoDB

      setTimeout(() => {

        return Bluebird.resolve(doc)
          .then((doc) => {

            const type = doc.constructor.modelName;

            return expect(elasticsearch.getDoc(doc.id, type, testIndex))
              .to.be.rejectedWith(errors.DocumentNotFoundError)
              .then(() => done());
          });
      }, elasticsearchTimeout);
    });
  });
});