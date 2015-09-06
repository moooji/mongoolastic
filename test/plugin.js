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
const catSchemaIndex = 'mongoolastic-test-plugin-cat';
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

const catSchemaSettings = {
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

const CatModel = mongoose.model('Cat', CatSchema);

const dogSchemaIndex = 'mongoolastic-test-plugin-dog';
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

const DogModel = mongoose.model('Dog', DogSchema);

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
      done();
    }

    mongoose.connection.on('error', onConnectionError);
    mongoose.connection.once('open', onConnectionOpen);
    mongoose.connect(connectionString, connectionOptions);

  });

  it('should register a schema with settings', function() {

    return expect(plugin.register(
      CatSchema,
      catSchemaIndex,
      catSchemaSettings))
      .to.eventually.be.fulfilled;
  });

  it('should register a schema without settings', function() {

    return expect(plugin.register(
      DogSchema,
      dogSchemaIndex))
      .to.eventually.be.fulfilled;
  });

  it('should throw an InvalidArgumentError if index is not valid', function() {

    return expect(plugin.register(
      CatSchema,
      123,
      catSchemaSettings))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw an InvalidArgumentError if settings are not valid', function() {

    return expect(plugin.register(
      CatSchema,
      catSchemaIndex,
      123))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });

  it('should throw an InvalidArgumentError if schema is not valid', function() {

    return expect(plugin.register(
      123,
      catSchemaIndex,
      catSchemaSettings))
      .to.be.rejectedWith(errors.InvalidArgumentError);
  });
});


describe('Plugin - Connect', function() {

  it('should connect to Elasticsearch', function() {

    return expect(plugin.connect(host))
      .to.eventually.be.fulfilled;
  });
});


describe('Plugin - Index', function() {

  before(function(done) {

    elasticsearch.ensureDeleteIndex([catSchemaIndex, dogSchemaIndex])
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

            return expect(elasticsearch.getDoc(doc.id, type, catSchemaIndex))
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

            return expect(elasticsearch.getDoc(doc.id, type, dogSchemaIndex))
              .to.eventually.be.fulfilled
              .then(() => done())
              .catch(done);
          });
      }, elasticsearchTimeout);
    });
  });
});