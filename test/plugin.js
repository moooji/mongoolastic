'use strict';

//var Promise = require('bluebird');
var chai = require('chai');
var Promise = require('bluebird');
var chaiAsPromised = require('chai-as-promised');
var mongoose = require('mongoose');
var elasticsearch = require('../lib/elasticsearch');
var plugin = require('../lib/plugin');
var errors = require('../lib/errors');

var expect = chai.expect;
chai.use(chaiAsPromised);

var host = 'localhost:9200';


/**
 * Test data
 *
 */
var catSchemaIndex = 'mongoolastic-test-cat';
var CatSchema = new mongoose.Schema({
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

var catSchemaSettings = {
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

var CatModel = mongoose.model('Cat', CatSchema);

var dogSchemaIndex = 'mongoolastic-test-dog';
var DogSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  color: {
    type: String,
    required: true
  }
});

var DogModel = mongoose.model('Dog', DogSchema);

/**
 * MongoDB
 *
 *
 *
 */
var connectionString = 'mongodb://localhost:27017/mongoolastic-test';
var connectionOptions = { server: { auto_reconnect: true }};


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

    // Make sure that indices do not exist yet
    elasticsearch.indexExists([catSchemaIndex, 'index-mooo'])
      .then(function(indexExists) {

        console.log(indexExists);
        done();
        /*
        if(indexExists){
          elasticsearch.deleteIndex(catSchemaIndex)
            .then(function(res) {
              return done();
            });
        }
        */
      });
  });


  it('should index a mongoose document when it has been saved', function(done) {

    var newCat = new CatModel({name: 'Bob', hobby: 'woof'});

    newCat.save(function(err, res) {

      if(err) {
        return done(err, null);
      }

      // Give Elasticsearch some to index
      setTimeout(function(){

        return Promise.resolve(res)
          .then(function(doc) {

            var type = doc.constructor.modelName;

            return expect(elasticsearch.getDoc(doc.id, type, 'mongoolastic-test-cat'))
              .to.eventually.be.fulfilled
              .then(function(res) {
                return done(null, res);
              })
              .catch(function(err) {
                return done(err, null);
              });
          });
      }, 200);
    });
  });

  it('should index a mongoose document when it has been saved', function(done) {

    var newDog = new DogModel({name: 'Bob', color: 'black'});

    newDog.save(function(err, res) {

      if(err) {
        return done(err, null);
      }

      // Give Elasticsearch some to index
      setTimeout(function() {

        return Promise.resolve(res)
          .then(function(doc) {

            var type = doc.constructor.modelName;

            return expect(elasticsearch.getDoc(doc.id, type, 'mongoolastic-test-dog'))
              .to.eventually.be.fulfilled
              .then(function(res) {
                return done(null, res);
              })
              .catch(function(err) {
                return done(err, null);
              });
          });
      }, 200);
    });
  });
});