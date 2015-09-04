"use strict";

var assert = require('assert');
var should = require('should');
var mongoose = require('mongoose');
var mongoolastic = require('../');

describe('mongoolastic', function() {
  //mongoose.set('debug', true);
  var cat;
  var CatSchema;
  var CostumeSchema;
  var costume;
  var DogSchema;
  var dog;
  var FailSchema;
  var myFail;
  var SettingsTestSchema;
  var settingsTest;

  before(function() {
    mongoose.connect('mongodb://localhost/mongoolastic');

    CostumeSchema = mongoose.Schema({
      name: {type: String},
      color: {type: String},
      integer: {type: Number, elastic: {mapping: {type: 'integer'}}}
    });
    CostumeSchema.plugin(mongoolastic.plugin, {modelName: 'costume'});
    costume = mongoose.model('costume', CostumeSchema);

    CatSchema = mongoose.Schema({
      name: String,
      date: {type: Date, default: Date.now},
      costume: {type: mongoose.Schema.ObjectId, ref: 'costume', elastic: {popfields: 'name'}},
      url: {type: String, elastic: {mapping: {type: 'string', index: 'not_analyzed'}}},
      test: {
        integer: {type: Number, elastic: {mapping: {type: 'integer'}}},
        deep: {
          mystring: {type: String, elastic: {mapping: {type: 'string'}}}
        }
      }
    });
    CatSchema.plugin(mongoolastic.plugin, {modelName: 'cat'});
    cat = mongoose.model('cat', CatSchema);

    cat.elastic = {
      mapping: {
        'location.geo': {type: 'geo_point', 'lat_lon': true}
      }
    };

    DogSchema = mongoose.Schema({
      name: String,
      date: {type: Date, default: Date.now},
      costume: {type: mongoose.Schema.ObjectId, ref: 'costume'}
    });
    DogSchema.plugin(mongoolastic.plugin, {modelName: 'dog'});
    dog = mongoose.model('dog', DogSchema);

    FailSchema = mongoose.Schema({
      name: String,
      keyword: {type: String, required: true}
    });
    FailSchema.plugin(mongoolastic.plugin, {modelName: 'fail'});
    myFail = mongoose.model('fail', FailSchema);

    // Settings test
    SettingsTestSchema = mongoose.Schema({
      name: String
    });
    SettingsTestSchema.plugin(mongoolastic.plugin, {modelName: 'settingsTest'});
    settingsTest = mongoose.model('settingsTest', SettingsTestSchema);

  });

  describe('mongoolastic', function() {
    it('should be a object', function() {
      assert('object' === typeof mongoolastic);
    });
  });

  describe('create connection', function() {
    it('should create a connection', function(done) {
      mongoolastic.connect('mongoolastic', {
        host: 'localhost:9200',
        sniffOnStart: true
      }, function(err, conn) {
        should.not.exist(err);
        conn.should.be.an.Object();
        done();
      });
    });

    it('should create the mapping for the cat model', function(done) {
      mongoolastic.registerModel(cat, function(err, result) {
        should.not.exist(err);
        result.should.be.a.Function();
        done();
      });
    });

    it('should create the mapping for the dog model', function(done) {
      mongoolastic.registerModel(dog, function(err, result) {
        should.not.exist(err);
        result.should.be.a.Function();
        done();
      });
    });

    it('should create the mapping for the costume model', function(done) {
      mongoolastic.registerModel(costume, function(err, result) {
        should.not.exist(err);
        result.should.be.a.Function();
        done();
      });
    });

    it('should create the mapping for the myFail model', function(done) {
      mongoolastic.registerModel(myFail, function(err, result) {
        should.not.exist(err);
        result.should.be.a.Function();
        done();
      });
    });

    it('should return the mappings for the cat model', function(done) {
      mongoolastic.indices.getMapping(cat.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongoolastic-cat'].should.be.an.Object();
        response['mongoolastic-cat'].mappings.should.be.an.Object();
        response['mongoolastic-cat'].mappings.cat.should.be.an.Object();
        done();
      });
    });

    it('should create the mapping for the settingsTest model', function(done) {

      var options = {
        'settings': {
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
        }
      };

      mongoolastic.registerModel(settingsTest, options, function(err, result) {
        should.not.exist(err);
        result.should.be.a.Function();
        done();
      });
    });

    it('should return the custom index settings for the settingsTest model', function(done) {
      mongoolastic.indices.getSettings(settingsTest.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongoolastic-settingstest'].should.be.an.Object();
        response['mongoolastic-settingstest'].settings.should.be.an.Object();
        response['mongoolastic-settingstest'].settings.index.should.be.an.Object();
        response['mongoolastic-settingstest'].settings.index['uuid'].should.be.a.String();
        response['mongoolastic-settingstest'].settings.index['analysis'].should.be.an.Object();
        done();
      });
    });

    it('should return default index settings for the cat model', function(done) {
      mongoolastic.indices.getSettings(cat.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongoolastic-cat'].should.be.an.Object();
        response['mongoolastic-cat'].settings.should.be.an.Object();
        response['mongoolastic-cat'].settings.index.should.be.an.Object();
        response['mongoolastic-cat'].settings.index['uuid'].should.be.a.String();
        response['mongoolastic-cat'].settings.index.should.not.have.property('analysis');
        done();
      });
    });
  });

  describe('save mongoose model', function() {
    var kitty;

    it('should create a new object in mongoose model', function(done) {
      kitty = new cat({name: 'Zildjian'});
      kitty.save(function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object();
        done();
      });
    });

    it('should update a mongoose object', function(done) {
      kitty.name = 'Zlatko';
      kitty.save(function(err, result) {
        should.not.exist(err);
        result['name'].should.equal('Zlatko');

        // Add timeout after saving to give elasticsearch some time to index
        setTimeout(function() {

          // Check that document has been updated in elasticsearch
          var query = {
            'body': {
              'query': {
                'match': {'_id': kitty.id}
              }
            }
          };

          kitty.search(query, function(err, result) {
            should.not.exist(err);
            var hit = result.hits.hits[0];
            hit['_id'].should.equal(kitty.id);
            done();
          });

        }, 1000);
      });
    });

    it('should find the mongoose object', function(done) {

      var query = {
        'body': {
          'query': {
            'match': {'_id': kitty.id}
          }
        }
      };

      kitty.search(query, function(err, result) {
        should.not.exist(err);
        var hit = result.hits.hits[0];
        hit['_id'].should.equal(kitty.id);
        done();
      });
    });

    it('should delete from index', function(done) {
      mongoolastic.delete('cat', kitty.id, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object();
        done();
      });
    });

    it('should return an error when trying to delete and id is not a string', function(done) {
      mongoolastic.delete('cat', kitty._id, function(err, result) {
        should.exist(err);
        err.should.be.an.Error();
        should.equal(null, result);
        done();
      });
    });

    it('should reindex mongoose object', function(done) {
      mongoolastic.index('cat', kitty, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object();
      });
      // Add timeout after saving to give elasticsearch some time to index
      setTimeout(function() {
        done();
      }, 1000);
    });

    it('should sync mongodb', function(done) {
      cat.sync(function(errcount, resultcount) {
        errcount.should.eql(0);
        resultcount.should.eql(1);
        done();
      });
    });

    it('should delete the mongoose object', function(done) {
      kitty.remove(function(err) {
        should.not.exist(err);
        done();
      });
    });

    var bat;
    it('should create a new sub object in mongoose model', function(done) {
      bat = new costume({
        name: 'Batman',
        color: 'black'
      });

      bat.save(function(err, res) {
        should.not.exist(err);
        res.should.be.an.Object();
        res['_id'].should.be.an.Object();
        res['name'].should.equal('Batman');
        res['color'].should.equal('black');
        done();
      });
    });

    it('should create a object with sub object in mongoose model', function(done) {
      var batCat = new cat({
        name: 'Batcat',
        costume: bat._id
      });

      batCat.save(function(err, res) {

        // Add timeout after saving to give elasticsearch some time to index
        setTimeout(function() {

          should.not.exist(err);
          res['_id'].should.be.an.Object();
          res['name'].should.equal('Batcat');

          var costume = res['costume'];
          costume['_id'].should.be.an.Object();
          costume['name'].should.equal('Batman');
          should.not.exist(costume['color']);
          done();

        }, 1000);
      });
    });

    it('should create a new object with sub object in mongoose without specified popfields', function(done) {
      var dogBat = new dog({
        name: 'DogBat',
        costume: bat._id
      });

      dogBat.save(function(err, res) {

        // Add timeout after saving to give elasticsearch some time to index
        setTimeout(function() {

          should.not.exist(err);
          res['_id'].should.be.an.Object();
          res['name'].should.equal('DogBat');

          var costume = res['costume'];
          costume['_id'].should.be.an.Object();
          costume['name'].should.equal('Batman');
          costume['color'].should.equal('black');
          done();

        }, 1000);
      });
    });

    it('should return the mappings for the cat model', function(done) {
      mongoolastic.indices.getMapping(cat.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongoolastic-cat'].should.be.an.Object();
        response['mongoolastic-cat'].mappings.should.be.an.Object();
        response['mongoolastic-cat'].mappings.cat.should.be.an.Object();
        response['mongoolastic-cat'].mappings.cat.properties.should.be.an.Object();
        response['mongoolastic-cat'].mappings.cat.properties.test.should.be.an.Object();
        response['mongoolastic-cat'].mappings.cat.properties.test.properties.should.be.an.Object();
        response['mongoolastic-cat'].mappings.cat.properties.test.properties.deep.should.be.an.Object();
        response['mongoolastic-cat'].mappings.cat.properties.test.properties.deep.properties.should.be.an.Object();
        done();
      });
    });

    it('should return the correct prefix', function(done) {
      assert.equal(mongoolastic.getIndexName('model'), 'mongoolastic-model');
      assert.equal(mongoolastic.getIndexName('mongoolastic-model'), 'mongoolastic-model');
      done();
    });

    it('should fail to save invalid object and not add it to the search index', function(done) {
      var failObject = new myFail({
        title: 'test2name',
        lang: 'en'
      });

      failObject.save(function(saveErr, saveRes) {

        var query = {
          'body': {
            'query': {
              'match': {'_id': failObject.id}
            }
          }
        };

        failObject.search(query, function(searchErr, res) {
          should.exist(saveErr);
          should.equal(null, saveRes);
          should.not.exist(searchErr);
          assert(res.hits.total === 0);
          done();
        });
      });
    });
  });

  after(function(done) {
    mongoose.connection.db.dropDatabase(function() {
      mongoolastic.deleteIndex('*', function() {
        done();
      });
    });
  });
});
