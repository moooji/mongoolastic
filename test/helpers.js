'use strict';

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const mongoose = require('mongoose');
const helpers = require('../lib/helpers');
const errors = require('../lib/errors');

const expect = chai.expect;
chai.use(chaiAsPromised);


/**
 * Test data
 *
 */
const FoodSchema = new mongoose.Schema({
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
  proteinAmount: {
    type: Number
  }
});

const MusicSchema = new mongoose.Schema({
  genre: {
    type: String,
    required: true,
    elasticsearch: {
      mapping: {
        index: 'not_analyzed',
        type: 'string'
      }
    }
  },
  artist: {
    type: String
  }
});

const CowSchema = new mongoose.Schema({
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
  food: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Food',
    elasticsearch: {
      populate: true
    }
  },
  favoriteSongs: [MusicSchema]
});

mongoose.model('Music', MusicSchema);
const CowModel = mongoose.model('Cow', CowSchema);
const FoodModel = mongoose.model('Food', FoodSchema);

/**
 * Get model mapping
 *
 *
 */
describe('Helpers - Get model mapping', function() {

  const newFood = new FoodModel({name: 'grass', proteinAmount: 2});
  const newCow = new CowModel({
    name: 'Bob',
    food: newFood._id,
    hobby: 'chilling',
    favoriteSongs: [
      {
        genre: 'jazz',
        artist: 'Moo Armstrong'
      }, {
        genre: 'techno',
        artist: 'Scooter'
      }]
  });

  //console.log(newFood);
  //console.log(newCow);
  //console.log(helpers.getModelMapping(CowModel));
  //console.log(helpers.getModelMapping(MusicModel));

  it('should throw InvalidArgument error if supplied model argument is not a mongoose model', () => {

    const notValidModel = {notValid: 'model'};
    expect(() => helpers.getModelMapping(notValidModel))
      .to.throw(errors.InvalidArgumentError);
  });

  it('should create mappings', () => {
    expect(() => helpers.getModelMapping(CowModel))
      .to.not.throw();
  });
});