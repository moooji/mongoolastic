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
  ingredients: [{
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Ingredient'
  }],
  proteinAmount: {
    type: Number
  }
});

const IngredientSchema = new mongoose.Schema({
  stockLevel: {
    type: Number,
    elasticsearch: {
      mapping: {
        type: 'integer'
      }
    }
  },
  taste: {
    type: String
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
      }
    }
  },
  hobby: {
    type: String
  },
  food: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Food',
    elasticsearch: {
    }
  },
  favoriteSongs: [MusicSchema]
});

mongoose.model('Music', MusicSchema);
const CowModel = mongoose.model('Cow', CowSchema);
const FoodModel = mongoose.model('Food', FoodSchema);
const IngredientModel = mongoose.model('Ingredient', IngredientSchema);

/**
 * Render model mapping
 *
 *
 */
describe('Helpers - Render mapping', function() {

  /*
  const newIngredient = new IngredientModel({stockLevel: 12, taste: 'yummy'});
  const newFood = new FoodModel({name: 'grass', proteinAmount: 2, ingredients: [newIngredient._id]});

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
  */

  const expected = {
    properties: {
      name: {
        type: 'string',
        index: 'not_analyzed'
      },
      food: {
        properties: {
          name: {
            type: 'string',
            index: 'not_analyzed'
          },
          ingredients: {
            properties: {
              stockLevel: {
                type: 'integer'
              }
            }
          }
        }
      },
      favoriteSongs: {
        properties: {
          genre: {
            type: 'string',
            index: 'not_analyzed'
          }
        }
      }
    }
  };

  const populationModels = new Map();
  populationModels.set(FoodModel.modelName, FoodModel);
  populationModels.set(IngredientModel.modelName, IngredientModel);

  it('should throw InvalidArgumentError if supplied schema is not valid mongoose schema', () => {
    return expect(() => helpers.renderMapping({notValid: true}, populationModels))
      .to.throw(errors.InvalidArgumentError);
  });

  it('should parse a schema with population and sub documents', () => {
    return expect(helpers.renderMapping(CowModel.schema, populationModels))
      .to.deep.equal(expected);
  });
});

/**
 * Render model population tree
 *
 *
 */
describe('Helpers - Render population tree', function() {

  const expected = {
    food: {
      fields: [
        'name', 'ingredients'
      ],
      paths: {
        ingredients: {
          fields: [
            'stockLevel'
          ]
        }
      }
    }
  };

  const populationModels = new Map();
  populationModels.set(FoodModel.modelName, FoodModel);
  populationModels.set(IngredientModel.modelName, IngredientModel);

  it('should throw InvalidArgumentError if supplied schema is not valid mongoose schema', () => {
    return expect(() => helpers.renderPopulationTree({notValid: true}, populationModels))
      .to.throw(errors.InvalidArgumentError);
  });

  it('should render population tree with population and sub documents', () => {

    console.log(JSON.stringify(helpers.renderPopulationTree(CowModel.schema, populationModels), null, 2));
    return expect(helpers.renderPopulationTree(CowModel.schema, populationModels))
      .to.deep.equal(expected);
  });
});