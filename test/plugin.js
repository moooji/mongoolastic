"use strict";

const chai = require('chai');
const chaiAsPromised = require("chai-as-promised");
const elasticsearch = require('../lib/plugin');
const errors = require('../lib/errors');

const InvalidArgumentError = errors.InvalidArgumentError;

const expect = chai.expect;
const should = chai.should;
chai.use(chaiAsPromised);

describe('Plugin', function() {

});