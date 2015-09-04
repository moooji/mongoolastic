"use strict";

var Promise = require('bluebird');
var async = require('async');
var util = require('util');
var _ = require('lodash');
var elasticsearch = require('elasticsearch');
var EventEmitter = require('events').EventEmitter;
var indices = require('./lib/indices');

var _es = null;
var _prefix = null;
var _indices = null;

var Mongoolastic = function() {
  this.connection = null;
  this.prefix = null;
};

util.inherits(mongolastic, EventEmitter);