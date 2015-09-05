"use strict";

const createError = require('custom-error-generator');
const InvalidArgumentError = createError('InvalidArgumentError');

module.exports.InvalidArgumentError = InvalidArgumentError;