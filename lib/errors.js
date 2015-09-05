'use strict';

const createError = require('custom-error-generator');

const codes = {
  'invalid-index-name': 'Invalid index name',
  'invalid-index-settings': 'Invalid index settings',
  'invalid-mapping': 'Invalid mapping',
  'invalid-model-name': 'Invalid model name',
  'invalid-query': 'Invalid query',
  'invalid-document-id': 'Invalid document id',
  'invalid-options': 'Invalid options',
  'invalid-settings': 'Invalid settings'
};

var InvalidArgumentError = createError('InvalidArgumentError', null, function (code) {
  this.code = code;
  this.message = codes[code];
});

module.exports.InvalidArgumentError = InvalidArgumentError;