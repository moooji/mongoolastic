'use strict';

const createError = require('custom-error-generator');

const codes = {
  'invalid-index-name': 'Invalid index name',
  'invalid-index-settings': 'Invalid index settings',
  'invalid-type': 'Invalid type',
  'invalid-mapping': 'Invalid mapping',
  'invalid-model-name': 'Invalid model name',
  'invalid-query': 'Invalid query',
  'invalid-document-id': 'Invalid document id',
  'invalid-options': 'Invalid options',
  'invalid-settings': 'Invalid elasticsearch index settings',
  'invalid-schema': 'Invalid mongoose schema',
  'invalid-model': 'Invalid mongoose model'
};

const InvalidArgumentError = createError('InvalidArgumentError', null, function(code) {
  this.code = code;
  this.message = codes[code];
});

const IndexNotFoundError = createError('IndexNotFoundError');
const DocumentNotFoundError = createError('DocumentNotFoundError');
const IndexOperationError = createError('IndexOperationError');

module.exports.InvalidArgumentError = InvalidArgumentError;
module.exports.IndexNotFoundError = IndexNotFoundError;
module.exports.DocumentNotFoundError = DocumentNotFoundError;
module.exports.IndexOperationError = IndexOperationError;