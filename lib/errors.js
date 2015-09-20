'use strict';

const createError = require('custom-error-generator');

const errorCodes = new Map();

errorCodes.set('invalid-host', 'Invalid host name');
errorCodes.set('invalid-index-name', 'Invalid index name');
errorCodes.set('invalid-index-settings', 'Invalid index settings');
errorCodes.set('invalid-type', 'Invalid type');
errorCodes.set('invalid-mapping', 'Invalid mapping');
errorCodes.set('invalid-model-name', 'Invalid model name');
errorCodes.set('invalid-query', 'Invalid query');
errorCodes.set('invalid-document-id', 'Invalid document id');
errorCodes.set('invalid-options', 'Invalid options');
errorCodes.set('invalid-settings', 'Invalid elasticsearch index settings');
errorCodes.set('invalid-schema', 'Invalid mongoose schema');
errorCodes.set('invalid-model', 'Invalid mongoose model');
errorCodes.set('invalid-transform', 'Invalid transform function');
errorCodes.set('invalid-bulk-timeout', 'Invalid bulk size');
errorCodes.set('invalid-bulk-size', 'Invalid bulk timeout');
errorCodes.set('invalid-bulk-buffer-size', 'Invalid bulk buffer size');
errorCodes.set('missing-document-model', 'No matching registered model found for document');
errorCodes.set('invalid-search-query', 'Invalid search query');

const InvalidArgumentError = createError('InvalidArgumentError', null, function(code) {
  this.code = code;
  this.message = errorCodes.get(code);
});

const IndexNotFoundError = createError('IndexNotFoundError');
const DocumentNotFoundError = createError('DocumentNotFoundError');
const IndexOperationError = createError('IndexOperationError');
const ModelNotFoundError = createError('ModelNotFoundError');
const InvalidMappingError = createError('InvalidMappingError');
const BufferOverflowError = createError('BufferOverflowError');

module.exports.InvalidArgumentError = InvalidArgumentError;
module.exports.IndexNotFoundError = IndexNotFoundError;
module.exports.DocumentNotFoundError = DocumentNotFoundError;
module.exports.IndexOperationError = IndexOperationError;
module.exports.ModelNotFoundError = ModelNotFoundError;
module.exports.InvalidMappingError = InvalidMappingError;
module.exports.BufferOverflowError = BufferOverflowError;
