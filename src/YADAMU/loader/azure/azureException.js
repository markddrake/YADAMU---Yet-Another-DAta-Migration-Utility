"use strict"

const {DatabaseError} = require('../../common/yadamuException.js')

class AzureError extends DatabaseError {
  
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

  possibleConsistencyError() {
	return ((this.cause.statusCode === 404) && (this.cause.details.errorCode = 'BlobNotFound'))
  }

}

module.exports = AzureError