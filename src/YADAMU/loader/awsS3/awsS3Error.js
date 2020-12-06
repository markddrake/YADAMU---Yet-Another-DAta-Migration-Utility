"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class AWSS3Error extends DatabaseError {
  
  constructor(cause,stack,operation) {
	if ((cause.message === null) && (cause.code = 'NotFound')) {
	  cause.message = 'Resource Not Found';
	}
    super(cause,stack,operation);
  }

  possibleConsistencyError() {
	return this.cause.code === 'NotFound'
  }
}

module.exports = AWSS3Error