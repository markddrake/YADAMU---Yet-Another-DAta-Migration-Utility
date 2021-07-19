"use strict"

const {DatabaseError} = require('../../common/yadamuException.js')

class AWSS3Error extends DatabaseError {
  
  constructor(cause,stack,url) {
	if ((cause.message === null) && (cause.code = 'NotFound')) {
	  cause.message = 'Resource Not Found';
	}
    super(cause,stack,url);
	this.path = this.sql
	delete this.sql
  }

  possibleConsistencyError() {
	return this.cause.code === 'NotFound'
  }
}

module.exports = AWSS3Error