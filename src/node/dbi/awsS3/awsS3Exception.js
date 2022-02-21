"use strict"

import {DatabaseError} from '../../core/yadamuException.js'

class AWSS3Error extends DatabaseError {
  
  constructor(driverId,cause,stack,url) {
	if ((cause.message === null) && (cause.code === 'NotFound')) {
	  cause.message = `Resource "${url}" Not Found`;
	}
    super(driverId,cause,stack,url);
	this.path = this.sql
	delete this.sql
  }

  FileNotFound() {
	return this.cause.code === 'NotFound'
  }

  possibleConsistencyError() {
	return this.cause.code === 'NotFound'
  }
}

export {AWSS3Error as default }