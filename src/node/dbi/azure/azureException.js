"use strict"

import HTTP from 'http'

import {DatabaseError} from '../../core/yadamuException.js'
import AzureConstants from './azureConstants.js'

class AzureError extends DatabaseError {
  
  constructor(driverId,cause,stack,url) {
	if (cause.message === '' && HTTP.STATUS_CODES.hasOwnProperty(cause.statusCode.toString())) {
	 const message = `${HTTP.STATUS_CODES[cause.statusCode.toString()]} [${url}]`
     cause.message = message
	}  
    super(driverId,cause,stack,url);
	this.path = this.sql
	delete this.sql
  }

  FileNotFound() {
	return (this.cause.statusCode === 404)
  }

}

export {AzureError as default }