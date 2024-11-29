
import HTTP from 'http'

import {DatabaseError} from '../../core/yadamuException.js'
import AzureConstants from './azureConstants.js'

class AzureError extends DatabaseError {
  
  constructor(dbi,cause,stack,operation) {
	if (cause.message === '' && HTTP.STATUS_CODES.hasOwnProperty(cause.statusCode.toString())) {
	 const message = `HTTP Status: ${HTTP.STATUS_CODES[cause.statusCode.toString()]} [${operation}] [${cause.statusCode}]`
     cause.message = message
	}  
	
    super(dbi,cause,stack,operation);
	this.path = this.sql
	delete this.sql
  }

  notFound() {
	return (this.cause.statusCode === 404)
  }

}

export {AzureError as default }