
import {DatabaseError} from '../../core/yadamuException.js'

class AWSS3Error extends DatabaseError {
  
  constructor(dbi,cause,stack,operation) {
	if (cause.Code && (cause.Code === 'NoSuchKey')) {
	  cause.message = `${cause.message} [${operation}] [${cause.$metadata.httpStatusCode}] `;
	}
    super(dbi,cause,stack,operation);
	this.path = this.sql
	delete this.sql
  }

  notFound() {
	return this.cause.$metadata.httpStatusCode === 404
  }

  unauthorized() {
	return this.cause.$metadata.httpStatusCode === 403
  }

  possibleConsistencyError() {
	return this.cause.code === 'NotFound'
  }
}

export {AWSS3Error as default }