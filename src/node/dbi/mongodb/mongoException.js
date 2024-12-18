
import {
  DatabaseError
}                       from '../../core/yadamuException.js'

import MongoConstants   from './mongoConstants.js'

class MongoError extends DatabaseError {

  constructor(dbi,cause,stack,operation) {
    super(dbi,cause,stack,operation)
  }
    
  lostConnection() {
	return ((this.cause.code && (MongoConstants.SESSION_ENDED_ERROR.includes(this.cause.code)) || MongoConstants.SESSION_ENDED_MESSAGE.includes(this.message)))
  }

  serverUnavailable() {
    return ((this.cause.code && (MongoConstants.SERVER_UNAVAILABLE_ERROR.includes(this.cause.code)) || MongoConstants.SERVER_UNAVAILABLE_MESSAGE(this.message)))
  }
    	   
  contentTooLarge() {
    return ((this.cause.code && MongoConstants.CONTENT_TOO_LARGE_ERROR.includes(this.cause.code)) || (MongoConstants.CONTENT_TOO_LARGE_MESSAGE.includes(this.cause.message)))
  }		   
}

export { MongoError as default }