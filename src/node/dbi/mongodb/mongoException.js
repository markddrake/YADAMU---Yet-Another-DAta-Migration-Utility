
import {
  DatabaseError
}                       from '../../core/yadamuException.js'

import MongoConstants   from './mongoConstants.js'

class MongoError extends DatabaseError {

  constructor(driverId,cause,stack,operation) {
    super(driverId,cause,stack,operation)
  }
    
  lostConnection() {
	return ((this.cause.code && (MongoConstants.SESSION_ENDED_ERROR.includes(this.cause.code)) || MongoConstants.SESSION_ENDED_MESSAGE.includes(this.message)))
  }

  serverUnavailable() {
	const knownErrors = Object.freeze([11600])
	const knownMessages = Object.freeze(["pool is draining, new operations prohibited"])
    return ((this.cause.code && (MongoConstants.SERVER_UNAVAILABLE_ERROR.includes(this.cause.code)) || MongoConstants.SERVER_UNAVAILABLE_MESSAGE(this.message)))
  }
    	   
  contentTooLarge() {
    return ((this.cause.code && MongoConstants.CONTENT_TOO_LARGE_ERROR.includes(this.cause.code)) || (MongoConstants.CONTENT_TOO_LARGE_MESSAGE.includes(this.cause.message)))
  }		   
}

export { MongoError as default }