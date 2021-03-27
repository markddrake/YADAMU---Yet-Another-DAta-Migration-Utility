"use strict"

const {DatabaseError} = require('../../common/yadamuException.js')

class MongoError extends DatabaseError {
  //  const err = new MongodbError(cause,stack,operation)
  constructor(cause,stack,operation) {
    super(cause,stack,operation)
  }
    
  lostConnection() {
	const knownErrors = Object.freeze([11600])
    const knownMessages = Object.freeze(["Cannot use a session that has ended"])
    return ((this.cause.code && (knownErrors.includes(this.cause.code)) || knownMessages.includes(this.message)))
  }

  serverUnavailable() {
	const knownErrors = Object.freeze([11600])
	const knownMessages = Object.freeze(["pool is draining, new operations prohibited"])
    return ((this.cause.code && (knownErrors.includes(this.cause.code)) || knownMessages.includes(this.message)))
  }
    	   
  contentTooLarge() {
    const knownCodes = Object.freeze(['ERR_OUT_OF_RANGE'])
    const knownMessages = Object.freeze(['document is larger than the maximum size 16777216'])
    return ((this.cause.code && knownCodes.includes(this.cause.code)) || (knownMessages.includes(this.cause.message)))
  }		   
}

module.exports = MongoError
