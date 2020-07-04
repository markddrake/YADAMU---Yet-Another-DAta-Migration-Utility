"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class MongoError extends DatabaseError {
  //  const err = new MongodbError(cause,sql)
  constructor(cause,stack,operation) {
    super(cause,null,sql)
  }
    
  lostConnection() {
	const knownErrors = [11600]
    const knownMessages = ["Cannot use a session that has ended"]
    return ((this.cause.code && (knownErrors.indexOf(this.cause.code) > -1)) || (knownMessages.indexOf(this.message) > -1))
  }

  serverUnavailable() {
	const knownErrors = [11600]
	const knownMessages = ["pool is draining, new operations prohibited"]
    return ((this.cause.code && (knownErrors.indexOf(this.cause.code) > -1)) || (knownMessages.indexOf(this.message) > -1))
  }
    	   
}

module.exports = MongoError
