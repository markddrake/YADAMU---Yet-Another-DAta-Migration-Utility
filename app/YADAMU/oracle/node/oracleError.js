"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class OracleError extends DatabaseError {
  //  const err = new OracleError(cause,stack,sql,args,outputFormat)
  constructor(cause,stack,sql,args,outputFormat) {
    super(cause,stack,sql);
	this.args = args
	this.outputFormat = outputFormat
	
  }

  invalidCredentials() { 
  }

  lostConnection() {
	const knownErrors = [3113,3114,3135,28,1012]
    return (this.cause.errorNum && (knownErrors.indexOf(this.cause.errorNum) > -1))
  }

  serverUnavailable() {
	const knownErrors = [1109,12514,12528,12537,12541]
	return (this.cause.errorNum && (knownErrors.indexOf(this.cause.errorNum) > -1))
  }

  invalidPool() {
	return this.cause.message.startsWith('NJS-002:')
  } 

  invalidConnection() {
	return this.cause.message.startsWith('NJS-003:')
  } 
  
  missingTable() {
	return (this.cause.errorNum && ((this.cause.errorNum === 942)))
  }
 
}

module.exports = OracleError
