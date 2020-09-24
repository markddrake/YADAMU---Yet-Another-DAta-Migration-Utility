"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')
const MsSQLConstants = require('./mssqlConstants.js')

class MsSQLError extends DatabaseError {
  //  const err = new MsSQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

  getUnderlyingError() {
	let cause = this.cause;
	while (cause.originalError && (cause.originalError instanceof Error)) {
	  cause =  cause.originalError;
	}
	if (cause.info && (cause.info instanceof Error)) {
      cause = cause.info
	}
	return cause;
  }

  lostConnection() { 
    let cause = this.getUnderlyingError()
	return ((cause.code &&  MsSQLConstants.LOST_CONNECTION_ERROR.includes(cause.code)) || ((this.cause.number) && (this.cause.number === 596)))
  }

  serverUnavailable() {
    let cause = this.getUnderlyingError()
	return ((cause.code &&  MsSQLConstants.LOST_CONNECTION_ERROR.includes(cause.code)) || ((this.cause.number) && (this.cause.number === 596)))
  }

  invalidConnection() {
    let cause = this.getUnderlyingError()
    return (cause.code &&  MsSQLConstants.INVALID_CONNECTION_ERROR.includes(cause.code))
  }

  missingTable() {
	return ((this.cause.name === 'RequestError') && ((this.cause.code && (this.cause.code === 'EREQUEST'))) && ((this.cause.number && (this.cause.number === 208))))
  }
  
  suppressedError() {
	return (MsSQLConstants.SUPPRESSED_ERROR.includes(this.cause.message))
  }
}

module.exports = MsSQLError