"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class MsSQLError extends DatabaseError {
  //  const err = new MsSQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

  lostConnection() { 
    const knownErrors = ['ETIMEOUT','ESOCKET','EINVALIDSTATE'] 
	let cause = this.cause
	if (cause.name ===  'RequestError') {
      if (cause.number && (cause.number === 596)) {
        return true
      } 
      if (cause.originalError && (cause.originalError instanceof Error)) {
  	    cause = cause.originalError 
  	    if (cause.info && (cause.info instanceof Error)) {
		  cause = cause.info
        }
	  }
	  switch (cause.name) {
        case 'ConnectionError':
	    case 'TransactionError':
	      return (cause.code && (knownErrors.indexOf(cause.code) > -1))
        default:
	      return false; 
      }
	}
  }

  serverUnavailable() {
	const knownErrors = ['ETIMEOUT','ESOCKET'] 
	let cause = this.cause
	if ((cause.name ===  'RequestError') && (cause.info !== undefined)) {
	  cause = cause.originalError 
	  if (cause  && (cause.info instanceof Error)) {
		cause = cause.info
      }
	}
	switch (cause.name) {
      case 'ConnectionError':
	    return (cause.code && (knownErrors.indexOf(cause.code) > -1))
      default:
	    return false; 
	}
  }

  invalidConnection() {
	return ((this.cause.name === 'RequestError') && ((this.cause.code && (this.cause.code === 'EINVALIDSTATE'))))
  }

  missingTable() {
	return ((this.cause.name === 'RequestError') && ((this.cause.code && (this.cause.code === 'EREQUEST'))) && ((this.cause.number && (this.cause.number === 208))))
  }

}

module.exports = MsSQLError