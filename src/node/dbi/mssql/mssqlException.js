"use strict"

import {DatabaseError} from '../../core/yadamuException.js'
import MsSQLConstants from './mssqlConstants.js'

class MsSQLError extends DatabaseError {

  constructor(driverId,cause,stack,sql) {
    super(driverId,cause,stack,sql);
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

  requestInProgress() {
	return ((this.cause.name === 'TransactionError') && (this.cause.code && (this.cause.code === 'EREQINPROG')) )
  }

  cancelledOperation() {
	return ((this.cause.name === 'RequestError') && (this.cause.code && (this.cause.code === 'ECANCEL')) )
  }
  
  serverUnavailable() {
    let cause = this.getUnderlyingError()
	return ((cause.code &&  MsSQLConstants.LOST_CONNECTION_ERROR.includes(cause.code)) || ((this.cause.number) && (this.cause.number === 596)))
  }

  missingTable() {
	return ((this.cause.name === 'RequestError') && ((this.cause.code && (this.cause.code === 'EREQUEST'))) && ((this.cause.number && (this.cause.number === 208))))
  }
  
  suppressedError() {
	return (MsSQLConstants.SUPPRESSED_ERROR.includes(this.cause.message))
  }
  
  contentTooLarge() {
    let cause = this.getUnderlyingError()
    return ((cause?.info?.number &&  MsSQLConstants.CONTENT_TOO_LARGE_ERROR.includes(cause.info.number))) 
  }

}

export { MsSQLError as default }