"use strict"

import {DatabaseError} from '../../core/yadamuException.js'

class MariadbError extends DatabaseError {

  constructor(driverId,cause,stack,sql) {
    super(driverId,cause,stack,sql);
	if (this.sql.indexOf('?),(?') > 0) {
	  const startElipises = this.sql.indexOf('?),(?') + 3
	  const endElipises =  this.sql.lastIndexOf('?),(?') + 3
	  this.sql = this.sql.substring(0,startElipises) + '(...),' + this.sql.substring(endElipises);
	}
  }
  
  lostConnection() {
	const knownErrors = ['ECONNRESET','ER_CMD_CONNECTION_CLOSED','ER_SOCKET_UNEXPECTED_CLOSE','ER_GET_CONNECTION_TIMEOUT']
    return (this.cause.code && ((knownErrors.indexOf(this.cause.code) > -1) || (this.cause.fatal && (this.cause.code === 'UNKNOWN') && (this.cause.errno && (this.cause.errno = -4077)))))
  }
  
  serverUnavailable() {
	const knownErrors = ['ECONNREFUSED','ER_GET_CONNECTION_TIMEOUT']
    return (this.cause.code && (knownErrors.indexOf(this.cause.code) > -1))
  }
    	   
  missingTable() {
    return ((this.cause.code && (this.cause.code === 'ER_NO_SUCH_TABLE')) && (this.cause.errno && (this.cause.errno === 1146)) && (this.cause.sqlState && (this.cause.sqlState === '42S02')))
  }

}
export { MariadbError as default }