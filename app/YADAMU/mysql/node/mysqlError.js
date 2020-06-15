"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class MySQLError extends DatabaseError {
  //  const err = new MySQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
	// Abbreviate Long Lists of Place Holders ...
  }
  
  lostConnection() {
	const knownErrors = ['ECONNRESET','PROTOCOL_CONNECTION_LOST','ER_CMD_CONNECTION_CLOSED','ER_SOCKET_UNEXPECTED_CLOSE','ER_GET_CONNECTION_TIMEOUT','PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR']
    return (this.cause.code && (knownErrors.indexOf(this.cause.code) > -1))
  }
  
  serverUnavailable() {
	const knownErrors = ['ECONNREFUSED','ER_GET_CONNECTION_TIMEOUT']
    return (this.cause.code && (knownErrors.indexOf(this.cause.code) > -1))
  }
    	     
  missingTable() {
    return ((this.cause.code && (this.cause.code === 'ER_NO_SUCH_TABLE')) && (this.cause.errno && (this.cause.errno === 1146)) && (this.cause.sqlState && (this.cause.sqlState === '42S02')))
  }

  spatialInsertFailed() {
	// MySQL could not decode spatial data in WKB format
    return ((this.cause.errno && this.cause.errno === 3037) && (this.cause.code && this.cause.code === 'ER_GIS_INVALID_DATA') && (this.cause.sqlState &&  this.cause.sqlState === '22023'))
  }
}

module.exports = MySQLError
