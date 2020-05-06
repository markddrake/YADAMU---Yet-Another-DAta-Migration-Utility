"use strict"

class YadamuError extends Error {
  constructor(message) {
    super(message);
  }
}

class UserError extends Error {
  constructor(message) {
    super(message);
  }
}

class CommandLineError extends UserError {
  constructor(message) {
    super(message);
  }
}

class ConfigurationFileError extends UserError {
  constructor(message) {
    super(message);
  }
}

class ConnectionError extends UserError {
  constructor(cause,connectionProperties) {
    super(cause.message);
	this.cause = cause;
	this.stack = cause.stack
	this.connectionProperties = Object.assign({},connectionProperties,{password: "************"})
  }
}

class DatabaseError extends Error {
  constructor(cause,stack,sql,) {
    super(cause.message);
	this.oneLineMessage = this.message.indexOf('\r') > 0 ? this.message.substr(0,this.message.indexOf('\r')) : this.message
	this.oneLineMessage = this.oneLineMessage.indexOf('\n') > 0 ? this.oneLineMessage.substr(0,this.oneLineMessage.indexOf('\n')) : this.oneLineMessage
    this.stack = `${stack.slice(0,5)}: ${cause.message}${stack.slice(5)}`
	this.sql = sql
	this.cause = cause
	Object.keys(cause).forEach(function(key){
	  if ((!this.hasOwnProperty(key))&& !(cause.key instanceof Error) && !(cause.key instanceof Function)) {
		this[key] = cause[key]
	  }
    },this)
	
  }
  
  lostConnection() {
	return false;
  }

  serverUnavailable() {
	return false;
  }
  
  missingTable() {
	return false;
  }
}

class OracleError extends DatabaseError {
  //  const err = new OracleError(cause,stack,sql,args,outputFormat)
  constructor(cause,stack,sql,args,outputFormat) {
    super(cause,stack,sql);
	this.args = args
	this.outputFormat = outputFormat
	
  }

  lostConnection() {
	const knownErrors = [3113,3114,3135]
    return (this.cause.errorNum && (knownErrors.indexOf(this.cause.errorNum) > -1))
  }

  serverUnavailable() {
	const knownErrors = [1109,12514,12528,12537,12541]
	return (this.cause.errorNum && (knownErrors.indexOf(this.cause.errorNum) > -1))
  }
 
  missingTable() {
	return (this.cause.errorNum && ((this.cause.errorNum === 942)))
  }
 
}

class MsSQLError extends DatabaseError {
  //  const err = new MsSQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

  lostConnection() {
	const knownErrors = ['ETIMEOUT','ESOCKET','EINVALIDSTATE'] 
	// console.log(this.cause.name,this.cause.code,this.cause.message,this.cause.message.indexOf('Connection lost - '))
	let cause = this.cause
	if (cause.name ===  'RequestError') {
	  cause = cause.originalError 
	  if (cause.info instanceof Error) {
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

  serverUnavailable() {
	const knownErrors = ['ETIMEOUT','ESOCKET'] 
	// console.log(this.cause.name,this.cause.code,this.cause.message,this.cause.message.indexOf('Connection lost - '))
	let cause = this.cause
	if (cause.name ===  'RequestError') {
	  cause = cause.originalError 
	  if (cause.info instanceof Error) {
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

  missingTable() {
	return ((this.cause.name === 'RequestError') && ((this.cause.code && (this.cause.code === 'EREQUEST'))) && ((this.cause.number && (this.cause.number === 208))))
  }

}

class PostgresError extends DatabaseError {
  //  const err = new PostgresError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

  lostConnection() {
	return ((this.cause.severity && (this.cause.severity === 'FATAL')) && (this.cause.code && (this.cause.code === '57P01')) || ((this.cause.name === 'Error') && (this.cause.message === 'Connection terminated unexpectedly')))
  }

  missingTable() {
	return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '42P01')))
  }

}

class MySQLError extends DatabaseError {
  //  const err = new MySQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }
  
  lostConnection() {
	const knownErrors = ['ECONNRESET','PROTOCOL_CONNECTION_LOST','ER_CMD_CONNECTION_CLOSED','ER_SOCKET_UNEXPECTED_CLOSE','ER_GET_CONNECTION_TIMEOUT']
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

class MariadbError extends DatabaseError {
  //  const err = new MariadbError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
	// console.log('MaraidbError',cause)
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

class SnowFlakeError extends DatabaseError {
  //  const err = new SnowFlakeError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

}

class MongodbError extends DatabaseError {
  //  const err = new MongodbError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }
}

module.exports = {
  YadamuError
, UserError
, DatabaseError
, CommandLineError  
, ConfigurationFileError
, ConnectionError
, OracleError
, MsSQLError
, MySQLError
, MariadbError
, PostgresError
, MongodbError
, SnowFlakeError
}