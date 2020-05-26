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

class BatchInsertError extends Error {
  constructor(cause,tableName,sql,batchSize,firstRow,lastRow,additionalInfo) {
	super(cause.message)
	this.cause = cause
    this.tableName = tableName
	this.sql = sql,
	this.rows = [firstRow,`\n...\n`,lastRow]
	if (typeof additionalInfo === 'object') {
	  Object.assign(this,additionalInfo)
    }
  }
}
class DatabaseError extends Error {
  constructor(cause,stack,sql,) {
	let oneLineMessage = cause.message.indexOf('\r') > 0 ? cause.message.substr(0,cause.message.indexOf('\r')) : cause.message 
	oneLineMessage = oneLineMessage.indexOf('\n') > 0 ? oneLineMessage.substr(0,oneLineMessage.indexOf('\n')) : oneLineMessage
    super(oneLineMessage);
    this.stack = `${stack.slice(0,5)}: ${cause.message}${stack.slice(5)}`
	this.sql = sql
	this.cause = cause
	/*
	// Clone properties of cause to DatabaseError
	Object.keys(cause).forEach(function(key){
	  if ((!this.hasOwnProperty(key))&& !(cause.key instanceof Error) && !(cause.key instanceof Function)) {
		this[key] = cause[key]
	  }
    },this)
	*/
	
  }
  
  lostConnection() {
	return false;
  }

  invalidPool() {
	// console.log(this.cause);
	return false;
  } 

 invalidConnection() {
	// console.log(this.cause);
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
      if (cause.originalError && (cause.origianlError instanceof Error)) {
  	    cause = cause.originalError 
  	    if (casse.info && (cause.info instanceof Error)) {
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

class PostgresError extends DatabaseError {
  //  const err = new PostgresError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
	// Abbreviate Long Lists of Place Holders ...
	if (this.sql.indexOf('),($') > 0) {
	  const startElipises = this.sql.indexOf('),($') + 2 
	  const endElipises =  this.sql.lastIndexOf('),($') + 2
	  this.sql = this.sql.substring(0,startElipises) + '(...),' + this.sql.substring(endElipises);
	}
  }

  lostConnection() {
	return ((this.cause.severity && (this.cause.severity === 'FATAL')) && (this.cause.code && (this.cause.code === '57P01')) || ((this.cause.name === 'Error') && (this.cause.message === 'Connection terminated unexpectedly')))
  }
  
  serverUnavailable() {
	const knownErrors = ['Connection terminated unexpectedly']
    return (this.cause.message && (knownErrors.indexOf(this.cause.message) > -1))
  }

  missingTable() {
	return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '42P01')))
  }

}

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

class MariadbError extends DatabaseError {
  //  const err = new MariadbError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
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
, BatchInsertError
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