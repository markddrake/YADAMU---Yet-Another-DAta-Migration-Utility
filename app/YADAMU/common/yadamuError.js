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

class LostConnection extends UserError {
  constructor(cause,connectionProperties) {
    super(cause.message);
	this.cause = cause;
	this.stack = cause.stack
  }
}

class DatabaseError extends Error {
  constructor(cause,stack,sql,) {
    super(cause.message);
	this.oneLineMessage = this.message.indexOf('\r') > 0 ? this.message.substr(0,this.message.indexOf('\r')) : this.message
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
    return (this.cause.errorNum && ((this.cause.errorNum === 3113) || (this.cause.errorNum === 3114) || (this.cause.errorNum === 3135)))
  }

  serverUnavailable() {
	return (this.cause.errorNum && ((this.cause.errorNum === 12514) || (this.cause.errorNum === 12541) || (this.cause.errorNum === 12528)))
  }
 
 
}

class MsSQLError extends DatabaseError {
  //  const err = new MsSQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

  lostConnection() {
	// console.log(this.cause.name,this.cause.code,this.cause.message,this.cause.message.indexOf('Connection lost - '))
	return ((this.cause.name === 'RequestError') && ((this.cause.code && (this.cause.code === 'EINVALIDSTATE')) || this.cause.message.indexOf('Connection lost - ') === 0 ))
 }

  serverUnavailable() {
	// console.log(this.cause.name,this.cause.code,this.cause.message,this.cause.message.indexOf('Connection lost - '))
	return ((this.cause.name === 'ConnectionError') && ((this.cause.code && (this.cause.code === 'ESOCKET')) || this.cause.message.indexOf('Failed to connect') === 0 ))
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

}

class MySQLError extends DatabaseError {
  //  const err = new MySQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }
  
  lostConnection() {
	return ((this.cause.fatal) && (this.cause.code && (this.cause.code === 'PROTOCOL_CONNECTION_LOST') || (this.cause.code === 'ECONNRESET')))
  }

  serverUnavailable() {
    return (this.cause.fatal && (this.cause.code && (this.cause.code === 'ECONNREFUSED') || (this.cause.code === 'ETIMEDOUT')))
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
  }
  
  lostConnection() {
    return (this.cause.code && (this.cause.code === 'ER_CMD_CONNECTION_CLOSED') || (this.cause.code === 'ER_SOCKET_UNEXPECTED_CLOSE') || (this.cause.code === 'ER_GET_CONNECTION_TIMEOUT'))
  }
  serverUnavailable() {
    return (this.cause.code && ((this.cause.code === 'ER_GET_CONNECTION_TIMEOUT') || (this.cause.code === 'ECONNREFUSED') ))
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