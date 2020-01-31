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
	this.oneLineMessage = this.message.indexOf('\n') > 0 ? this.message.substr(0,this.message.indexOf('\n')) : this.message
    this.stack = `${stack.slice(0,5)}: ${cause.message}${stack.slice(5)}`
	this.sql = sql
	this.cause = cause
	Object.keys(cause).forEach(function(key){
	  if ((!this.hasOwnProperty(key))&& !(cause.key instanceof Error) && !(cause.key instanceof Function)) {
		this[key] = cause[key]
	  }
    },this)
  }
}

class OracleError extends DatabaseError {
  //  const err = new OracleError(cause,stack,sql,args,outputFormat)
  constructor(cause,stack,sql,args,outputFormat) {
    super(cause,stack,sql);
	this.args = args
	this.outputFormat = outputFormat
  }
}

class MsSQLError extends DatabaseError {
  //  const err = new MsSQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }
}

class PostgresError extends DatabaseError {
  //  const err = new PostgresError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }
}

class MySQLError extends DatabaseError {
  //  const err = new MySQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }
}

class MariadbError extends DatabaseError {
  //  const err = new MariadbError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
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