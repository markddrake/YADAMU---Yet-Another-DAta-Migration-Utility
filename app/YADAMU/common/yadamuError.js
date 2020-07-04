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
  constructor(cause,tableName,batchSize,firstRow,lastRow,info) {
	super(cause.message)
	this.cause = cause
    this.tableName = tableName
	this.batchSize = batchSize
	this.rows = [firstRow,lastRow]
	if (typeof info === 'object') {
	  Object.assign(this,info)
    }
  }
}

class IterativeInsertError extends Error {
  constructor(cause,tableName,batchSize,rowNumber,row,info) {
	super(cause.message)
	this.cause = cause
    this.tableName = tableName
    this.rowNumber = `${rowNumber} of ${batchSize}`
	this.row = row
	if (typeof info === 'object') {
	  Object.assign(this,info)
    }
  }
}

class DatabaseError extends Error {
  constructor(cause,stack,sql,) {
	let oneLineMessage = cause.message.indexOf('\r') > 0 ? cause.message.substr(0,cause.message.indexOf('\r')) : cause.message 
	oneLineMessage = oneLineMessage.indexOf('\n') > 0 ? oneLineMessage.substr(0,oneLineMessage.indexOf('\n')) : oneLineMessage
    super(oneLineMessage);
	this.cause = cause
	this.stack = this.cloneStack(stack)
	this.sql = sql
	/*
	// Clone properties of cause to DatabaseError
	Object.keys(cause).forEach((key) => {
	  if ((!this.hasOwnProperty(key))&& !(cause.key instanceof Error) && !(cause.key instanceof Function)) {
		this[key] = cause[key]
	  }
    })
	*/
	
  }
  
  cloneStack(stack) {
	return `${stack.slice(0,5)}: ${this.cause.message}${stack.slice(5)}`
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

module.exports = {
  YadamuError
, UserError
, BatchInsertError
, IterativeInsertError
, DatabaseError
, CommandLineError  
, ConfigurationFileError
, ConnectionError
}