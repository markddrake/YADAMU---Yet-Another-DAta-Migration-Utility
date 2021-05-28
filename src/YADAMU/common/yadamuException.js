"use strict"

class YadamuError extends Error {
  constructor(message) {
    super(message);
  }
  
  static lostConnection(e) {
	return ((e instanceof DatabaseError) && e.lostConnection())
  }

  static serverUnavailable(e) {
	return ((e instanceof DatabaseError) && e.serverUnavailable())
  }
  
  static prematureClose(e) {
	return (e.code && e.code === 'ERR_STREAM_PREMATURE_CLOSE')
  }

  static missingTable(e) {
	return ((e instanceof DatabaseError) && e.missingTable())
  }
  
}

class InternalError extends Error {
  constructor(message,args,info) {
    super(message);
	this.args = args
	this.info = info
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
	this.vendorProperties = typeof connectionProperties === 'object' ? Object.assign({},connectionProperties,{password: "#REDACTED#"}) : connectionProperties
  }
}

class ContentTooLarge extends Error {
  constructor(cause,vendor,operation,maxSize) {
    super(`Source record too large for Target Database`);
	this.cause=cause
	this.vendor = vendor
	this.operation = operation
	this.maxSize = maxSize
  }
  
  getTags() {
	return [this.vendor,this.tableName,this.operation,this.maxSize]
  }
  
  setTableName(tableName) {
	this.tableName = tableName
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

class RejectedColumnValue extends Error {
  constructor (columnName, value) {
	super(`Column "${columnName}" contains unsupported value "${value}". Row Rejected.`);
  }
}
  

class DatabaseError extends Error {
  constructor(cause,stack,sql) {
	let oneLineMessage = cause.message.indexOf('\r') > 0 ? cause.message.substr(0,cause.message.indexOf('\r')) : cause.message 
	oneLineMessage = oneLineMessage.indexOf('\n') > 0 ? oneLineMessage.substr(0,oneLineMessage.indexOf('\n')) : oneLineMessage
    super(oneLineMessage);
	this.cause = cause
	this.stack = this.cloneStack(stack)
	this.sql = sql
	
	this.setTags()
	/*
	// Clone properties of cause to DatabaseError
	Object.keys(cause).forEach((key) => {
	  if ((!this.hasOwnProperty(key))&& !(cause.key instanceof Error) && !(cause.key instanceof Function)) {
		this[key] = cause[key]
	  }
    })
	*/
    
  }
  
  setTags() {
	this.tags = []
	switch (true) {
	  case (this.contentTooLarge()):
	     this.tags.push('CONTENT_TOO_LARGE');
    }
  }
  
  cloneStack(stack) {
	return stack && stack.indexOf('Error') === 0 ? `${stack.slice(0,5)}: ${this.cause.message}${stack.slice(5)}` : stack
  }
  
  lostConnection() {
	return false;
  }

  invalidPool() {
	return false;
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
  
  spatialInsertFailed() {
	return false;
  }

  contentTooLarge() {
    return false;
  }	  
  
  getTags() {
	return this.tags
  }
  
}

class InputStreamError extends DatabaseError {
  constructor(cause,stack,sql) {
    super(cause,stack,sql)
  }
}
	
module.exports = {
  YadamuError
, InternalError
, UserError
, BatchInsertError
, IterativeInsertError
, RejectedColumnValue
, DatabaseError
, InputStreamError
, CommandLineError  
, ConfigurationFileError
, ConnectionError
, ContentTooLarge
}

