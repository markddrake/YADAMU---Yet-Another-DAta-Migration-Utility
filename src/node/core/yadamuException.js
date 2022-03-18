"use strict"

class YadamuError extends Error {

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
     
  static createIterativeException(dbi,tableInfo,cause,batchSize,rowNumber,row,info) {

	const tableName = tableInfo.tableName
	
    try {
      const details = {
        currentSettings        : {
          yadamu               : dbi.yadamu
        , systemInformation    : dbi.systemInformation
        , metadata             : { 
            [tableName]        : dbi.metadata[tableName]
          }
        }
      , columnNames            : tableInfo.columnNames
      , targetDataTypes        : tableInfo.targetDataTypes 
      }
      Object.assign(details, info === undefined ? {} : typeof info === 'object' ? info : {info: info})
      return new IterativeInsertError(cause,tableName,batchSize,rowNumber,row,details)
    } catch (e) {
	  cause.IterativeErrorIssue = e
      return cause
    }
  }
  
  static createBatchException(dbi,tableInfo,cause,batchNumber,batchSize,firstRow,lastRow,info) {

	const tableName = tableInfo.tableName

    try {
      const details = {
        currentSettings        : {
          yadamu               : dbi.yadamu
        , systemInformation    : dbi.systemInformation
        , metadata             : { 
            [tableName]        : dbi.metadata[tableName]
          }
        }
      , columnNames          : tableInfo.columnNames
      , targetDataTypes      : tableInfo.targetDataTypes 
      }
      Object.assign(details, info === undefined ? {} : typeof info === 'object' ? info : {info: info})
      return new BatchInsertError(cause,tableName,batchNumber,batchSize,firstRow,lastRow,details)
    } catch (e) {
	  cause.batchErrorIssue = e
      return cause
    }
  }
  
  constructor(message) {
    super(message);
  }

  cloneStack(stack) {
	return stack && stack.indexOf('Error') === 0 ? `${stack.slice(0,5)}: ${this.cause.message}${stack.slice(5)}` : stack
  }
  
}

class InternalError extends YadamuError {
  constructor(message,args,info) {
    super(message);
	this.args = args
	this.info = info
  }
}

class ExportError extends YadamuError {
  constructor(tableName,cause,stack) {
    super(`Export Operation Failed while processing "${tableName}". Cannot create export file. Retry using "Unload" Operation`);
	this.cause = cause;
	this.tableName = tableName;
	this.stack = this.cloneStack(stack)
  }
}



class UserError extends YadamuError {
  constructor(message) {
    super(message);
  }
}

class CopyOperationAborted extends YadamuError {
  constructor() {
    super('Copy operation aborted: Error raised by sibling operation.');
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

class ContentTooLarge extends YadamuError {
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

class BatchInsertError extends YadamuError {
  constructor(cause,tableName,batchNumber,batchSize,firstRow,lastRow,info) {
	super(cause.message)
	this.cause = cause
    this.tableName = tableName
	this.batchNumber = batchNumber
	this.batchSize = batchSize
	this.rows = [firstRow,lastRow]
	if (typeof info === 'object') {
	  Object.assign(this,info)
    }
  }
}

class IterativeInsertError extends YadamuError {
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

class RejectedColumnValue extends YadamuError {
  constructor (columnName, value) {
	super(`Column "${columnName}" contains unsupported value "${value}". Row Rejected.`);
  }
}
 

class InvalidMessageSequence extends YadamuError {
  constructor(tableName,found,expected) {
	super(`Error while processing table "${tableName}": Incorrect sequenece of messages. Found "${found}" when expecting "${expected}".`)
  }
}

class UnimplementedMethod extends YadamuError {
  constructor(method, superclass, subclass) {
	super(`Abstract method "${method}" expected by "${superclass}" not implemented by subclass "${subclass}".`)
  }
}

class DatabaseError extends YadamuError {
	
	
  get DRIVER_ID()  { return this._DRIVER_ID }
  set DRIVER_ID(v) { this._DRIVER_ID = v }
  
  constructor(driverId,cause,stack,sql) {
	let oneLineMessage = cause.message.indexOf('\r') > 0 ? cause.message.substr(0,cause.message.indexOf('\r')) : cause.message 
	oneLineMessage = oneLineMessage.indexOf('\n') > 0 ? oneLineMessage.substr(0,oneLineMessage.indexOf('\n')) : oneLineMessage
    super(oneLineMessage);
	this._DRIVER_ID = driverId
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
	
export { 
  YadamuError
, InternalError
, UserError
, CommandLineError  
, BatchInsertError
, IterativeInsertError
, RejectedColumnValue
, DatabaseError
, InputStreamError
, ConfigurationFileError
, ConnectionError
, ContentTooLarge
, ExportError
, InvalidMessageSequence
, CopyOperationAborted
, UnimplementedMethod
}

