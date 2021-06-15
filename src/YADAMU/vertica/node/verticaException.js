"use  strict"

const {DatabaseError} = require('../../common/yadamuException.js')

class VerticaError extends DatabaseError {
  //  const err = new VerticaError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
	// Abbreviate Long Lists of Place Holders ...
	if ((typeof this.sql === 'string') && (this.sql.indexOf('),($')) > 0) {
	  const startElipises = this.sql.indexOf('),($') + 2 
	  const endElipises =  this.sql.lastIndexOf('),($') + 2
	  this.sql = this.sql.substring(0,startElipises) + '(...),' + this.sql.substring(endElipises);
	}
  }
  
  lostConnection() {
	const knownErrors = ['Connection terminated unexpectedly','Client has encountered a connection error and is not queryable']
	return ((this.cause.severity && (this.cause.severity === 'FATAL')) && (this.cause.code && (this.cause.code === '57P01')) || ((this.cause.name === 'Error') && (knownErrors.indexOf(this.cause.message) > -1)))
  }
  
  serverUnavailable() {
	const knownErrors = ['Connection terminated unexpectedly','Client has encountered a connection error and is not queryable']
    return (this.cause.message && (knownErrors.indexOf(this.cause.message) > -1))
  }

  missingTable() {
	return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '42P01')))
  }

  missingFile() {
	return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '58V01')))
  }
 
}

class StagingAreaMisMatch extends VerticaError {
  constructor(local,remote,cause) {
	super( new Error(`Vertica Copy Operation Failed. File Not Found. Please ensure folder "${local}" is mapped to folder "${remote}" on the server hosting your Vertica databases.`))
    this.cause = cause
    this.local_staging_area = local
    this.remote_staging_area = remote
  }
}
  
class VertiaCopyOperationFailure extends VerticaError {

  constructor(accepted, rejected, stack, sql) {
	super( new Error(`Vertica Copy Operation Failed. ${accepted} rows accepted. ${rejected} rows rejected`),stack,sql)
  }
}

class WhitespaceIssue extends Error {
  constructor(columnName) {
    super(`Empty String Detected: "${columnName}"`)
  }
}
  
class EmptyStringDetected extends Error {
  constructor(emptyStringList) {
    super(`Empty Strings Detected`)
	this.emptyStringList = emptyStringList
  }
}
  
class ContentTooLarge extends Error {

   constructor(columnName,length,maxLength) {
	 super(`Column "${columnName}: Comtent length ${length} exceeeds maximum permitted (${maxLength}).`)
	 this.tags = ["CONTENT_TOO_LARGE"]
   }
   
   getTags() {
	 return this.tags
   }
}

module.exports = {
  VerticaError
, VertiaCopyOperationFailure
, WhitespaceIssue
, EmptyStringDetected
, ContentTooLarge
, StagingAreaMisMatch
}
