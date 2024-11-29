
import {
  DatabaseError
}                  from '../../core/yadamuException.js'

class VerticaError extends DatabaseError {

  constructor(dbi,cause,stack,sql) {
    super(dbi,cause,stack,sql);
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
  constructor(dbi,filename,local,remote,cause) {
	super(dbi, new Error(`Vertica Copy Operation Failed. File "${filename}" Not Found. Please ensure folder "${local}" maps to folder "${remote}" on the server hosting your Vertica databases.`))
    this.cause = cause
    this.local_staging_area = local
    this.remote_staging_area = remote
  }
}
  
class VertiaCopyOperationFailure extends VerticaError {

  constructor(dbi, accepted, rejected, stack, sql) {
	super(dbi,  new Error(`Vertica Copy Operation Failed. ${accepted} rows accepted. ${rejected} rows rejected`),stack,sql)
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
	 super(`Column "${columnName}: Content length ${length} exceeeds maximum permitted (${maxLength}).`)
	 this.tags = ["CONTENT_TOO_LARGE"]
   }
   
   getTags() {
	 return this.tags
   }
}

export {
  VerticaError
, VertiaCopyOperationFailure
, WhitespaceIssue
, EmptyStringDetected
, ContentTooLarge
, StagingAreaMisMatch
}
