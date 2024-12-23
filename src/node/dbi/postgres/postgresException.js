
import {DatabaseError} from '../../core/yadamuException.js'

class PostgresError extends DatabaseError {
  //  const err = new PostgresError(dbi,dbi,cause,stack,sql)
  constructor(dbi,cause,stack,sql) {
	const primaryCause = Array.isArray(cause.errors) ? cause.errors[0] : cause
    primaryCause.message = primaryCause.detail ? `${primaryCause.message} [${primaryCause.detail}]` : primaryCause.message
    super(dbi,primaryCause,stack,sql);
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
  
  postgisUnavailable() { 
	return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '42883')) || (this.cause.code && (this.cause.message === 'function postgis_full_version() does not exist')))
  }
  
  bjsonTooLarge() {
	return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '54000')))
  }	  
  
  prettyPrintedJSON() {
	return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '22P02')))
  }
}

export { PostgresError as default }