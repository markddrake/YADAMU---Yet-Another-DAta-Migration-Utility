
import PostgresError from '../postgres/postgresException.js'

class YugabyteError extends PostgresError {
  //  const err = new YugabyteError(dbi,dbi,cause,stack,sql)
  constructor(dbi,cause,stack,sql) {
    super(dbi,cause,stack,sql);
  }
  
  transactionAborted() {
    return ((this.cause.severity && (this.cause.severity === 'FATAL')) && (this.cause.code && (this.cause.code === '40001')) && this.cause.message.includes('Unknown transaction, could be recently aborted'))
  }	  
  
  lostConnection() {
	const knownErrors = ['Invalid argument: Unknown session','Client has encountered a connection error and is not queryable']
	return (
	  super.lostConnection() || 
	  ((this.cause.severity && (this.cause.severity === 'FATAL')) && (this.cause.code && (this.cause.code === 'XX000')) && this.cause.message.startsWith('Timed out: Perform RPC')) ||
	  ((this.cause.severity && (this.cause.severity === 'FATAL')) && (this.cause.code && (this.cause.code === 'XX000')) && this.cause.message.startsWith('Invalid argument: Unknown session')) ||
	  ((this.cause.name === 'Error') && (this.cause.message.startsWith('Connection terminated unexpectedly'))) ||
	  ((this.cause.name === 'Error') && (this.cause.message.startsWith('Client has encountered a connection error'))) 
	 )
  }
  
  serverUnavailable() {
    super.serverUnavailable() || 
    ((this.cause.severity && (this.cause.severity === 'FATAL')) && (this.cause.code && (this.cause.code === '57P03')) && this.cause.message.startsWith('the database system is in recovery mode')) ||
    ((this.cause.errno && (this.cause.errno ===  -111)) && (this.cause.code && (this.cause.code === 'ECONNREFUSED')))	
  }
}

export { YugabyteError as default }