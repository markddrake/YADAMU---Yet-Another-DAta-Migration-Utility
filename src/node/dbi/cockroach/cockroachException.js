
import PostgresError from '../postgres/postgresException.js'

class CockroachError extends PostgresError {
  //  const err = new YugabyteError(driverId,driverId,cause,stack,sql)
  constructor(driverId,cause,stack,sql) {
    super(driverId,cause,stack,sql);
  }
 
  readBufferTooSmall() {
     return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '08P01')) && this.cause.message.includes('bigger than maximum allowed message size'))
  }
 
  transactionAborted() {
    return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === '40001')) && this.cause.message.startsWith('restart transaction'))
  }
 
  noActiveTransaction() {
    return ((this.cause.severity && (this.cause.severity === 'ERROR')) && (this.cause.code && (this.cause.code === 'XXUUU')) && this.cause.message.includes('no transaction in progress'))
  }
}

export { CockroachError as default }