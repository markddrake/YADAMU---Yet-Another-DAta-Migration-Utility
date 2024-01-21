
import {DatabaseError} from '../../core/yadamuException.js'
import SnowflakeConstants from './snowflakeConstants.js'

class SnowflakeError extends DatabaseError {
  
  constructor(drvierID,cause,stack,sql) {
    super(drvierID,cause,stack,sql);
    if (this.sql.indexOf('),(?') > 0) {
	  const startElipises = this.sql.indexOf('),(?') + 2 
	  const endElipises =  this.sql.lastIndexOf('),(?') + 2
	  this.sql = this.sql.substring(0,startElipises) + '(...),' + this.sql.substring(endElipises);
	}
  }

  lostConnection() {
    return (this.cause.isFatal && (this.cause.code && SnowflakeConstants.LOST_CONNECTION_ERROR.includes(this.cause.code)) && (this.cause.sqlState && SnowflakeConstants.LOST_CONNECTION_STATE.includes(this.cause.sqlState)))
	    || ((this.cause.name === 'NetworkError') && (this.cause.code === 401001) && (this.cause.message.includes('Could not reach Snowflake')))
	    || ((this.cause.name === 'NetworkError') && (this.cause.code === 402001) && (this.cause.message.includes('Could not reach S3/Blob')))	
  }

  serverUnavailable() {
    return this.lostConnection()
  }
  
  spatialInsertFailed() {
	const spatialErrorCodes = Object.freeze(['100217','100205'])
    return (this.cause.code && spatialErrorCodes.includes(this.cause.code))
  }
 
  requestTooLarge() {
	return ((this.cause.code && SnowflakeConstants.REQUEST_TOO_LARGE_ERROR.includes(this.cause.code)) && (this.cause?.response?.status && (this.cause.response.status === 413)))
  }

  contentTooLarge() {
    return ((this.cause.code && SnowflakeConstants.CONTENT_TOO_LARGE_ERROR.includes(this.cause.code)) && (this.cause.sqlState && SnowflakeConstants.CONTENT_TOO_LARGE_STATE.includes(this.cause.sqlState)))
  }
}

export { SnowflakeError as default }