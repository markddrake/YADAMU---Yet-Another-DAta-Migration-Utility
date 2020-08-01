"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class SnowflakeError extends DatabaseError {
  
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
    if (this.sql.indexOf('),(?') > 0) {
	  const startElipises = this.sql.indexOf('),(?') + 2 
	  const endElipises =  this.sql.lastIndexOf('),(?') + 2
	  this.sql = this.sql.substring(0,startElipises) + '(...),' + this.sql.substring(endElipises);
	}
  }

  spatialInsertFailed() {
    return ((this.cause.code && this.cause.code === '100205') )
  }

}

module.exports = SnowflakeError