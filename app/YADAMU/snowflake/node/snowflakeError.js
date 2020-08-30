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
	const spatialErrorCodes = Object.freeze(['100217','100205'])
    return (this.cause.code && spatialErrorCodes.includes(this.cause.code))
  }

}

module.exports = SnowflakeError