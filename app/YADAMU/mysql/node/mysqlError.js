"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')
const MySQLConstants = require('./mysqlConstants.js')

class MySQLError extends DatabaseError {
  //  const err = new MySQLError(cause,stack,sql)
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
	// Abbreviate Long Lists of Place Holders ...
	if (this.sql.indexOf('),(') > 0) {
	  const startElipises = this.sql.indexOf('),(') + 2
	  const endElipises =  this.sql.lastIndexOf('),(') + 2
	  this.sql = this.sql.substring(0,startElipises) + '(...),' + this.sql.substring(endElipises);
	}
  }
  
  lostConnection() {
    return (this.cause.code && MySQLConstants.LOST_CONNECTION_ERROR.includes(this.cause.code))
  }
  
  serverUnavailable() {
    return (this.cause.code && MySQLConstants.SERVER_UNAVAILABLE_ERROR.includes(this.cause.code))
  }
    	     
  missingTable() {
    return ((this.cause.code && MySQLConstants.MISSING_TABLE_ERROR.includes(this.cause.code)) && ((this.cause.errno && (this.cause.errno === 1146)) && (this.cause.sqlState && (this.cause.sqlState === '42S02'))))
  }

  spatialError() {
	// MySQL could not decode spatial data in WKB format
    return (this.cause.code && MySQLConstants.SPATIAL_ERROR.includes(this.cause.code))
  }

  unknownCodeError() {
	// MySQL could not decode spatial data in WKB format
    return (this.cause.code && MySQLConstants.UNKNOWN_CODE_ERROR.includes(this.cause.code))
  }
  
  spatialErrorGeoJSON() {
	return ((this.spatialError() || this.unknownCodeError()) && (this.cause.message.indexOf(' st_geomfromgeojson.') > -1))
  }

}

module.exports = MySQLError
