"use strict"

const {DatabaseError} = require('../../common/yadamuException.js')

const OracleConstants = require('./oracleConstants.js')

class OracleError extends DatabaseError {
  //  const err = new OracleError(cause,stack,sql,args,outputFormat)
  
  obfuscateBindValues(args) {
    if (Array.isArray(args)) {
      return args.map((arg) => {
        if (arg.type && arg.val) {
          arg.jsType = OracleConstants.BIND_TYPES[arg.type]
          switch (true) {
            case (Buffer.isBuffer(arg.val)):
              arg.val = "Buffer"
              break;
            case ((typeof arg.val === 'object') && (arg.val.constructor !== undefined) && (arg.val.constructor.name === 'Lob')):
              arg.val = "Lob"
              break;
            default:
              arg.val = typeof arg.val
          }
        }
        return arg
      })
    }
    else {
      if (typeof args === 'object') {
        Object.keys(args).forEach((key) => {
          switch (true) {
            case (Buffer.isBuffer(args[key])):
              args[key] = "Buffer"
              break;
            case ((args[key] !== null) && (typeof args[key] === 'object') && (args[key].constructor !== undefined) && (args[key].constructor.name === 'Lob')):
              args[key] = "Lob"
              break;
            default:
          }
        })
      }
    }
	return args
  }
 
  constructor(cause,stack,sql,args,outputFormat) {
    super(cause,stack,sql);
    this.args = this.obfuscateBindValues(args)
    this.outputFormat = outputFormat
    
  }

  invalidCredentials() { 
  }

  invalidPool() {
    return this.cause.message.startsWith(OracleConstants.INVALID_POOL)
  } 
  
  lostConnection() {
    return ((this.cause.errorNum && OracleConstants.LOST_CONNECTION_ERROR.includes(this.cause.errorNum)) || ((this.cause.errorNum === 0) && (this.cause.message.startsWith(OracleConstants.NOT_CONNECTED) || this.cause.message.startsWith(OracleConstants.INVALID_CONNECTION))))
  }

  missingTable() {
    return (this.cause.errorNum && OracleConstants.MISSING_TABLE_ERROR.includes(this.cause.errorNum))
  }
  
  serverUnavailable() {
    return (this.cause.errorNum && OracleConstants.SERVER_UNAVAILABLE_ERROR.includes(this.cause.errorNum))
  }

  jsonParsingFailed() {
    return (this.cause.errorNum && OracleConstants.JSON_PARSING_ERROR.includes(this.cause.errorNum))
  }

  spatialError() {
    return (this.cause.errorNum && OracleConstants.SPATIAL_ERROR.includes(this.cause.errorNum))
  }

  copyFileNotFoundError() {
    return (this.cause.errorNum && OracleConstants.COPY_FILE_NOT_FOUND_ERROR.includes(this.cause.errorNum))
  }

  spatialErrorWKB() {
    return (this.spatialError() && (this.cause.message.indexOf(' WKB ') > -1))
  }
  
  includesSpatialOperation() {
	return (this.cause.message.indexOf('MDSYS.SDO_UTIL') > -1)
  }
 
}

module.exports = OracleError
