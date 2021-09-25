"use strict"

const {DatabaseError} = require('../../common/yadamuException.js')

const OracleConstants = require('./oracleConstants.js')

class OracleError extends DatabaseError {
  //  const err = new OracleError(cause,stack,sql,args,outputFormat)
  
  obfuscateBindValues(args) {
    if (Array.isArray(args)) {
      return args.map((arg) => {
        if (arg.type && arg.val) {
          arg.dataType = OracleConstants.BIND_TYPES[arg.type]
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
	  if (Array.isArray(args?.bindDefs)) {
		args.bindDefs.forEach((bindDef) => {bindDef.dataType = OracleConstants.BIND_TYPES[bindDef.type]})
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
    return this.cause.message.startsWith(OracleConstants.NJS_INVALID_POOL)
  } 
  
  lostConnection() {
	const oracledbCode = this.cause.message.substring(0,this.cause.message.indexOf(':'))
	return (OracleConstants.LOST_CONNECTION_ERROR.includes(this.cause.errorNum)) || (OracleConstants.ORACLEDB_LOST_CONNECTION.includes(oracledbCode))
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

class StagingFileError extends OracleError {
  constructor(local,remote,cause) {
	super(cause,cause.stack,cause.sql,cause.args,cause.outputFormat)
	this.message = `Oracle Copy Operation Failed. File Not Found. Please ensure folder "${local}" maps to folder "${remote}" on the server hosting your Oracle databases.`
	this.stack = cause.stack
    this.cause = cause
    this.local_staging_area = local
    this.remote_staging_area = remote
  }
}
module.exports = {
  OracleError,
  StagingFileError
}
  
