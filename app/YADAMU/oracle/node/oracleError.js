"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

const oracledb = require('oracledb');

const BIND_TYPES = {
   [oracledb.BLOB]    : "BLOB"
 , [oracledb.BUFFER]  : "BUFFER"
 , [oracledb.CLOB]    : "CLOB"
 , [oracledb.CURSOR]  : "CURSOR"
 , [oracledb.DATE]    : "DATE"
 , [oracledb.DEFAULT] : "DEFAULT"
 , [oracledb.NCLOB]   : "NCLOB"
 , [oracledb.NUMBER]  : "NUMBER"
 , [oracledb.STRING]  : "STRING"
}

class OracleError extends DatabaseError {
  //  const err = new OracleError(cause,stack,sql,args,outputFormat)
  
  obfuscateBindValues(args) {
    if (Array.isArray(args)) {
      return args.map((arg) => {
        if (arg.type && arg.val) {
          arg.jsType = BIND_TYPES[arg.type]
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

  lostConnection() {
    const knownErrors = [3113,3114,3135,28,1012]
    return (this.cause.errorNum && (knownErrors.indexOf(this.cause.errorNum) > -1))
  }

  serverUnavailable() {
    const knownErrors = [1109,12514,12528,12537,12541]
    return (this.cause.errorNum && (knownErrors.indexOf(this.cause.errorNum) > -1))
  }

  invalidPool() {
    return this.cause.message.startsWith('NJS-002:')
  } 

  invalidConnection() {
    return this.cause.message.startsWith('NJS-003:')
  } 
  
  missingTable() {
    return (this.cause.errorNum && ((this.cause.errorNum === 942)))
  }
  
  spatialInsertFailed() {
    return ((this.cause.errorNum && (this.cause.errorNum === 29532)) && (this.cause.message.indexOf(' WKB ') > -1))
  }
 
}

module.exports = OracleError
