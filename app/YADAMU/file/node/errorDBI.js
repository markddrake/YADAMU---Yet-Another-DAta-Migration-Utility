"use strict"

const FileDBI = require('./fileDBI.js')
const ErrorWriter = require('./errorWriter.js');

class ErrorDBI extends FileDBI {
  
  constructor(yadamu,exportFilePath) {
    super(yadamu,exportFilePath)
  }
 
  getOutputStream() {
    // Override parent method to allow output stream to be passed to worker
    // this.yadamuLogger.trace([this.constructor.name],`getOutputStream(${tableName},${this.firstTable})`)
	const os = new ErrorWriter(this,this.yadamuLogger)
    return os
  }
  
}
module.exports =ErrorDBI

