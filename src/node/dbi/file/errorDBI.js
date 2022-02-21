"use strict"

import FileDBI from './fileDBI.js'
import ErrorOutputManager from './errorOutputManager.js';

class ErrorDBI extends FileDBI {
  
  constructor(yadamu,filename) {
    super(yadamu,null,{},{FILE : filename})
	this.FILE = filename
  }
 
  getOutputStream(tableName) {
    // Override parent method to allow output stream to be passed to worker
    // this.yadamuLogger.trace([this.constructor.name],`getOutputStream(${tableName},${this.firstTable})`)
	const os = new ErrorOutputManager(this,tableName,this.firstTable,this.yadamuLogger)
    return os
  }
  
}

export { ErrorDBI as default }
