"use strict" 

const FileDBI = require('../../YADAMU/file/node/fileDBI.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class HttpDBI extends FileDBI {

  get DATABASE_VENDOR()    { return 'HTTP' };
  get SOFTWARE_VENDOR()    { return 'N/A' };
  get SPATIAL_FORMAT()     { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().http }
  
  constructor(yadamu,httpStream) {
    super(yadamu,yadamu.getYadamuDefaults().http)    
	this.httpStream = httpStream;
  }

  getInputStream() {
 	// this.yadamuLogger.trace([this.constructor.name,],'getInputStream()')
	this.inputStream = this.httpStream;
	return this.inputStream;
  }

  async initializeImport() {
	this.outputStream = this.httpStream;
	this.outputStream.write(`{`)
  }

  closeInputStream() {      
  }

  closeOutputStream() {
  }
  
}

module.exports = HttpDBI
