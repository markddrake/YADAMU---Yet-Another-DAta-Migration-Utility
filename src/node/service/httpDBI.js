"use strict" 

import FileDBI from '../dbi/file/fileDBI.js';

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

    super(yadamu,{},{})    
	this.httpStream = httpStream;
  }
  
  createInputStream() {
	 return this.httpStream
  }

  getInputStream() {
 	// this.yadamuLogger.trace([this.constructor.name,],'getInputStream()')
	this.inputStream = this.httpStream;
	return this.inputStream;
  }

  async initializeImport() {
	this.outputStream = this.httpStream;
  }

  closeInputStream() {      
  }

  closeOutputStream() {
  }
  
}

export { HttpDBI as default }
