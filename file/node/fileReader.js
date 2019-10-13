"use strict" 
const fs = require('fs');
const path = require('path');

/* 
**
** Require Database Vendors API 
**
*/

const FileParser = require('./fileParser.js');
const YadamuDBI = require('../../common/yadamuDBI.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class FileReader extends YadamuDBI {

  getConnectionProperties() {
    return {}
  }
  
  closeFile() {      
    this.inputStream.close();
  }
  
  isDatabase() {
    return false;
  }
  
  objectMode() {
     return false;
  }
  
  get DATABASE_VENDOR()    { return 'FILE' };
  get SOFTWARE_VENDOR()    { return 'N/A' };
  get SPATIAL_FORMAT()     { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().file }
  
  getReader() {
     
    return this.inputStream.pipe(this.parser);
    
  }

  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().file)
    this.inputStream = undefined;
    this.parser = new FileParser(yadamu.getYadamuLogger());
  }

  async initialize() {
    super.initialize();
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT   
    const importFilePath = path.resolve(this.parameters.FILE);
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size
    this.inputStream = fs.createReadStream(importFilePath);
    this.yadamuLogger.log([`${this.constructor.name}`],`Processing file "${importFilePath}". Size ${fileSizeInBytes} bytes.`)
  }
  
  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    await this.closeFile()
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
      
    if (this.inputStream !== undefined) {
      try {
        await this.closeFile()
      } catch (err) {
        this.yadamuLogger.logException([`${this.constructor.name}`],err)
      }
    }
  }

}

module.exports = FileReader
