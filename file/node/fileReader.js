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
const TableWriter = require('./tableWriter.js');

const defaultParameters = {}

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
  
  get DATABASE_VENDOR() { return 'FILE' };
  get SOFTWARE_VENDOR() { return 'Vendor Long Name' };
  get SPATIAL_FORMAT()  { return 'WKT' };
  
  getReader() {
     
    return this.inputStream.pipe(this.parser);
    
  }

  constructor(yadamu) {
    super(yadamu,defaultParameters)
    this.inputStream = undefined;
    this.parser = new FileParser(yadamu.getLogWriter());
  }

  async initialize() {
    super.initialize();
    const importFilePath = path.resolve(this.parameters.FILE);
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size
    this.inputStream = fs.createReadStream(importFilePath);
    this.logWriter.write(`${new Date().toISOString()}[FileReader()]: : Processing file "${importFilePath}". Size ${fileSizeInBytes} bytes.\n`)
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
        this.logWriter.write(`${new Date().toISOString()}[FileReader()]: Fatal Error:${err.stack}.\n`)
      }
    }
  }

}

module.exports = FileReader
