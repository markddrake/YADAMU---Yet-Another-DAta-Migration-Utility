"use strict" 
const fs = require('fs');
const path = require('path');

const YadamuDBI = require('../../common/yadamuDBI.js');
const TableWriter = require('./tableWriter.js');
const TextParser = require('./fileParser.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class FileDBI extends YadamuDBI {

  getConnectionProperties() {
    return {}
  }
  
  closeInputStream() {      
    this.inputStream.close();
  }

  closeOutputStream() {
        
     const outputStream = this.outputStream;
        
     return new Promise(function(resolve,reject) {
      outputStream.on('finish',function() { resolve() });
      outputStream.close();
    })

  }
  
  isDatabase() {
    return false;
  }
  
  objectMode() {
     return false;
  }

  getInputStream() {
	return this.inputStream.pipe(this.parser);
  }
  
  get DATABASE_VENDOR()    { return 'FILE' };
  get SOFTWARE_VENDOR()    { return 'N/A' };
  get SPATIAL_FORMAT()     { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().file }
  
  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().file )
    this.outputStream = undefined;
    this.inputStream = undefined;
    this.firstTable = true;
  }

  isValidDDL() {
    return true;
  }
  
  setSystemInformation(systemInformation) {
    this.outputStream.write(`"systemInformation":${JSON.stringify(systemInformation)}`);
	 
  }

  setMetadata(metadata) {
    this.outputStream.write(',');
    this.outputStream.write(`"metadata":${JSON.stringify(metadata)}`);
  }
    
  async executeDDL(ddl) {
    this.outputStream.write(',');
    this.outputStream.write(`"ddl":${JSON.stringify(ddl)}`);
  }

  async initialize() {
    super.initialize(false);
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
  }

  async initializeExport() {
	// For FileDBI Export is writig data to the file system..
	super.initializeExport();
	this.parser = new TextParser(this.yadamuLogger);
	const importFilePath = path.resolve(this.parameters.FILE);
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size
    this.inputStream = fs.createReadStream(importFilePath);
    this.yadamuLogger.log([`${this.constructor.name}`],`Processing file "${importFilePath}". Size ${fileSizeInBytes} bytes.`)
  }

  async finalizeExport() {
	this.closeInputStream()
  }
  
  async initializeImport() {
	// For FileDBI Import is Writing data to the file system..
	super.initializeImport()
    const exportFilePath = path.resolve(this.parameters.FILE);
    this.outputStream = fs.createWriteStream(exportFilePath);
    this.yadamuLogger.log([`${this.constructor.name}`],`Writing file "${exportFilePath}".`)
    this.outputStream.write(`{`)
  }

  async initializeData() {
    this.outputStream.write(',');
    this.outputStream.write('"data":{'); 
  }
  
  async finalizeData() {
	this.outputStream.write('}');
  }  
  
  async finalizeImport() {
  }
  
  async finalize() {
  }


  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {

    if (this.inputStream !== undefined) {
      try {
        await this.closeInputStream()
      } catch (err) {
        this.yadamuLogger.logException([`${this.constructor.name}.abort()`],err)
      }
    }
      
    if (this.oututStream !== undefined) {
      try {
        await this.closeOutputStream()
      } catch (err) {
        this.yadamuLogger.logException([`${this.constructor.name}.abort()`],e)
      }
    }
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    return []
  }
  
  async getSchemaInfo(schema) {
    return null
  }

  getTableWriter(tableName) {

    if (this.firstTable === true) {
      this.firstTable = false
    }
    else {
      this.outputStream.write(',');
    }

    return new TableWriter(tableName,this.outputStream);      
  }



}

module.exports = FileDBI
