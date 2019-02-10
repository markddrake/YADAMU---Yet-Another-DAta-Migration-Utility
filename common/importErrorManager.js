"use strict"   
const fs = require('fs')
const fsp = require('fs').promises;
const path = require('path')
const assert = require('assert')

class ImportErrorManager {

  constructor(filename,maxErrors) {

    this.folderPath =  process.cwd() + path.sep + "Invalid" + path.sep + (new Date().toISOString().split(':').join(''))
    this.filename = this.folderPath + path.sep + path.basename(filename);
    this.maxErrors = maxErrors
    this.errorCount = 0;
    this.tableName = undefined;
    this.ws = undefined;
    this.seperator = '';
  }
  
  logError(tableName,data) {

    assert.notEqual(this.maxErrors,this.errorCount,"Encountered too many errors");
    this.errorCount++;
    
    if (this.tableName === undefined) {
      fs.mkdirSync(this.folderPath, { recursive: true });
      this.ws = fs.createWriteStream(this.filename);
      this.ws.write(`{"errors": { "${tableName}": [`)
      this.tableName = tableName;
    }
    
    if ( this.tableName!== tableName ) {
      this.ws.write(`]},{"${tableName}":[`);
      this.tableName = tableName;
      this.seperator = '';
    }
    
    this.ws.write(`${this.seperator}${JSON.stringify(data)}`);
    this.seperator = ',';
     
  }
  
  close() {
    if (this.tableName !== undefined) {
      this.ws.write(`]}}`);
      this.ws.close();
    }
  }
}
    
module.exports = ImportErrorManager;


