"use strict"   
const fs = require('fs')
const fsp = require('fs').promises;
const path = require('path')
const assert = require('assert')

class YadamuRejectManager {

  constructor(filename,yadamuLogger) {
    this.filename = filename;
    this.yadamuLogger  = yadamuLogger;
    this.tableName = undefined;
    this.ws = undefined;
    this.seperator = undefined;
    this.recordCount = 0;
  }
  
  createLogFile(filename) {
    const fileLocation = path.dirname(filename);
    fs.mkdirSync(fileLocation, { recursive: true });
    const ws = fs.createWriteStream(filename);
    return ws;
  }
   
  addTableName(tableName) {
    if (this.tableName !== tableName) {
      if (this.tableName) {
        this.ws.write(`],`);
      }
      this.ws.write(`"${tableName}" : [`)
      this.tableName = tableName
      this.seperator = '';
    }
  }    
   
  rejectRow(tableName,data) {

    if (this.ws === undefined) {
      this.ws = this.createLogFile(this.filename);
      this.ws.write(`{ "errors": {`)
    }
    
    this.addTableName(tableName);
    
    this.ws.write(`${this.seperator}${JSON.stringify(data)}`);
    this.seperator = ',';
    this.recordCount++;
     
  }
  
  close() {
    if (this.recordCount > 0) {
      this.yadamuLogger.warning([`REJECTIONS`],`${this.recordCount} records written to "${this.ws.path}"`)
      this.ws.write(`]}}`);0
      this.ws.close();
    }
  }
}
    
module.exports = YadamuRejectManager;


