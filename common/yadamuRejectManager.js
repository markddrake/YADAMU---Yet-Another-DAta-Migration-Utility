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
  
  createLogFile() {
    const errorFolderPath = path.dirname(this.filename);
    fs.mkdirSync(errorFolderPath, { recursive: true });
    const ws = fs.createWriteStream(this.filename);
    ws.write(`{ "errors": {`)
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
      this.ws = this.createLogFile();
    }
    
    this.addTableName(tableName);
    
    this.ws.write(`${this.seperator}${JSON.stringify(data)}`);
    this.seperator = ',';
    this.recordCount++;
     
  }
  
  close() {
    if (this.tableName !== undefined) {
      this.yadamuLogger.info([`${this.constructor.name}`],`${this.recordCount} records written to "${this.ws.path}"`)
      this.ws.write(`]}}`);0
      this.ws.close();
    }
  }
}
    
module.exports = YadamuRejectManager;


