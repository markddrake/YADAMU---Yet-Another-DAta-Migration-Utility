"use strict"   
const fs = require('fs')
const fsp = require('fs').promises;
const path = require('path')
const assert = require('assert')

class YadamuErrorLogger {

  constructor(filename,logWriter) {
    this.filename = filename;
    this.logWriter  = logWriter;
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
   
  logError(tableName,data) {

  
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
      this.logWriter.write(`${new Date().toISOString()}["YadauErrorLogger"]: ${this.recordCount} records written to "${this.ws.path}"\n`)
      this.ws.write(`]}}`);
      this.ws.close();
    }
  }
}
    
module.exports = YadamuErrorLogger;


