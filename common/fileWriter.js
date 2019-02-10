"use strict";
const Writable = require('stream').Writable

const Yadamu = require('./yadamuCore.js');

class FileWriter extends Writable {
  
  constructor(outputStream,status,logWriter,options) {
    super({objectMode: true });
    const self = this;
   
    this.status = status;
    this.logWriter = logWriter;

    this.outputStream = outputStream;
    this.outputStream.write('{');
    
    this.tableName = undefined;

    this.rowCount = 0; 
    
    this.logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    // this.logDDLIssues   = true;    
    
  }      
    
  objectMode() {
    
    return false;
  
  }  
  
  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.outputStream.write(`"systemInformation":${JSON.stringify(obj.systemInformation)}`);
          break;
        case 'ddl':
          this.outputStream.write(',');
          this.outputStream.write(`"ddl":${JSON.stringify(obj.ddl)}`);
          break;
        case 'metadata':
          this.outputStream.write(',');
          this.outputStream.write(`"metadata":${JSON.stringify(obj.metadata)}`);
          break;
        case 'table':
          this.endTime = new Date().getTime();
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName === undefined) {
            this.outputStream.write(',"data":{');
          }
          else {
            const elapsedTime = this.endTime - this.startTime;            
            this.logWriter.write(`${new Date().toISOString()}[FileWriter "${this.tableName}"]. Rows written: ${this.rowCount}. Elaspsed Time: ${Math.round(elapsedTime)}ms. Throughput: ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            this.outputStream.write(',');
          }
          this.outputStream.write(`"${obj.table}":[`);
          this.rowCount = 0;
          this.tableName = obj.table;
          this.endTime = undefined;
          this.startTime = new Date().getTime();
          break;
        case 'data': 
          if (this.rowCount > 0) {
            this.outputStream.write(',');
          }
          this.outputStream.write(obj.data);
          this.rowCount++;
          break;
        case 'rowCount':
          this.outputStream.write(']');
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.tableName) {        
        this.endTime = new Date().getTime();
        const elapsedTime = this.endTime - this.startTime;            
        this.logWriter.write(`${new Date().toISOString()}[FileWriter "${this.tableName}"]: Rows written: ${this.rowCount}. Elaspsed Time: ${Math.round(elapsedTime)}ms. Throughput: ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
        this.outputStream.write('}');
      }
      else {
        this.outputStream.write(',"data":{}');
        this.logWriter.write(`${new Date().toISOString()}[FileWriter] No tables found.\n`);
      }
      this.outputStream.write('}');
      callback();
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      callback(e);
    } 
  } 
}

module.exports = FileWriter;