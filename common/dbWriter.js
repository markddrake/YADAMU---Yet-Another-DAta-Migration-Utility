"use strict";
const Writable = require('stream').Writable
const Readable = require('stream').Readable;

const Yadamu = require('./yadamu.js');

class DBWriter extends Writable {
  
  constructor(dbi,schema,mode,status,logWriter,options) {

    super({objectMode: true });
    const self = this;
    
    this.dbi = dbi;
    this.schema = schema;
    this.mode = mode;
    this.ddlRequired = (mode !== 'DATA_ONLY');    
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[DBWriter ${dbi.DATABASE_VENDOR}]: Ready. Mode: ${this.mode}.\n`)
        
    this.currentTable = undefined;
    this.rowCount     = undefined;
    this.ddlComplete  = false;
  }      
  
  objectMode() {
    return this.dbi.objectMode(); 
  }
  
  setOptions(options) {
    OPTIONS = options
  }
  
  setTableName(tableName) {
     switch (this.dbi.parameters.IDENTIFIER_CASE) {
       case 'UPPER':
         this.tableName = tableName.toUpperCase();
         break;
       case 'LOWER':
         this.tableName = tableName.toLowerCase();
         break;         
      default: 
        this.tableName = tableName;
    }   
  }
  
  mergeMetadata(targetMetadata, sourceMetadata) {
            
    for (let table of Object.keys(sourceMetadata)) {
      if (!targetMetadata.hasOwnProperty(table)) {
        Object.assign(targetMetadata, {[table] : sourceMetadata[table]})
      }     
    }            
    return targetMetadata
  }
  
  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.dbi.setSystemInformation(obj.systemInformation)
          break;
        case 'ddl':
          if ((this.ddlRequired) && (obj.ddl.length > 0) && (this.dbi.isValidDDL())) { 
            await this.dbi.executeDDL(this.schema,obj.ddl);
            this.ddlComplete = true;
          }
          break;
        case 'metadata':
          let metadata = Yadamu.convertIdentifierCase(this.dbi.parameters.IDENTIFIER_CASE,obj.metadata);
          const targetTableInfo = await this.dbi.getTableInfo(this.schema,this.status);
          if (targetTableInfo.length > 0) {
             metadata = this.mergeMetadata(this.dbi.generateMetadata(targetTableInfo,false),metadata);
          }
          this.dbi.setMetadata(metadata)
          if (Object.keys(metadata).length > 0) {   
            // ### if the import already processed a DDL object do not execute DDL when generating statements.
            await this.dbi.generateStatementCache(this.schema,!this.ddlComplete)
          } 
          break;
        case 'table':
          if (this.currentTable === undefined) {
            await this.dbi.initializeDataLoad(this.schema);
          }
          else {
            const results = await this.currentTable.finalize();
            if (!this.skipTable) {
              const elapsedTime = results.endTime - results.startTime;            
              this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${results.insertMode}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTableName(obj.table)
          this.currentTable = this.dbi.getTableWriter(this.schema,this.tableName);
          await this.currentTable.initialize();
          this.rowCount = 0;
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          await this.currentTable.appendRow(obj.data);
          this.rowCount++;
          if (this.currentTable.batchComplete()) {
            this.skipTable = await this.currentTable.writeBatch(this.status);
          }  
          if (this.currentTable.commitWork(this.rowCount)) {
             await this.dbi.commitTransaction()
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._write() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.currentTable !== undefined) {
        const results = await this.currentTable.finalize();
        if (!this.skipTable) {
          const elapsedTime = results.endTime - results.startTime;            
          this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${results.insertMode}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
        }
        await this.dbi.finalizeDataLoad();
      }
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      } 
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._final() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    } 
  } 
}

module.exports = DBWriter;