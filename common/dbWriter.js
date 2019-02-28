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
        
    this.systemInformation = undefined;
    this.metadata = undefined;

    this.currentTable = undefined;
    this.rowCount     = undefined;
    this.ddlComplete  = false;
  }      
  
  objectMode() {
    return true; 
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
          this.systemInformation = obj.systemInformation;
          break;
        case 'ddl':
          if ((this.ddlRequired) && (obj.ddl.length > 0) && (this.systemInformation.vendor === this.dbi.DATABASE_VENDOR)) {
            await this.dbi.executeDDL(this.schema, this.systemInformation, obj.ddl);
            this.ddlComplete = true;
          }
          break;
        case 'metadata':
          this.metadata = Yadamu.convertIdentifierCase(this.dbi.parameters.IDENTIFIER_CASE,obj.metadata);
          const targetTableInfo = await this.dbi.getTableInfo(this.schema,this.status);
          if (targetTableInfo.length > 0) {
             this.metadata = this.mergeMetadata(this.dbi.generateMetadata(targetTableInfo,false),this.metadata);
          }
          // ### if a DDL section has been processed then we can skip DDL as part of statement generation.
          if (Object.keys(this.metadata).length > 0) {   
            await this.dbi.generateStatementCache(this.schema,this.systemInformation,this.metadata,!this.ddlComplete)
          } 
          break;
        case 'table':
          if (this.currentTable === undefined) {
            await this.dbi.initializeDataLoad(this.systemInformation.vendor);
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
      }
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      } 
      await this.dbi.finalizeDataLoad();
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._final() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    } 
  } 
}

module.exports = DBWriter;