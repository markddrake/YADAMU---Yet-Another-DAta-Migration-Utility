"use strict";
const Readable = require('stream').Readable;
const Yadamu = require('./yadamu.js');

class DBReader extends Readable {  

  constructor(dbi,mode,status,logWriter,options) {

    super({objectMode: true });  
    const self = this;
 
    this.dbi = dbi;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[DBReader ${dbi.DATABASE_VENDOR}]: Ready. Mode: ${this.mode}.\n`)
       
    this.schemaInfo = [];
    
    this.nextPhase = 'systemInformation'
    this.ddlCompleted = false;
    this.outputStream = undefined;
      
  }
  
  setOutputStream(outputStream) {
    this.outputStream = outputStream;
  }

  async getSystemInformation(version) {
    return this.dbi.getSystemInformation(version)
  }
  
  async getDDLOperations() {
    return this.dbi.getDDLOperations()
  }
  
  async getMetadata() {
      
     this.schemaInfo = await this.dbi.getSchemaInfo('OWNER')
     return this.dbi.generateMetadata(this.schemaInfo)
  }
      
  async copyContent(tableMetadata,outputStream) {
    
    const query = await this.dbi.generateSelectStatement(tableMetadata)
    const parser = this.dbi.createParser(query,outputStream.objectMode())
    const inputStream = await this.dbi.getInputStream(query,parser)

    function waitUntilEmpty(outputStream,outputStreamError,resolve) {
        
      const recordsRemaining = outputStream.writableLength;
      if (recordsRemaining === 0) {
        outputStream.removeListener('error',outputStreamError)
        // console.log(`${new Date().toISOString()}[${DATABASE_VENDOR}]: Writer Complete.`);
        resolve(parser.getCounter());
      } 
      else  {
        // console.log(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBReader Records Reamaining ${recordsRemaining}.`);
        setTimeout(waitUntilEmpty, 10,outputStream,outputStreamError,resolve);
      }   
    }
    
    const copyOperation = new Promise(function(resolve,reject) {  
      const outputStreamError = function(err){reject(err)}       
      outputStream.on('error',outputStreamError);
      parser.on('end',function() {waitUntilEmpty(outputStream,outputStreamError,resolve)})
      parser.on('error',function(err){reject(err)});
      inputStream.on('error',function(err){reject(err)});
      inputStream.pipe(parser).pipe(outputStream,{end: false })
    })
    
    const startTime = new Date().getTime()
    const rows = await copyOperation;
    const elapsedTime = new Date().getTime() - startTime
    this.logWriter.write(`${new Date().toISOString()}[DBReader.copyContent("${tableMetadata.TABLE_NAME}")]: Rows read: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
    return rows;
      
  }
  
  async generateStatementCache(metadata) {
    if (Object.keys(metadata).length > 0) {   
      // ### if the import already processed a DDL object do not execute DDL when generating statements.
      Object.keys(metadata).forEach(function(table) {
         metadata[table].vendor = this.dbi.systemInformation.vendor;
      },this)
    }
    this.dbi.setMetadata(metadata)      
    await this.dbi.generateStatementCache('%%SCHEMA%%',false)
  }
  
  
  getReader() {
    
    if (this.dbi.isDatabase()) {
      return this
    }
    else {
      return this.dbi.getReader()
    }
    
  }
  
  async _read() {
  
    try {
      switch (this.nextPhase) {
         case 'systemInformation' :
           const systemInformation = await this.getSystemInformation(Yadamu.EXPORT_VERSION);
           // Needed in case we have to generate DDL from the system information and metadata.
           this.dbi.setSystemInformation(systemInformation);
           this.push({systemInformation : systemInformation});
           if (this.mode === 'DATA_ONLY') {
             this.nextPhase = 'metadata';
           }
           else { 
             this.nextPhase = 'ddl';
           }
           break;
         case 'ddl' :
           let ddl = await this.getDDLOperations();
           if (ddl === undefined) {
             // Database does not provide mechansim to retrieve DDL statements used to create a schema (eg Oracle's DBMS_METADATA package)
             // Reverse Engineer DDL from metadata.
             const metadata = await this.getMetadata();
             await this.generateStatementCache(metadata)
             ddl = Object.keys(this.dbi.statementCache).map(function(table) {
               return this.dbi.statementCache[table].ddl
             },this)
           } 
           this.push({ddl: ddl});
           if (this.mode === 'DDL_ONLY') {
             this.push(null);
             break;
           }
           this.nextPhase = 'metadata';
           break;
         case 'metadata' :
           const metadata = await this.getMetadata();
           this.push({metadata: metadata});
           this.nextPhase = 'table';
           break;
         case 'table' :
           if (this.mode !== 'DDL_ONLY') {
             if (this.schemaInfo.length > 0) {
               this.push({table : this.schemaInfo[0].TABLE_NAME})
               this.nextPhase = 'data'
               break;
             }
           }
           this.push(null);
           break;
         case 'data' :
           const rows = await this.copyContent(this.schemaInfo[0],this.outputStream)
           this.push({rowCount:rows});
           this.schemaInfo.splice(0,1)
           this.nextPhase = 'table';
           break;
         default:
      }
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBReader._read()]} ${e.stack}\n`);
      process.nextTick(() => this.emit('error',e));
    }
  }
}

module.exports = DBReader;