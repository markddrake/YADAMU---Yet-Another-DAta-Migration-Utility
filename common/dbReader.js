"use strict";
const Readable = require('stream').Readable;
const Yadamu = require('./yadamu.js');

class DBReader extends Readable {  

  constructor(dbi,schema,mode,status,logWriter,options) {

    super({objectMode: true });  
    const self = this;
  
    this.dbi = dbi;
    this.schema = schema;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[DBReader ${dbi.DATABASE_VENDOR}]: Ready. Mode: ${this.mode}.\n`)
       
    this.tableInfo = [];
    
    this.nextPhase = 'systemInformation'
    this.outputStream = undefined;
  
  }
  
  setOutputStream(outputStream) {
    this.outputStream = outputStream;
  }

  async getSystemInformation(schema,version) {
    return this.dbi.getSystemInformation(schema,version)
  }
  
  async getDDLOperations(schema) {
    return this.dbi.getDDLOperations(schema)
  }
  
  async getMetadata(schema) {
      

     this.tableInfo = await this.dbi.getTableInfo(schema)
     return this.dbi.generateMetadata(this.tableInfo)
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
      parser.on('finish',function() {waitUntilEmpty(outputStream,outputStreamError,resolve)})
      parser.on('error',function(err){reject(err)});
      inputStream.on('error',function(err){reject(err)});
      inputStream.pipe(parser).pipe(outputStream,{end: false })
    })
    
    const startTime = new Date().getTime()
    const rows = await copyOperation;
    const elapsedTime = new Date().getTime() - startTime
    this.logWriter.write(`${new Date().toISOString()}[DBReader "${tableMetadata.TABLE_NAME}"]: Rows read: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
    return rows;
      
  }
  
  async _read() {

    try {
      switch (this.nextPhase) {
         case 'systemInformation' :
           // await db.setDateFormatMask(this.conn,this.status,'Oracle')
           const sysInfo = await this.getSystemInformation(this.schema,Yadamu.EXPORT_VERSION);
           this.push({systemInformation : sysInfo});
           if (this.mode === 'DATA_ONLY') {
             this.nextPhase = 'metadata';
           }
           else { 
             this.nextPhase = 'ddl';
           }
           break;
         case 'ddl' :
           const ddl = await this.getDDLOperations(this.schema);
           this.push({ddl: ddl});
           if (this.mode === 'DDL_ONLY') {
             this.push(null);
           }
           else {
             this.nextPhase = 'metadata';
           }
           break;
         case 'metadata' :
           const metadata = await this.getMetadata(this.schema);
           this.push({metadata: metadata});
           this.nextPhase = 'table';
           break;
         case 'table' :
           if (this.mode !== 'DDL_ONLY') {
             if (this.tableInfo.length > 0) {
               this.push({table : this.tableInfo[0].TABLE_NAME})
               this.nextPhase = 'data'
               break;
             }
           }
           this.push(null);
           break;
         case 'data' :
           const rows = await this.copyContent(this.tableInfo[0],this.outputStream)
           this.push({rowCount:rows});
           this.tableInfo.splice(0,1)
           this.nextPhase = 'table';
           break;
         default:
      }
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._read()]} ${e}\n`);
      process.nextTick(() => this.emit('error',e));
    }
  }
}

module.exports = DBReader;