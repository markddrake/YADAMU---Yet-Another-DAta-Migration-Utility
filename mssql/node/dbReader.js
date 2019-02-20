"use strict";
const Readable = require('stream').Readable;
const Transform = require('stream').Transform;
const sql = require('mssql');

const MsSQLCore = require('./mssqlCore.js');

const DATABASE_VENDOR = 'MSSQLSERVER';
const SOFTWARE_VENDOR = 'Microsoft Corporation';
const EXPORT_VERSION = 1.0;
const SPATIAL_FORMAT = "EWKT";


const sqlGetSystemInformation = 
`select db_Name() "DATABASE_NAME", current_user "CURRENT_USER", session_user "SESSION_USER", CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION",CONVERT(NVARCHAR(128),SERVERPROPERTY('MachineName')) "HOSTNAME"`;                     


class DBReader extends Readable {  

  constructor(pool,schema,outputStream,mode,status,logWriter,options) {

    super({objectMode: true });  
    const self = this;
  
    this.pool = pool

    this.schema = schema;
    this.outputStream = outputStream;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBReader ready. Mode: ${this.mode}.\n`)
        
    this.tableInfo = [];
    
    this.nextPhase = 'systemInformation'
    this.serverGeneration = undefined;
    this.maxVarcharSize = undefined;
  
  }

  async getSystemInformation() {     
  
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
    
    const results = await this.pool.request().query(sqlGetSystemInformation);
    const sysInfo =  results.recordsets[0][0];
   
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : DATABASE_VENDOR
     ,spatialFormat      : SPATIAL_FORMAT
     ,schema             : this.schema
     ,exportVersion      : EXPORT_VERSION
	 ,sessionUser        : sysInfo.SESSION_USER
	 ,currentUser        : sysInfo.CURRENT_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,softwareVendor     : SOFTWARE_VENDOR
     ,hostname           : sysInfo.HOSTNAME
    }
    
  }

  async getDDLOperations() {
  }
   
  async getMetadata() {
     this.tableInfo = await MsSQLCore.getTableInfo(this.pool.request(),this.schema,this.status)
     return MsSQLCore.generateMetadata(this.tableInfo)
  }   

  async pipeTableData(request,tableInfo,outputStream) {

    function waitUntilEmpty(outputStream,outputStreamError,resolve) {
        
      const recordsRemaining = outputStream.writableLength;
      if (recordsRemaining === 0) {
        outputStream.removeListener('error',outputStreamError)
        // console.log(`${new Date().toISOString()}[${DATABASE_VENDOR}]: Writer Complete.`);
        resolve(counter);
      } 
      else  {
        // console.log(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBReader Records Reamaining ${recordsRemaining}.`);
        setTimeout(waitUntilEmpty, 10,outputStream,outputStreamError,resolve);
      }   
    }

    let counter = 0;
    const column_list = JSON.parse(`[${tableInfo.COLUMN_LIST}]`);
    
    request.stream = true // You can set streaming differently for each request
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
    
    return new Promise(async function(resolve,reject) { 
      const outputStreamError = function(err){reject(err)}        
      outputStream.on('error',outputStreamError);

      request.on('done', 
      function(result) {
        readStream.push(null);
        waitUntilEmpty(outputStream,outputStreamError,resolve)
      })
  
      request.on('row', 
      function(row){
        counter++
        const array = []
        for (let i=0; i < column_list.length; i++) {
          if (row[column_list[i]] instanceof Buffer) {
            array.push(row[column_list[i]].toString('hex'))
          }
          else {
            array.push(row[column_list[i]]);
          }
        }
        if (!outputStream.objectMode()) {
          readStream.push({data : JSON.stringify(array)})
        }
        else {        
          readStream.push({data : array})
        }
      })
      
      request.on('error',
      function(err) {
        reject(err)
      })      
      request.query(tableInfo.SQL_STATEMENT) 
      readStream.pipe(outputStream,{end: false })
    })
  }
    
  async getTableData(tableInfo) {

    const startTime = new Date().getTime()
    const rows = await this.pipeTableData(this.pool.request(),tableInfo,this.outputStream) 
    const elapsedTime = new Date().getTime() - startTime
    this.logWriter.write(`${new Date().toISOString()}[DBReader "${tableInfo.TABLE_NAME}"]: Rows read: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
    return rows;
  }
  
  async _read() {
      
    try {
      switch (this.nextPhase) {
         case 'systemInformation' :
           const sysInfo = await this.getSystemInformation();
           this.push({systemInformation : sysInfo});
           if (this.mode === 'DATA_ONLY') {
             this.nextPhase = 'metadata';
           }
           else {
             this.nextPhase = 'ddl';
           }
           break;
         case 'ddl' :
           const ddl = await this.getDDLOperations();
           this.push({ddl: ddl});
           if (this.mode === 'DDL_ONLY') {
             this.push(null);
           }
           else {
             this.nextPhase = 'metadata';
           }
           break;
         case 'metadata' :
           const metadata = await this.getMetadata();
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
           if (this.tableInfo.length > 0) {
             this.push({table : this.tableInfo[0].TABLE_NAME})
             this.nextPhase = 'data'
           }
           this.push(null);
           break;
         case 'data' :
           const rows = await this.getTableData(this.tableInfo[0])
           
           
           this.push({rowCount:rows});
           this.tableInfo.splice(0,1)
           this.nextPhase = 'table';
           break;
         default:
      }
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }
  }
}

module.exports = DBReader;    
 
  

