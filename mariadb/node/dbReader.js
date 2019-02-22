"use strict";
const Readable = require('stream').Readable;
const Transform = require('stream').Transform;

const MariaCore = require('./mariaCore.js');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'MariaDB';
const SPATIAL_FORMAT = "WKT";

const sqlGetSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID"`;					   


class DBReader extends Readable {  

  constructor(conn,schema,outputStream,mode,status,logWriter,options) {

    super({objectMode: true });  
    const self = this;
  
    this.conn = conn;
    this.schema = schema;
    this.outputStream = outputStream;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[DBReader ${DATABASE_VENDOR}]: Ready. Mode: ${this.mode}.\n`)
        
    this.sqlQueries = [];
    
    this.nextPhase = 'systemInformation'
    this.serverGeneration = undefined;
    this.maxVarcharSize = undefined;
  
  }
 
  async getSystemInformation() {     
  
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
    
	const results = await this.conn.query(sqlGetSystemInformation);
    const sysInfo = results[0];
	
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : DATABASE_VENDOR
     ,spatialFormat      : SPATIAL_FORMAT
     ,schema             : this.schema
     ,exportVersion      : EXPORT_VERSION
     ,sessionUser        : sysInfo.SESSION_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,serverHostName     : sysInfo.SERVER_HOST
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,serverVendor       : sysInfo.SERVER_VENDOR_ID
    }
    
  }

  async getDDLOperations() {
    return []
  }
   
  async getMetadata() {
             
	this.tableInfo = await MariaCore.getTableInfo(this.conn,this.schema,this.status);
    return MariaCore.generateMetadata(this.tableInfo);
    
  }
  
  
  pipeTableData(conn,sqlStatement,outputStream) {

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
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};  

    return new Promise(async function(resolve,reject) { 
      const outputStreamError = function(err){reject(err)}       
      outputStream.on('error',outputStreamError);

      conn.queryStream(sqlStatement).on('data',
      function(row) {
        counter++;
        if (outputStream.objectMode()) {
          readStream.push({data : JSON.parse(row.json)});
        } 
        else {
          readStream.push({data : row.json})
        }
      }).on('end',
      function() {
        readStream.push(null);
        waitUntilEmpty(outputStream,outputStreamError,resolve)
      }).on('error',
      function(err) {
        reject(err);
      }) 
      readStream.pipe(outputStream,{end: false })
    })
  }  
    
  async getTableData(tableInfo) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${tableInfo.SQL_STATEMENT}\n\/\n`)
    }

    const startTime = new Date().getTime()      
    const rows = await this.pipeTableData(this.conn,tableInfo.SQL_STATEMENT,this.outputStream) 
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
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._read()]} ${e}\n`);
      process.nextTick(() => this.emit('error',e));        
    }
  }
}

module.exports = DBReader