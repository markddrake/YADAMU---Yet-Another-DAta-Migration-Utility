"use strict";
const Readable = require('stream').Readable;
const Transform = require('stream').Transform;
const sql = require('mssql');

const DATABASE_VENDOR = 'MSSQLSERVER';
const SOFTWARE_VENDOR = 'Microsoft Corporation';
const EXPORT_VERSION = 1.0;
const SPATIAL_FORMAT = "EWKT";

const MsSQLCore = require('./mssqlCore.js');

const sqlGetSystemInformation = 
`select db_Name() "DATABASE_NAME", current_user "CURRENT_USER", session_user "SESSION_USER", CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION",CONVERT(NVARCHAR(128),SERVERPROPERTY('MachineName')) "HOSTNAME"`;                     

const sqlGenerateQueries =
`select t.table_schema "TABLE_SCHEMA"
       ,t.table_name   "TABLE_NAME"
       ,string_agg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position) "COLUMN_LIST"
       ,string_agg(concat('"',data_type,'"'),',') within group (order by ordinal_position) "DATA_TYPES"
       ,string_agg(case
                     when (numeric_precision is not null) and (numeric_scale is not null) 
                       then concat('"',numeric_precision,',',numeric_scale,'"')
                     when (numeric_precision is not null) 
                       then concat('"',numeric_precision,'"')
                     when (datetime_precision is not null)
                       then concat('"',datetime_precision,'"')
                     when (character_maximum_length is not null)
                       then concat('"',character_maximum_length,'"')
                     else
                       '""'
                   end
                  ,','
                 )
                 within group (order by ordinal_position) "SIZE_CONSTRAINTS"
       ,concat('select ',string_agg(case 
                                      when data_type = 'hierarchyid' then
                                        concat('cast("',column_name,'" as NVARCHAR(4000)) "',column_name,'"') 
                                      when data_type = 'geography' then
                                        concat('"',column_name,'".AsTextZM() "',column_name,'"') 
                                      when data_type = 'geometry' then
                                        concat('"',column_name,'".AsTextZM() "',column_name,'"') 
                                      when data_type = 'datetime2' then
                                        concat('convert(VARCHAR(33),"',column_name,'",127) "',column_name,'"') 
                                      when data_type = 'datetimeoffset' then
                                        concat('convert(VARCHAR(33),"',column_name,'",127) "',column_name,'"') 
                                      else 
                                        concat('"',column_name,'"') 
                                    end
                                   ,','
                                  ) 
                                  within group (order by ordinal_position)
                        ,' from "',t.table_schema,'"."',t.table_name,'"') "SQL_STATEMENT"
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = @SCHEMA
  group by t.table_schema, t.table_name`;    


class DBReader extends Readable {  

  constructor(pool,schema,outputStream,mode,status,logWriter,options) {

    super({objectMode: true });  
    const self = this;
  
    this.pool = pool
    this.request = this.pool.request();

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
    
    const results = await this.request.query(sqlGetSystemInformation);
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
             
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }
    
    const results = await this.request.input('SCHEMA',sql.VARCHAR,this.schema).batch(sqlGenerateQueries);
    this.tableInfo = results.recordsets[0]
       
    const metadata = {}
	for (let table of this.tableInfo) {
      metadata[table.TABLE_NAME] = {
        owner                    : table.TABLE_SCHEMA
       ,tableName                : table.TABLE_NAME
       ,columns                  : table.COLUMN_LIST
       ,dataTypes                : JSON.parse('[' + table.DATA_TYPES + ']')
       ,sizeConstraints          : JSON.parse('[' + table.SIZE_CONSTRAINTS + ']')
      }
    }
    return metadata;    
  }
    
  async pipeTableData(request,tableInfo,outStream) {

    let counter = 0;
    const column_list = JSON.parse(`[${tableInfo.COLUMN_LIST}]`);
    
    request.stream = true // You can set streaming differently for each request
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
    
    return new Promise(async function(resolve,reject) { 
      const outStreamError = function(err){reject(err)}        
      outStream.on('error',outStreamError);

      request.on('done', 
      function(result) {
        readStream.push(null);
        outStream.removeListener('error',outStreamError)
        resolve(counter)
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
        readStream.push({data : array})
      })
      
      request.on('error',
      function(err) {
        reject(err)
      })      
      request.query(tableInfo.SQL_STATEMENT) 
      readStream.pipe(outStream,{end: false })
    })
  }
    
  async getTableData(tableInfo) {

    const startTime = new Date().getTime()
    const rows = await this.pipeTableData(new sql.Request(this.pool),tableInfo,this.outputStream) 
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
           if (this.tableInfo.length > 0) {
             this.request = this.pool.request();
             this.push({table : this.tableInfo[0].TABLE_NAME})
             this.nextPhase = 'data'
           }
           else {
             this.push(null);
           }
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
 
  

