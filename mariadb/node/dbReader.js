"use strict";
const Readable = require('stream').Readable;
const Transform = require('stream').Transform;

const MariaCore = require('./mariaCore.js');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'MariaDB';
const SPATIAL_FORMAT = "WKT";


const sqlGetSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID"`;					   

const sqlGenerateTables = 
// Cannot use JSON_ARRAYAGG for DATA_TYPES and SIZE_CONSTRAINTS beacuse MYSQL implementation of JSON_ARRAYAGG does not support ordering
 
`select c.table_schema "TABLE_SCHEMA"
       ,c.table_name "TABLE_NAME"
       ,group_concat(concat('"',column_name,'"') order by ordinal_position separator ',')  "COLUMN_LIST"
       ,concat('[',group_concat(json_quote(data_type) order by ordinal_position separator ','),']')  "DATA_TYPES"
       ,concat('[',group_concat(json_quote(
                            case when (numeric_precision is not null) and (numeric_scale is not null)
                                   then concat(numeric_precision,',',numeric_scale) 
                                 when (numeric_precision is not null)
                                   then case
                                          when column_type like '%unsigned' 
                                            then numeric_precision
                                          else
                                            numeric_precision + 1
                                        end
                                 when (datetime_precision is not null)
                                   then datetime_precision
                                 when (character_maximum_length is not null)
                                   then character_maximum_length
                                 else   
                                   ''   
                            end
                           ) 
                           order by ordinal_position separator ','
                    ),']') "SIZE_CONSTRAINTS"
       ,concat(
          'select json_array('
          ,group_concat(
            case 
              when data_type = 'date'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%TZ'')')
              when data_type = 'timestamp'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')')
              when data_type = 'datetime'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT("', column_name, '", ''%Y-%m-%dT%T.%f'')')
              when data_type = 'year'
                -- Prevent rendering of value as base64:type13: 
                then concat('CAST("', column_name, '"as DECIMAL)')
              when data_type like '%blob'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              when data_type = 'varbinary'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              when data_type = 'binary'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              when data_type = 'geometry'
                -- Force WKT rendering of value
                then concat('ST_asText("', column_name, '")')
              else
                concat('"',column_name,'"')
            end
            order by ordinal_position separator ','
          )
          ,') "json" from "'
          ,c.table_schema
          ,'"."'
          ,c.table_name
          ,'"'
        ) "SQL_STATEMENT"
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
     and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
	  group by t.table_schema, t.table_name`;

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
    this.logWriter.write(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBReader ready. Mode: ${this.mode}.\n`)
        
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
  }
   
  async getMetadata() {
             
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
    
	this.tableInfo = await this.conn.query(sqlGenerateTables,[this.schema]);
    const metadata = {}
	for (let table of this.tableInfo) {
      metadata[table.TABLE_NAME] = {
        owner                    : table.TABLE_SCHEMA
       ,tableName                : table.TABLE_NAME
       ,columns                  : table.COLUMN_LIST
       ,dataTypes                : JSON.parse(table.DATA_TYPES)
       ,sizeConstraints          : JSON.parse(table.SIZE_CONSTRAINTS)
      }
    }
    return metadata;    
  }
  
  
  pipeTableData(conn,sqlStatement,outStream) {

    function waitUntilEmpty(outStream,resolve) {
        
      const recordsRemaining = outStream.writableLength;
      if (recordsRemaining === 0) {
        resolve(counter);
      } 
      else  {
        // console.log(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBReader Records Reamaining {$recordsRemaining}.`);
        setTimeout(waitUntilEmpty, 10,outStream,resolve);
      }   
    }

    let counter = 0;
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};  

    return new Promise(async function(resolve,reject) { 
      const outStreamError = function(err){reject(err)}       
      outStream.on('error',outStreamError);

      conn.queryStream(sqlStatement).on('data',
      function(row) {
        counter++;
        readStream.push({data : row.json})
      }).on('end',
      function() {
        readStream.push(null);
        outStream.removeListener('error',outStreamError)
        waitUntilEmpty(outStream,resolve)
      }).on('error',
      function(err) {
        reject(err);
      }) 
      readStream.pipe(outStream,{end: false })
    })
  }  
    
  async getTableData(tableInfo) {

    const startTime = new Date().getTime()
    const rows = await this.pipeTableData(this.conn,tableInfo.SQL_STATEMENT,this.outputStream) 
    const elapsedTime = new Date().getTime() - startTime
    this.logWriter.write(`${new Date().toISOString()}[DBReader "${tableInfo.TABLE_NAME}"]: Rows read: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
    return rows;
  }
  
  async _read() {

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
  }
}

module.exports = DBReader