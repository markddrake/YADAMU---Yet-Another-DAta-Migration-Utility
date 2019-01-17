"use strict";
const Readable = require('stream').Readable;
const Transform = require('stream').Transform;

const MySQLCore = require('./mysqlCore.js');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'MySQL';
const SPATIAL_FORMAT = "WKT";

const sqlGetSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE"`;                     

const sqlGenerateMetadata =

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
              when data_type = 'timestamp'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%TZ'')')
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
        ) "SQL_STATEMENT"`;

// Check for duplicate entries INFORMATION_SCHEMA.columns

const sqlCheckInformationSchemaState =
`select distinct c.table_schema, c.table_name
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
  group by TABLE_SCHEMA,TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
  having count(*) > 1`


const sqlInformationSchemaClean =
`   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
      group by t.table_schema, t.table_name`;
      
    
// Hack for Duplicate Entries in INFORMATION_SCHEMA.columns seen MSSQL 5.7

const sqlInformationSchemaFix  = 
`   from (
     select distinct c.table_catalog, c.table_schema, c.table_name,column_name,ordinal_position,data_type,column_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision
       from information_schema.columns c, information_schema.tables t
       where t.table_name = c.table_name 
         and c.extra <> 'VIRTUAL GENERATED'
         and t.table_schema = c.table_schema
         and t.table_type = 'BASE TABLE'
         and t.table_schema = ?
   ) c
  group by c.table_schema, c.table_name`;

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
        
    this.tableInfo = [];
    
    this.nextPhase = 'systemInformation'
    this.serverGeneration = undefined;
    this.maxVarcharSize = undefined;
  
  }
 
  async getSystemInformation() {     

    const results = await MySQLCore.query(this.conn,this.status,sqlGetSystemInformation); 
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
   
  async getMetadataQuery() { 

    /*
    **
    ** During testing on 5.7 it appeared tha that is is possible for the Information Schema to get corrupted
    ** In this state it contains duplicate entires for each column in the table.
    ** 
    ** This routine checks for this state and creates a query that will workaround the problem if the 
    ** Information schema is corrupt.
    ** 
    */   
   
    const results = await MySQLCore.query(this.conn,this.status,sqlCheckInformationSchemaState,[this.schema]);
    if (results.length ===  0) {
      return sqlGenerateMetadata + sqlInformationSchemaClean;
    }
    else {
      for (const i in results) {
        this.logWriter.write(`${new Date().toISOString()}[WARNING]: Table: "${results[i].TABLE_SCHEMA}"."${results[i].TABLE_NAME}". Duplicate entires detected in INFORMATION_SCHEMA.COLUMNS.\n`)
      }
      return sqlGenerateMetadata + sqlInformationSchemaFix;
    }
  }  
  
  async getMetadata() {
             
    const sqlGenerateTables = await this.getMetadataQuery();
       
    this.tableInfo = await MySQLCore.query(this.conn, this.status, sqlGenerateTables,[this.schema]);
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
  
  
  pipeTableData(sqlQuery,outStream) {

    let counter = 0;
    const parser = new Transform({objectMode:true});
    parser._transform = function(data,encodoing,done) {
      counter++;
      this.push({data : data.json})
      done();
    }
 
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlQuery};\n--\n`);
    }
    const stream = this.conn.query(sqlQuery).stream();

    return new Promise(async function(resolve,reject) {
      const outStreamError = function(err){reject(err)}        
      outStream.on('error',outStreamError);
      parser.on('finish',function() {outStream.removeListener('error',outStreamError);resolve(counter)})
      parser.on('error',function(err){reject(err)});
      stream.on('error',function(err){reject(err)});
      stream.pipe(parser).pipe(outStream,{end: false })
    })
  }
    
  async getTableData(table) {

    const startTime = new Date().getTime()
    const rows = await this.pipeTableData(table.SQL_STATEMENT,this.outputStream) 
    const elapsedTime = new Date().getTime() - startTime
    this.logWriter.write(`${new Date().toISOString()}[DBReader "${table.TABLE_NAME}"]: Rows read: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
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

module.exports = DBReader