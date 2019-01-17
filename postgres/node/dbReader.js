"use strict";
const Readable = require('stream').Readable;
const Transform = require('stream').Transform;
const QueryStream = require('pg-query-stream')

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'Postgres';
const SPATIAL_FORMAT = "WKT";

const sqlGetSystemInformation =
`select current_database() database_name,current_user,session_user,current_setting('server_version_num') database_version`;					   

const sqlGenerateQueries =
`select t.table_schema "TABLE_SCHEMA"
       ,t.table_name "TABLE_NAME"
	   ,string_agg('"' || column_name || '"',',' order by ordinal_position) "COLUMN_LIST" 
	   ,jsonb_agg(case when data_type = 'USER-DEFINED' then udt_name else data_type end order by ordinal_position) "DATA_TYPES"
       ,jsonb_agg(case
                     when (numeric_precision is not null) and (numeric_scale is not null) 
                       then cast(numeric_precision as varchar) || ',' || cast(numeric_scale as varchar)
                     when (numeric_precision is not null) 
                       then cast(numeric_precision as varchar)
                     when (character_maximum_length is not null)
                       then cast(character_maximum_length as varchar)
                     else
                       ''
                   end
                   order by ordinal_position
                 ) "SIZE_CONSTRAINTS"
	   ,'select jsonb_build_array(' || string_agg('"' || column_name || '"' ||
                                                  case   
                                                    when data_type = 'xml' then
                                                       '::text'
                                                    else
                                                      ''
                                                  end
                                                 ,',' order by ordinal_position
                                                 ) || ') "json" from "' || t.table_schema || '"."' || t.table_name ||'"' "SQL_STATEMENT" 
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
	and t.table_schema = c.table_schema
	and t.table_type = 'BASE TABLE'
    and t.table_schema = $1
  group by t.table_schema, t.table_name`;


class DBReader extends Readable {  

  constructor(pgClient,schema,outputStream,mode,status,logWriter,options) {

    super({objectMode: true });  
    const self = this;
  
    this.pgClient = pgClient
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
    

	const results = await this.pgClient.query(sqlGetSystemInformation);
	const sysInfo = results.rows[0];

	
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : DATABASE_VENDOR
     ,spatialFormat      : SPATIAL_FORMAT
     ,schema             : this.schema
     ,exportVersion      : EXPORT_VERSION
	 ,sessionUser        : sysInfo.session_user
     ,dbName             : sysInfo.database_name
     ,databaseVersion    : sysInfo.database_version
    }
    
  }

  async getDDLOperations() {
  }
   
  async getMetadata() {
             
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }
    
	const results = await this.pgClient.query(sqlGenerateQueries,[this.schema]);
    this.tableInfo = results.rows;
       
    const metadata = {}
	for (let table of this.tableInfo) {
      metadata[table.TABLE_NAME] = {
        owner                    : table.TABLE_SCHEMA
       ,tableName                : table.TABLE_NAME
       ,columns                  : table.COLUMN_LIST
       ,dataTypes                : table.DATA_TYPES
       ,sizeConstraints          : table.SIZE_CONSTRAINTS
      }
    }
    return metadata;    
  }
  
  async pipeTableData(sqlStatement,outStream) {

    let counter = 0;
    const parser = new Transform({objectMode:true});
    parser._transform = function(data,encodoing,done) {
      counter++;
      this.push({data : data.json})
      done();
    }
 
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
    }

    const queryStream = new QueryStream(sqlStatement)
    const stream = await this.pgClient.query(queryStream)   
  
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

module.exports = DBReader;    
 
  

