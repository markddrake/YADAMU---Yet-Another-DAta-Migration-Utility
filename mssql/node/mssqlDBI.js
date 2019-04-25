"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const sql = require('mssql');

const Yadamu = require('../../common/yadamu.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StagingTable = require('./stagingTable.js');

const STAGING_TABLE =  { tableName : '#JSON_STAGING', columnName : 'DATA'}

const defaultParameters = {
  BATCHSIZE         : 10000
, COMMITSIZE        : 10000
, PORT              : 1433
, OWNER             : 'dbo'
, TOUSER            : 'dbo'
}

const sqlTableInfo =
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

const sqlSystemInformation = 
`select db_Name() "DATABASE_NAME", current_user "CURRENT_USER", session_user "SESSION_USER", CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION",CONVERT(NVARCHAR(128),SERVERPROPERTY('MachineName')) "HOSTNAME"`;                     

class MsSQLDBI extends YadamuDBI {
    
  /*
  **
  ** Local methods 
  **
  */
  
  decomposeDataType(targetDataType) {
    const dataType = Yadamu.decomposeDataType(targetDataType);
    if (dataType.length === -1) {
      dataType.length = sql.MAX;
    }
    return dataType;
  }
  
  async getConnectionPool() {
  
    const logWriter = this.logWriter;
    if (this.status.sqlTrace) {
      const pwRedacted = Object.assign({},this.connectionProperties)
      delete pwRedacted.password
      this.status.sqlTrace.write(`-- Connection Properies: ${JSON.stringify(pwRedacted)}\ngo\n`)
    }
    const pool = await new sql.ConnectionPool(this.connectionProperties).connect();
    pool.on('error',function(err){logWriter.log('Pool Error:',err)});
    const statement = `SET QUOTED_IDENTIFIER ON`
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }
    await pool.query(statement);
    return pool;      
 
 }
  
  async getConnection() {
  
    const conn = await sql.connect(this.connectionProperties);
    const statement = `SET QUOTED_IDENTIFIER ON`
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }
    await conn.query(statement);
    return conn;     

  }
  
  async useDatabase(pool,databaseName,status) {
      
    const statement = `use ${databaseName}`
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }
    await pool.query(statement);
  }
   
  async verifyDataLoad(request,tableSpec) {    
    const statement = `select ISJSON("${tableSpec.columnName}") "VALID_JSON" from "${tableSpec.tableName}"`;
    const startTime = new Date().getTime();
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }  
    const results = await request.query(statement);
    this.logWriter.write(`${new Date().toISOString()}: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${new Date().getTime() - startTime}ms.\n`);
    return results;
  }
  
  getRequest() {
     return this.pool.request();
  }
      
  getPreparedStatement() {
     return  new sql.PreparedStatement(this.pool)
  }
  
  async createSchema(schema) {
    
    if (schema !== 'dbo') {
      const createSchema = `if not exists (select 1 from sys.schemas where name = N'${schema}') exec('create schema "${schema}"')`;
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${createSchema}\ngo\n`);
      }
      try {
        const results = await this.pool.request().batch(createSchema);
      } catch (e) {
        console.log(e);
      }
    }     
  }
  
  async executeDDL(schema, ddl) {
    await this.createSchema(schema);
    await Promise.all(ddl.map(async function(ddlStatement) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,schema);
      try {
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`${ddlStatement}\ngo\n`);
        }
        const results = await this.pool.request().batch(ddlStatement)   
      } catch (e) {
        this.logWriter.write(`${e}\n${ddlStatement}\n`)
      } 
    },this))
  }

  /*
  **
  ** Overridden Methods
  **
  */
   
  get DATABASE_VENDOR() { return 'MSSQLSERVER' };
  get SOFTWARE_VENDOR() { return 'Microsoft Corporation' };
  get SPATIAL_FORMAT()  { return 'EWKT' };
  get DEFAULT_PARAMETERS() { return defaultParameters };

  constructor(yadamu) {
    
    super(yadamu,defaultParameters);
    this.pool = undefined;
    this.sql = sql
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize(schema) {
     this.pool = await this.getConnectionPool()
  }

    
  getConnectionProperties() {
    return {
      server          : this.parameters.HOSTNAME
    , user            : this.parameters.USERNAME
    , database        : this.parameters.DATABASE
    , password        : this.parameters.PASSWORD
    , port            : this.parameters.PORT
    , requestTimeout  : 2 * 60 * 60 * 10000
    , options         : {
        encrypt: false // Use this if you're on Windows Azure
      }
    }
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    await this.pool.close();
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    if (this.pool !== undefined) {
      await this.pool.close();
    }
  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
  }
  
  /*
  **
  ** The following methods are used by JSON_TABLE() style import operations  
  **
  */

  /*
  **
  **  Upload a JSON File to the server. Optionally return a handle that can be used to process the file
  **
  */
  
  async uploadFile(importFilePath) {
    
    const stagingTable = new StagingTable(this.pool,STAGING_TABLE,importFilePath,this.status); 
    let results = await stagingTable.uploadFile()
    results = await this.verifyDataLoad(this.pool.request(),STAGING_TABLE);
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  async processFile(hndl) {
     let results = await this.pool.request().input('TARGET_DATABASE',sql.VarChar,this.parameters.TOUSER).execute('sp_IMPORT_JSON');
     results = results.recordset;
     return  JSON.parse(results[0][Object.keys(results[0])[0]])
  }
  
  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
  
  /*
  **
  **  Generate the SystemInformation object for an Export operation
  **
  */
  
  async getSystemInformation(schema,EXPORT_VERSION) {     
  
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlSystemInformation}\ngo\n`)
    }
    
    const results = await this.pool.request().query(sqlSystemInformation);
    const sysInfo =  results.recordsets[0][0];
   
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.schema
     ,exportVersion      : EXPORT_VERSION
	 ,sessionUser        : sysInfo.SESSION_USER
	 ,currentUser        : sysInfo.CURRENT_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,hostname           : sysInfo.HOSTNAME
    }
    
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations(schema) {
    return undefined
  }
  
  async getSchemaInfo(schema) {
      
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlTableInfo}\ngo\n`)
    }
    
    const results = await this.pool.request().input('SCHEMA',sql.VarChar,schema).query(sqlTableInfo);
    return results.recordsets[0]
  
  }

  generateMetadata(tableInfo,server) {    
    const metadata = {}
    for (let table of tableInfo) {
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

  createParser(query,objectMode) {
    return new DBParser(query,objectMode,this.logWriter);
  }  
  
  async getInputStream(query,parser) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${query.SQL_STATEMENT}\ngo\n`)
    }
    
       
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
   
    const request = this.pool.request();
    request.stream = true // You can set streaming differently for each request
    request.on('row', function(row) {readStream.push(row)})
    request.on('done',function(result) {readStream.push(null)});
    request.query(query.SQL_STATEMENT) 
    return readStream;      
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
  
  async initializeDataLoad(schema) {
  }
  
  async generateStatementCache(schema,executeDDL) {
    /* ### OVERRIDE ### Pass additional parameter Database Name */
    const statementGenerator = new StatementGenerator(this, this.parameters.BATCHSIZE, this.parameters.COMMITSIZE, this.status, this.logWriter);
    this.statementCache = await statementGenerator.generateStatementCache(schema, this.metadata, executeDDL, this.connectionProperties.database)
  }
  
  getTableWriter(schema,table) {
    return super.getTableWriter(TableWriter,schema,table)
  }

  async finalizeDataLoad() {
  }  

}

module.exports = MsSQLDBI
