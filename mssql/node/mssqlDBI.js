"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const sql = require('mssql');

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StagingTable = require('./stagingTable.js');

const STAGING_TABLE =  { tableName : '#YADAMU_STAGING', columnName : 'DATA'}

const defaultParameters = {
  OWNER             : 'dbo'
, TOUSER            : 'dbo'
}

const sqlSystemInformation = 
`select db_Name() "DATABASE_NAME", current_user "CURRENT_USER", session_user "SESSION_USER", CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION",CONVERT(NVARCHAR(128),SERVERPROPERTY('MachineName')) "HOSTNAME"`;                     

const sqlCreateSavePoint = `SAVE TRANSACTION BulKInsert`;

const sqlRestoreSavePoint = `ROLLBACK TRANSACTION BulKInsert`;

class MSSQLDBI extends YadamuDBI {
    
  /*
  **
  ** Local methods 
  **
  */
  
  sqlTableInfo() {
   return `select t.table_schema "TABLE_SCHEMA"
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
                                          concat('"',column_name,'".${this.spatialSerializer} "',column_name,'"') 
                                        when data_type = 'geometry' then
                                          concat('"',column_name,'".${this.spatialSerializer} "',column_name,'"') 
                                        when data_type = 'datetime2' then
                                          concat('convert(VARCHAR(33),"',column_name,'",127) "',column_name,'"') 
                                        when data_type = 'datetimeoffset' then
                                          concat('convert(VARCHAR(33),"',column_name,'",127) "',column_name,'"') 
                                        when data_type = 'xml' then
                                          concat('replace(replace(convert(NVARCHAR(MAX),"',column_name,'"),''&#x0A;'',''\n''),''&#x20;'','' '') "',column_name,'"') 
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
  }    

  decomposeDataType(targetDataType) {
    const dataType = super.decomposeDataType(targetDataType);
    if (dataType.length === -1) {
      dataType.length = sql.MAX;
    }
    return dataType;
  }
  
  setConnectionProperties(connectionProperties) {

    if (!connectionProperties.options) {
      connectionProperties.options = { abortTransactionOnError : false }
    }
    else {
      connectionProperties.options.abortTransactionOnError = false
    }    
    super.setConnectionProperties(connectionProperties)
  }

  async useDatabase(databaseName) {
      
    const statement = `use ${databaseName}`
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }
    const results = await this.getRequest().batch(statement)
  }

  setTargetDatabase() {   
    if ((this.parameters.MSSQL_SCHEMA_DB) && (this.parameters.MSSQL_SCHEMA_DB !== this.connectionProperties.database)) {
      this.connectionProperties.database = this.parameters.MSSQL_SCHEMA_DB
    }
  }
  
  async setQuotedIdentifiers(databaseName) {
    const statement = `SET QUOTED_IDENTIFIER ON`
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }
    const results = await this.getRequest().batch(statement)
  }

  logConnectionParameters() {    
    if (this.status.sqlTrace) {
      const pwRedacted = Object.assign({},this.connectionProperties)
      delete pwRedacted.password
      this.status.sqlTrace.write(`--\n-- Connection Properies: ${JSON.stringify(pwRedacted)}\n--\n`)
    }
  }
  
  async getConnectionPool() {
    this.setTargetDatabase();
    this.logConnectionParameters();
    this.requestSource = await new sql.ConnectionPool(this.connectionProperties).connect();
    const yadamuLogger = this.yadamuLogger;
    this.requestSource .on('error',(err, p) => {
      this.yadamuLogger.logException([`${this.DATABASE_VENDOR}`,`Pool.onError()`],err);
      throw err
    })
    
    await this.setQuotedIdentifiers()
    // await this.setTargetDatabase()
    return this.requestSource ;      
 
 }
  
  async getConnection() {
    this.setTargetDatabase()
    this.logConnectionParameters();
    this.requestSource = await sql.connect(this.connectionProperties);
    await this.setQuotedIdentifiers()
    // await this.setTargetDatabase()
    return this.requestSource ;    
  }
  
  async verifyDataLoad(request,tableSpec) {    
    const statement = `select ISJSON("${tableSpec.columnName}") "VALID_JSON" from "${tableSpec.tableName}"`;
    const startTime = new Date().getTime();
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }  
    const results = await request.query(statement);
    this.yadamuLogger.log([`${this.constructor.name}.verifyDataLoad()`],`: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${new Date().getTime() - startTime}ms.`);
    return results;
  }
  
  getRequest() {
    // console.log(new Error(`REQUEST [${this.requestSource.constructor.name}]`).stack)
    return new sql.Request(this.requestSource);
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
        const results = await this.getRequest().batch(createSchema);
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.createSchema()`],e)
      }
    }     
  }
  
  async executeDDL(ddl) {
    
    await this.beginTransaction()     

    await this.createSchema(this.parameters.TOUSER);
    // Cannot use Promise.all with mssql Transaction class
    for (let ddlStatement of ddl) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TOUSER);
      try {
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`${ddlStatement}\ngo\n`);
        }
        const results = await this.getRequest().batch(ddlStatement)   
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.executeDDL()`],e)
        this.yadamuLogger.writeDirect(`${ddlStatement}\n`)
      } 
    }

    await this.commitTransaction()      

  }

  /*
  **
  ** Overridden Methods
  **
  */
   
  get DATABASE_VENDOR() { return 'MSSQLSERVER' };
  get SOFTWARE_VENDOR() { return 'Microsoft Corporation' };
  get SPATIAL_FORMAT()  { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return defaultParameters };

  constructor(yadamu) {
    
    super(yadamu,defaultParameters);

    this.pool = undefined;
    this.transaction = undefined;
    this.requestSource = undefined;
    this.sql = sql

    sql.on('error',(err, p) => {
      this.yadamuLogger.logException([`${this.DATABASE_VENDOR}`,`mssql.onError()`],err);
      throw err
    })
    
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  setSpatialSerializer(spatialFormat) {      
    switch (spatialFormat) {
      case "WKB":
        this.spatialSerializer = "STAsBinary()";
        break;
      case "EWKB":
        this.spatialSerializer = "AsBinaryZM()";
        break;
      case "WKT":
        this.spatialSerializer = "STAsText()";
        break;
      case "EWKT":
        this.spatialSerializer = "AsTextZM()";
        break;
     default:
        this.spatialSerializer = "AsBinaryZM()";
    }  
  }   
  
  async initialize() {
    super.initialize();      
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
    this.setSpatialSerializer(this.spatialFormat);
    this.pool = await this.getConnectionPool()
    this.transaction = new sql.Transaction(this.pool);
    this.requestSource = this.pool;
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
      , abortTransactionOnError : false
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
  ** Begin the current transaction
  **
  */
  
  async beginTransaction() {
    await this.transaction.begin();
    // console.log(new Error('BEGIN TRANSACTION').stack)
    this.requestSource = this.transaction
  }

  /*
  ** Commit the current transaction
  **
  **
  */
  
  async commitTransaction() {
    await this.transaction.commit();
    // console.log(new Error('COMMIT TRANSACTION').stack)
    this.requestSource = this.pool
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
    await this.transaction.rollback();
    // console.log(new Error('ROLLBACK TRANSACTION').stack)
    this.requestSource = this.pool
  }
  
  async createSavePoint() {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlCreateSavePoint}\ngo\n`)
    }
  }
  
  async restoreSavePoint() {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlRestoreSavePoint}\ngo\n`)
    }
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
    results = await this.verifyDataLoad(this.getRequest(),STAGING_TABLE);
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  async processFile(hndl) {
     let results = await this.getRequest().input('TARGET_DATABASE',sql.VarChar,this.parameters.TOUSER).execute('sp_IMPORT_JSON');
     results = results.recordset;
     const log = JSON.parse(results[0][Object.keys(results[0])[0]])
     super.processLog(log, this.status, this.yadamuLogger)
     return log
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
  
  async getSystemInformation(EXPORT_VERSION) {     
  
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlSystemInformation}\ngo\n`)
    }
    
    const results = await this.getRequest().query(sqlSystemInformation);
    const sysInfo =  results.recordsets[0][0];
   
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.OWNER
     ,exportVersion      : EXPORT_VERSION
	 ,sessionUser        : sysInfo.SESSION_USER
	 ,currentUser        : sysInfo.CURRENT_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,hostname           : sysInfo.HOSTNAME
     ,nodeClient         : {
        version          : process.version
       ,architecture     : process.arch
       ,platform         : process.platform
      } 
    }
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    return undefined
  }
   
  async getSchemaInfo(schemaKey) {
      
    const statement = this.sqlTableInfo()
      
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`--\n-- @SCHEMA="${this.parameters[schemaKey]}"\n--\n`)
      this.status.sqlTrace.write(`${statement}\ngo\n`)
    }

    const results = await this.getRequest().input('SCHEMA',sql.VarChar,this.parameters[schemaKey]).query(statement);
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
    return new DBParser(query,objectMode,this.yadamuLogger);
  }  
  
  async getInputStream(query,parser) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${query.SQL_STATEMENT}\ngo\n`)
    }
       
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
   
    const request = this.getRequest();
    request.stream = true // You can set streaming differently for each request
    request.on('row', function(row) {readStream.push(row)})
    request.on('done',function(result) {readStream.push(null)});
    request.query(query.SQL_STATEMENT) 
    return readStream;      
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
  
  async initializeExport() {
  }
  
  /*
  **
  ** The following methods are used by the YADAMU DBWriter class
  **
  */
  
  async initializeImport() {
  }
  
  async generateStatementCache(schema, executeDDL) {
    /* ### OVERRIDE ### Pass additional parameter Database Name */
    const statementGenerator = new StatementGenerator(this, schema, this.metadata, this.systemInformation.spatialFormat, this.batchSize, this.commitSize, this.status, this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL, this.systemInformation.vendor, this.parameters.MSSQL_SCHEMA_DB ? this.parameters.MSSQL_SCHEMA_DB : this.connectionProperties.database)
  }

  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)
  }
  
}

module.exports = MSSQLDBI
