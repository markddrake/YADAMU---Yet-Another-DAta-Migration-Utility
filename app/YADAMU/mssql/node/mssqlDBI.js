"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const sql = require('mssql');

/*
**
** Unlike most Driver's which have a concept of a pool and connections MsSQL uses pool and request. 
** The pool which acts a requestProvider, providing request objects on demand.
** Each request is good for one operation.
**
** When working in parallel Master and Slave instances share the same Pool.
**
** Transactions are managed via a Transaction object. The transaction object owns a connection.
** Each instance of the DBI owns it's own Transaction object. 
** When operations need to be transactional the Transaction object becomes the requestProvider for the duration of the transaction.
**
*/


const YadamuLibrary = require('../../common/yadamuLibrary.js')
const YadamuDBI = require('../../common/yadamuDBI.js');
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StagingTable = require('./stagingTable.js');

const STAGING_TABLE =  { tableName : '#YADAMU_STAGING', columnName : 'DATA'}

const sqlSystemInformation = 
`select db_Name() "DATABASE_NAME", current_user "CURRENT_USER", session_user "SESSION_USER", CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION",CONVERT(NVARCHAR(128),SERVERPROPERTY('MachineName')) "HOSTNAME"`;                     

const sqlCreateSavePoint = `SAVE TRANSACTION BulkInsert`;

const sqlRestoreSavePoint = `ROLLBACK TRANSACTION BulkInsert`;

class MsSQLDBI extends YadamuDBI {
    
  /*
  **
  ** Local methods 
  **
  */
  
  sqlTableInfo() {
     
    let spatialClause = `concat('"',"COLUMN_NAME",'".${this.spatialSerializer} "',"COLUMN_NAME",'"')`
   
    if (this.parameters.SPATIAL_MAKE_VALID === true) {
      spatialClause = `concat('case when "',"COLUMN_NAME",'".STIsValid = 0 then "',"COLUMN_NAME",'".makeValid().${this.spatialSerializer} else "',"COLUMN_NAME",'".${this.spatialSerializer} "',"COLUMN_NAME",'"')`
    }
      
    return `select t."TABLE_SCHEMA" "TABLE_SCHEMA"
                  ,t."TABLE_NAME"   "TABLE_NAME"
                  ,string_agg(concat('"',c."COLUMN_NAME",'"'),',') within group (order by "ORDINAL_POSITION") "COLUMN_LIST"
                  ,string_agg(concat('"',"DATA_TYPE",'"'),',') within group (order by "ORDINAL_POSITION") "DATA_TYPES"
                  ,string_agg(concat('"',"COLLATION_NAME",'"'),',') within group (order by "ORDINAL_POSITION") "COLLATION_NAMES"
                  ,string_agg(case
                                 when ("NUMERIC_PRECISION" is not null) and ("NUMERIC_SCALE" is not null) 
                                   then concat('"',"NUMERIC_PRECISION",',',"NUMERIC_SCALE",'"')
                                 when ("NUMERIC_PRECISION" is not null) 
                                   then concat('"',"NUMERIC_PRECISION",'"')
                                 when ("DATETIME_PRECISION" is not null)
                                   then concat('"',"DATETIME_PRECISION",'"')
                                 when ("CHARACTER_MAXIMUM_LENGTH" is not null)
                                   then concat('"',"CHARACTER_MAXIMUM_LENGTH",'"')
                                 else
                                   '""'
                               end
                              ,','
                             )
                    within group (order by "ORDINAL_POSITION") "SIZE_CONSTRAINTS"
                   ,concat('select ',string_agg(case 
                                                  when "DATA_TYPE" = 'hierarchyid' then
                                                    concat('cast("',"COLUMN_NAME",'" as NVARCHAR(4000)) "',"COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" in ('geography','geometry') then
                                                    ${spatialClause}
                                                  when "DATA_TYPE" = 'datetime2' then
                                                    concat('convert(VARCHAR(33),"',"COLUMN_NAME",'",127) "',"COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" = 'datetimeoffset' then
                                                    concat('convert(VARCHAR(33),"',"COLUMN_NAME",'",127) "',"COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" = 'xml' then
                                                    concat('replace(replace(convert(NVARCHAR(MAX),"',"COLUMN_NAME",'"),''&#x0A;'',''\n''),''&#x20;'','' '') "',"COLUMN_NAME",'"') 
                                                  else 
                                                    concat('"',"COLUMN_NAME",'"') 
                                                end
                                               ,','
                                               ) 
                                     within group (order by "ORDINAL_POSITION")
                           ,' from "',t."TABLE_SCHEMA",'"."',t."TABLE_NAME",'"') "SQL_STATEMENT"
              from "INFORMATION_SCHEMA"."COLUMNS" c, "INFORMATION_SCHEMA"."TABLES" t
             where t."TABLE_NAME" = c."TABLE_NAME"
               and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
               and t."TABLE_TYPE" = 'BASE TABLE'
               and t."TABLE_SCHEMA" = @SCHEMA
             group by t."TABLE_SCHEMA", t."TABLE_NAME"`;  
  }    

  async testConnection(connectionProperties,parameters) {   
    try {
      this.setConnectionProperties(connectionProperties);
      this.setTargetDatabase();
      const connection = await sql.connect(this.connectionProperties);
      await sql.close();
	  super.setParameters(parameters)
	} catch (e) {
      await sql.close();
	  throw (e)
	} 
  }

  async configureConnection() {

    const statement = `SET QUOTED_IDENTIFIER ON`
    const results = await this.generateRequest().batch(statement)

  }
  
  setTargetDatabase() {  
    if ((this.parameters.MSSQL_SCHEMA_DB) && (this.parameters.MSSQL_SCHEMA_DB !== this.connectionProperties.database)) {
      this.connectionProperties.database = this.parameters.MSSQL_SCHEMA_DB
    }
  }

  async createConnectionPool() {
    
	this.setTargetDatabase();
    this.logConnectionProperties();

    const sqlStartTime = performance.now();
	this.pool = new sql.ConnectionPool(this.connectionProperties)
    await this.pool.connect();
	this.traceTiming(sqlStartTime,performance.now())

    const yadamuLogger = this.yadamuLogger;
    this.pool.on('error',(err, p) => {
      this.yadamuLogger.logException([`${this.DATABASE_VENDOR}`,`sql.ConnectionPool.onError()`],err);
      throw err
    })
    
	this.transaction = new sql.Transaction(this.pool);
    this.requestProvider = this.pool;
	await this.configureConnection();
  }
    
  async releaseConnection() {
  };
  
  async getDatabaseConnectionImpl() {
    await this.createConnectionPool()
  }
  
  generateRequest() {
    const yadamuLogger = this.yadamuLogger	
	const request = new sql.Request(this.requestProvider)
    request.on('info',function(infoMsg){ 
      yadamuLogger.info([`sql.Request.onInfo()`],`${infoMsg.message}`);
    })
    return request
  }

  
  getPreparedStatement() {
     return new sql.PreparedStatement(this.requestProvider)
  }
  
  setConnectionProperties(connectionProperties) {
	if (Object.getOwnPropertyNames(connectionProperties).length > 0) {	  
      if (!connectionProperties.options) {
        connectionProperties.options = { abortTransactionOnError : false }
      }
      else {
        connectionProperties.options.abortTransactionOnError = false
      }    
	}
    super.setConnectionProperties(connectionProperties)
  }

  async executeBatch(sqlStatment,batchable) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatment}\ngo\n`)
    }  

    const sqlStartTime = performance.now();
    const results = await batchable.batch(sqlStatment);  
	this.traceTiming(sqlStartTime,performance.now())
	return results
  }     

  async execute(executeable,args,traceEntry) {
     
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${traceEntry}\ngo\n`)
    }  

   	const sqlStartTime = performance.now();
    const results = await executeable.execute(args);
	this.traceTiming(sqlStartTime,performance.now())
	return results
  }
 
  async bulkInsert(bulkOperation) {
     
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`--\n-- Bulk Operation: ${bulkOperation.path}. Rows ${bulkOperation.rows.length}.\n--\n`);
    }

   	const sqlStartTime = performance.now();
    const results = await this.generateRequest().bulk(bulkOperation);
	this.traceTiming(sqlStartTime,performance.now())
	return results
  }

  async executeSQL(sqlStatment,queryable) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatment}\ngo\n`)
    }  

    const sqlStartTime = performance.now();
    const results = await queryable.query(sqlStatment);  
	this.traceTiming(sqlStartTime,performance.now())
	return results;
  }     
 
  async verifyDataLoad(request,tableSpec) {    
    const statement = `select ISJSON("${tableSpec.columnName}") "VALID_JSON" from "${tableSpec.tableName}"`;
    const results = await this.executeSQL(statement,this.generateRequest());  
    this.yadamuLogger.log([`${this.constructor.name}.verifyDataLoad()`],`: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${performance.now() - startTime}ms.`);
    return results;
  }
  
  async createSchema(schema) {
    
    if (schema !== 'dbo') {
      const createSchema = `if not exists (select 1 from sys.schemas where name = N'${schema}') exec('create schema "${schema}"')`;
      try {
		const results = await this.executeBatch(createSchema,this.generateRequest())
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.createSchema()`],e)
      }
    }     
  }
  
  async executeDDLImpl(ddl) {
    
    await this.beginTransaction()     

    await this.createSchema(this.parameters.TO_USER);
    // Cannot use Promise.all with mssql Transaction class
    for (let ddlStatement of ddl) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      try {
        const results = await this.executeBatch(ddlStatement,this.generateRequest());
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.executeDDL()`],e)
        this.yadamuLogger.writeDirect(`${ddlStatement}\n`)
      } 
    }

    await this.commitTransaction()      

  }

  decomposeDataType(targetDataType) {
    const dataType = super.decomposeDataType(targetDataType);
    if (dataType.length === -1) {
      dataType.length = sql.MAX;
    }
    return dataType;
  }

  /*
  **
  ** Overridden Methods
  **
  */
   
  get DATABASE_VENDOR()    { return 'MSSQLSERVER' };
  get SOFTWARE_VENDOR()    { return 'Microsoft Corporation' };
  get SPATIAL_FORMAT()     { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().mssql }

  constructor(yadamu) {
    
    super(yadamu,yadamu.getYadamuDefaults().mssql);

    this.pool = undefined;
    this.transaction = undefined;
    this.requestProvider = undefined;
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
    await super.initialize(true);   
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
    this.setSpatialSerializer(this.spatialFormat);
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
    if (this.pool !== undefined) {
	  if (this.preparedStatement !== undefined){
        await this.preparedStatement.unprepare();
      }	  
      this.preparedStatement = undefined;
	}
    await this.pool.close();
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    if (this.pool !== undefined) {
	  if (this.preparedStatement !== undefined){
		try {
          await this.preparedStatement.unprepare();
		} catch (e) {
	      this.yadamuLogger.logException([`${this.constructor.name}.abort()`],e);
        }	  
	    this.preparedStatement = undefined;
      }
      try {
        await this.rollbackTransaction()
      } catch (e) {
	    this.yadamuLogger.logException([`${this.constructor.name}.abort()`],e);
      }	  
      try {
        await this.pool.close();
      } catch (e) {
	    this.yadamuLogger.logException([`${this.constructor.name}.abort()`],e);
      }	  
	}
	console.log('aborted')
  }


  /*
  **
  ** Begin the current transaction
  **
  */
  
  async beginTransaction() {
	  
    // console.log(new Error('BEGIN TRANSACTION').stack)

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`begin transaction\ngo\n`);
    }
	  
    const sqlStartTime = performance.now();
    await this.transaction.begin();
	this.traceTiming(sqlStartTime,performance.now())
    this.requestProvider = this.transaction
  }

  /*
  ** Commit the current transaction
  **
  **
  */
  
  async commitTransaction() {
	  
    // console.log(new Error('COMMIT TRANSACTION').stack)
	
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`commit transaction\ngo\n`);
    }
	  
    const sqlStartTime = performance.now();
    await this.transaction.commit();
	this.traceTiming(sqlStartTime,performance.now())
    this.requestProvider = this.pool
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {

    // console.log(new Error('ROLLBACK TRANSACTION').stack)

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`rollback transaction\ngo\n`);
    }

    const sqlStartTime = performance.now();
    await this.transaction.rollback();
	this.traceTiming(sqlStartTime,performance.now())
    this.requestProvider = this.pool
  }
  
  async createSavePoint() {
    await this.executeSQL(sqlCreateSavePoint,this.generateRequest());
  }
  
  async restoreSavePoint() {
    await this.executeSQL(sqlRestoreSavePoint,this.generateRequest());
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
    // results = await this.verifyDataLoad(this.generateRequest(),STAGING_TABLE);
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  async processFile(hndl) {
     let results = await this.generateRequest().input('TARGET_DATABASE',sql.VarChar,this.parameters.TO_USER).execute('sp_IMPORT_JSON');
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
  
    const results = await this.executeSQL(sqlSystemInformation, await this.generateRequest())
    const sysInfo =  results.recordsets[0][0];
   
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : EXPORT_VERSION
	 ,sessionUser        : sysInfo.SESSION_USER
	 ,currentUser        : sysInfo.CURRENT_USER
     ,dbName             : sysInfo.DATABASE_NAME
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

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`--\n-- @SCHEMA="${this.parameters[schemaKey]}"\n--\n`)
    }
      
    const statement = this.sqlTableInfo()
    const results = await this.executeSQL(statement, this.generateRequest().input('SCHEMA',sql.VarChar,this.parameters[schemaKey]))
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
	   ,collations               : JSON.parse('[' + table.COLLATION_NAMES + ']')
      }
    }
    return metadata;   
  }

  createParser(tableInfo,objectMode) {
    return new DBParser(tableInfo,objectMode,this.yadamuLogger);
  }  
  
  async getInputStream(tableInfo,parser) {

    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
   
    const request = this.generateRequest();
    request.stream = true // You can set streaming differently for each request
    request.on('row', function(row) {readStream.push(row)})
    request.on('done',function(result) {readStream.push(null)});
	this.executeSQL(tableInfo.SQL_STATEMENT,request);
    return readStream;      
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
    
  async generateStatementCache(schema, executeDDL) {
    /* ### OVERRIDE ### Pass additional parameter Database Name */
    const statementGenerator = new StatementGenerator(this, schema, this.metadata, this.systemInformation.spatialFormat, this.batchSize, this.commitSize, this.status, this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL, this.systemInformation.vendor, this.parameters.MSSQL_SCHEMA_DB ? this.parameters.MSSQL_SCHEMA_DB : this.connectionProperties.database)
  }

  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)
  }

  configureSlave(slaveNumber,pool) {
	this.slaveNumber = slaveNumber
	this.pool = pool
	this.transaction = new sql.Transaction(this.pool)
	this.requestProvider = pool
  }

  async newSlaveInterface(slaveNumber) {
	const dbi = new MsSQLDBI(this.yadamu)
	dbi.setParameters(this.parameters);
	// return await super.newSlaveInterface(slaveNumber,dbi,this.pool)
	dbi.configureSlave(slaveNumber,this.pool);
	this.cloneSlaveConfiguration(dbi);
	return dbi
  }

  tableWriterFactory(tableName) {
    this.skipCount = 0;    
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger)
  }
  
}

module.exports = MsSQLDBI
