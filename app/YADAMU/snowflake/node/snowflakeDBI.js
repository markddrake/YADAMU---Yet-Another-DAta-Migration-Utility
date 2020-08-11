"use strict" 
const fs = require('fs');
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/
const snowflake = require('snowflake-sdk');

const Yadamu = require('../../common/yadamu.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const SnowflakeConstants = require('./snowflakeConstants.js');
const SnowflakeError = require('./snowflakeError.js')
const SnowflakeReader = require('./snowflakeReader.js');
const SnowflakeParser = require('./snowflakeParser.js');
const SnowflakeWriter = require('./snowflakeWriter.js');
const StatementGenerator = require('./statementGenerator.js');

class SnowflakeDBI extends YadamuDBI {

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()                         { return _SQL_SCHEMA_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_BEGIN_TRANSACTION()                          { return _SQL_BEGIN_TRANSACTION }  
  static get SQL_COMMIT_TRANSACTION()                         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()                       { return _SQL_ROLLBACK_TRANSACTION }
 
  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_VENDOR()      { return SnowflakeConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()      { return SnowflakeConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR() { return SnowflakeConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  get SPATIAL_FORMAT()        { return this.parameters.SPATIAL_FORMAT || SnowflakeConstants.SPATIAL_FORMAT };
  
  constructor(yadamu) {	  
    super(yadamu,SnowflakeConstants.DEFAULT_PARAMETERS);
  }

  /*
  **
  ** Local methods 
  **
  */
  
  establishConnection(connection) {
      
    return new Promise((resolve,reject) => {
	  const stack = new Error().stack
      connection.connect((err,connection) => {
        if (err) {
          reject(this.captureException(new SnowflakeError(err,stack,`snowflake-sdk.Connection.connect()`)))
        }
        resolve(connection);
      })
    })
  } 

  async testConnection(connectionProperties) {   
    super.setConnectionProperties(connectionProperties);
	this.setDatabase();
	try {
      let connection = snowflake.createConnection(this.connectionProperties);
      connection = await this.establishConnection(connection);
      connection.destroy()
	  super.setParameters(parameters)
	} catch (e) {
      throw this.captureException(new SnowflakeError(e,'Snowflake-SDK.connection.connect()'))
	}
  }
  
  async createConnectionPool() {	
  	// Snowflake-SDK does not support connection pooling
  }
  
  async useDatabase(database) {
	 await this.executeSQL(`use database "${database}"`)
  }
  
  async getConnectionFromPool() {
  	// Snowflake-SDK does not support connection pooling
    this.setDatabase();
    this.logConnectionProperties();
    let connection = snowflake.createConnection(this.connectionProperties);
    connection = await this.establishConnection(connection);
    const sqlStartTime = performance.now();
    this.traceTiming(sqlStartTime,performance.now())
    return connection
  }

  async getConnection() {
    // Get a direct connection to the database bypassing the connection pool.
    throw new Error('Unimplemented Method')
  }
  
  async configureConnection() {    
    // Perform connection specific configuration such as setting sesssion time zone to UTC...
	//  await this.useDatabase(this.connectionProperties.database);
	const results = await this.executeSQL(SnowflakeDBI.SQL_CONFIGURE_CONNECTION);
  }

  async closeConnection() {
	  
  	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getSlaveNumber()],`closeConnection(${(this.connection !== undefined && this.connection.destroy)})`)
	  
    if (this.connection !== undefined && this.connection.destroy) {
      await this.connection.destroy();
	}
	
  }
	
  async closePool() {
  	// Snowflake-SDK does not support connection pooling
  }
    
  getConnectionProperties() {
	// Convert supplied parameters to format expected by connection mechansim
    return {
      account           : this.parameters.ACCOUNT
    , username          : this.parameters.USERNAME
    , password          : this.parameters.PASSWORD
	, warehouse         : this.parameters.WAREHOUSE
    , database          : this.parameters.DATABASE
    }
     
  }

  executeSQL(sqlStatement,args) {
    
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    return new Promise((resolve,reject) => {

      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceSQL(sqlStatement));
      }

	  const stack = new Error().stack;
      const sqlStartTime = performance.now();
	  this.connection.execute({
        sqlText        : sqlStatement
      , binds          : args
	  , fetchAsString  : ['Number','Date','JSON']
      , complete       : async (err,statement,rows) => {
		                   const sqlEndTime = performance.now()
                           if (err) {
              		         const cause = this.captureException(new SnowflakeError(err,stack,sqlStatement))
    		                 if (attemptReconnect && cause.lostConnection()) {
	      				       attemptReconnect = false
			   			       try {
                                 await this.reconnect(cause,'SQL')
                                 results = await this.executeSQL(sqlStatement,args);
							     resolve(results)
						       } catch (e) {
                                 reject(e);
                               } 							 
                  1          }
                             else {
                               reject(cause);
                             }
                           }
					       // this.traceTiming(sqlStartTime,sqlEndTime)
                           resolve(rows);
				         }    
      })
    })
  }  

  async executeDDLImpl(ddl) {
    const results = await Promise.all(ddl.map(async (ddlStatement) => {
      try {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
        const result = this.executeSQL(ddlStatement,[]);
        return result;
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.executeDDL()`],e)
        this.yadamuLogger.writeDirect(`${ddlStatement}\n`)
      } 
    }))
  }

  setDatabase() {  
    if ((this.parameters.SNOWFLAKE_SCHEMA_DB) && (this.parameters.SNOWFLAKE_SCHEMA_DB !== this.connectionProperties.database)) {
      this.connectionProperties.database = this.parameters.SNOWFLAKE_SCHEMA_DB
    }
  }  
  
  async initialize() {
    await super.initialize(true);   
  }
    
  /*
  **
  **  Gracefully close down the database connection and pool.
  **
  */
  
  async finalize() {
	await super.finalize()
  } 

  /*
  **
  **  Abort the database connection and pool.
  **
  */

  async abort() {
    await super.abort();
  }

  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
	 
	/*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.beginTransaction() is called after the transaction has been started
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
     await this.executeSQL(SnowflakeDBI.SQL_BEGIN_TRANSACTION,[]);
     super.beginTransaction();

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.commitTransaction() is called after the transaction has been committed
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/

	 super.commitTransaction();
     await this.executeSQL(SnowflakeDBI.SQL_COMMIT_TRANSACTION,[]);
	
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

   // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.rollackTransaction() is called after the transaction has been aborted
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/

	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.
	
     
	try {
      super.rollbackTransaction()
      await this.executeSQL(SnowflakeDBI.SQL_ROLLBACK_TRANSACTION,[]);
    } catch (newIssue) {
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue);								   
	}
  }

  async createSavePoint() {
	throw new Error('Unimplemented Method')
  }
  
  async restoreSavePoint(cause) {
	throw new Error('Unimplemented Method')
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
  
  async getSystemInformation() {     
  
    // Get Information about the target server
	
  
    
    const results = await this.executeSQL(SnowflakeDBI.SQL_SYSTEM_INFORMATION,[]);
    
    const sysInfo = results[0];

    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     //,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.OWNER
     ,exportVersion      : Yadamu.EXPORT_VERSION
	 //,sessionUser      : sysInfo.SESSION_USER
	 //,currentUser      : sysInfo.CURRENT_USER
     ,warehouse          : sysInfo.WAREHOUSE
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,client             : sysInfo.CLIENT
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,account            : sysInfo.ACCOUNT
     //,nodeClient         : {}} 
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

  async describeVariantColumns(schemaInfo) {
      
    // Horrible Hack to get information columns which appear as type USER_DEFINED_TYPE in the INFORMATION SCHEMA
    
    const SAMPLE_SIZE  = 1000
    
    
    return await Promise.all(schemaInfo.map(async (tableInfo) => {
    
      const SQL_DESCRIBE_TABLE = `desc table "${tableInfo.TABLE_SCHEMA}"."${tableInfo.TABLE_NAME}"`
	  
      const columnNames = JSON.parse(tableInfo.COLUMN_NAME_ARRAY)
      let dataTypes = JSON.parse(tableInfo.DATA_TYPE_ARRAY)
      const sizeConstraints = JSON.parse(tableInfo.SIZE_CONSTRAINT_ARRAY)
      
      if (dataTypes.includes('USER_DEFINED_TYPE') || dataTypes.includes('BINARY')) {
	    // Perform a describe to get more info about the USER_DEFINED_TYPE and BINARY
	    const descOutput = await this.executeSQL(SQL_DESCRIBE_TABLE);
	    dataTypes.forEach((dataType,idx) => {
          dataTypes[idx] = dataType === 'USER_DEFINED_TYPE' ? descOutput[idx].type : dataType
          sizeConstraints[idx] = dataType === 'BINARY' ? '' + YadamuLibrary.decomposeDataType(descOutput[idx].type).length : sizeConstraints[idx] 
        })
	  }
      /*
      **
      ** Sucky DUCK-TYPING of VARIANT columns.. Basically if 1000 random rows contain JSON it's JSON otherwise it's XML !
      **
      */
      if (dataTypes.includes('VARIANT')) {
	    // -- Use TRY_PARSE_JSON test a random sample of non null columns to see of they contain valid JSON, if so, assume JSON otherwise assume XML.
	    dataTypes = await Promise.all(dataTypes.map(async (dataType,idx) => {
           if (dataType === 'VARIANT') {
             const columnName = columnNames[idx]
             const SQL_ANALYZE_VARIANT = `with SAMPLE_DATA_SET as (
  select "${columnName}" from "${tableInfo.TABLE_SCHEMA}"."${tableInfo.TABLE_NAME}" 
           sample (1000 rows) 
           where "${columnName}" is not null
)
select (select count(*) from SAMPLE_DATA_SET) "SAMPLED_ROWS",
       (select count(*) from SAMPLE_DATA_SET where TRY_PARSE_JSON("${columnName}") is not null) "JSON",
       (select count(*) from SAMPLE_DATA_SET where IS_OBJECT("${columnName}")) "OBJECTS",
       (select count(*) from SAMPLE_DATA_SET where IS_ARRAY("${columnName}")) "ARRAYS"`
             let results = await this.executeSQL(SQL_ANALYZE_VARIANT)
             switch (true) {
               case (results[0].SAMPLED_ROWS === '0'):
                 break;
               case (results[0].SAMPLED_ROWS === results[0].JSON): 
                 dataType = 'JSON'
                 break
               case (results[0].SAMPLED_ROWS === results[0].OBJECTS):
                 // #### Bad Assumption ???? What about other VARIANT types....
                 dataType = 'XML'
                 break
               case (results[0].ARRAY > 0):
                 break;
               default:
             }               
           }
           return dataType
        }))
	  }
      tableInfo.COLUMN_NAME_ARRAY = columnNames
      tableInfo.DATA_TYPE_ARRAY = dataTypes
      tableInfo.SIZE_CONSTRAINT_ARRAY = sizeConstraints
	  return tableInfo
	}))
  }

  async getSchemaInfo(keyName) {

    let schemaInfo = await this.executeSQL(SnowflakeDBI.SQL_SCHEMA_INFORMATION,[this.parameters[keyName]])
    schemaInfo = await this.describeVariantColumns(schemaInfo)
    return schemaInfo
  }
  
  createParser(tableInfo) {
    return new SnowflakeParser(tableInfo,this.yadamuLogger);
  }  
  
  streamingError(e,sqlStatement) {
    return this.captureException(new SnowflakeError(e,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(tableInfo) {

    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],tableInfo.TABLE_NAME)
    this.streamingStackTrace = new Error().stack;
    return new SnowflakeReader(this.connection,tableInfo.SQL_STATEMENT);
	
  }  
  

  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
   
  async createSchema(schema) {
	// Create a schema 
    throw new Error('Unimplemented Method')
  }
   
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL) 
  }
 
  getOutputStream(tableName,ddlComplete) {
	 // Get an instance of the YadamuWriter implementation associated for this database
	 return super.getOutputStream(SnowflakeWriter,tableName,ddlComplete)
  }
 
  async workerDBI(workerNumber) {
	// Create a worker DBI that has it's own connection to the database (eg can begin and end transactions of it's own. 
	// Ideally the connection should come from the same connection pool that provided the connection to this DBI.
	const dbi = new SnowflakeDBI(this.yadamu)
	return await super.workerDBI(workerNumber,dbi)
  }
 
}

module.exports = SnowflakeDBI

const _SQL_CONFIGURE_CONNECTION = `alter session set autocommit=false timezone='UTC' TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' TIME_INPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' GEOGRAPHY_OUTPUT_FORMAT ='WKB'`

const _SQL_SYSTEM_INFORMATION   = `select CURRENT_WAREHOUSE() WAREHOUSE, CURRENT_DATABASE() DATABASE_NAME, CURRENT_SCHEMA() SCHEMA, CURRENT_ACCOUNT() ACCOUNT, CURRENT_VERSION() DATABASE_VERSION, CURRENT_CLIENT() CLIENT`
    
const _SQL_SCHEMA_INFORMATION = 
`select t.table_schema   "TABLE_SCHEMA"
         ,t.table_name   "TABLE_NAME"
         ,concat('[',listagg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position),']') "COLUMN_NAME_ARRAY"
         ,concat('[',listagg(concat('"',data_type,'"'),',') within group (order by ordinal_position),']') "DATA_TYPE_ARRAY"
         ,concat('[',listagg(case
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
                   ) within group (order by ordinal_position)
                 ,']') "SIZE_CONSTRAINT_ARRAY"
         ,listagg(case
                   when c.data_type = 'VARIANT' then
                     concat('TO_VARCHAR("',column_name,'") "',column_name,'"')
                   else
                     concat('"',column_name,'"')
                   end
                  ,','
                 ) within group (order by ordinal_position) "CLIENT_SELECT_LIST"
     from information_schema.columns c, information_schema.tables t
    where t.table_name = c.table_name
      and t.table_schema = c.table_schema
      and t.table_type = 'BASE TABLE'
      and t.table_schema = ?
    group by t.table_schema, t.table_name`;

const _SQL_BEGIN_TRANSACTION    = `begin`;

const _SQL_COMMIT_TRANSACTION   = `commit`;

const _SQL_ROLLBACK_TRANSACTION = `rollback`;

