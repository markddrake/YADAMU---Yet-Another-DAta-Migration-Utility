"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/
const snowflake = require('snowflake-sdk');

const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const SnowflakeError = require('./snowflakeError.js')
const SnowflakeParser = require('./snowflakeParser.js');
const SnowflakeWriter = require('./snowflakeWriter.js');
const StatementGenerator = require('./statementGenerator.js');

const sqlTableInfo = 
`select t.table_schema "TABLE_SCHEMA"
         ,t.table_name   "TABLE_NAME"
         ,listagg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position) "COLUMN_LIST"
         ,listagg(concat('"',data_type,'"'),',') within group (order by ordinal_position) "DATA_TYPES"
         ,listagg(case
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
         ,concat('select ',listagg(case
                                     when c.data_type = 'VARIANT' then
                                       concat('TO_VARCHAR("',column_name,'") "',column_name,'"')
                                     else
                                       concat('"',column_name,'"')
                                   end
                                  ,',') within group (order by ordinal_position)
                          ,' from "',t.table_schema,'"."',t.table_name,'"') "SQL_STATEMENT"
     from information_schema.columns c, information_schema.tables t
    where t.table_name = c.table_name
      and t.table_schema = c.table_schema
      and t.table_type = 'BASE TABLE'
      and t.table_schema = ?
    group by t.table_schema, t.table_name`;

const sqlBeginTransaction = `begin`;

const sqlCommitTransaction = `commit`;

const sqlRollbackTransaction = `rollback`;

class SnowflakeDBI extends YadamuDBI {
    
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
          reject(new SnowflakeError(err,stack,`snowflake-sdk.Connection.connect()`));
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
      throw e;
	}
  }
  
  async createConnectionPool() {	
  	// Snowflake-SDK does not support connection pooling
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
	 const results = await this.executeSQL(`alter session set autocommit = false timezone = 'UTC' TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM'`);
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
    
  /*
  **
  ** Overridden Methods
  **
  */
  
  get DATABASE_VENDOR() { return 'SNOWFLAKE' };
  get SOFTWARE_VENDOR() { return 'Snokwflake Inc' };
  get SPATIAL_FORMAT()  { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().snowflake }

  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().example);
  }

  getConnectionProperties() {
	// Convert supplied parameters to format expected by connection mechansim
    return {
      account           : this.parameters.HOSTNAME
    , username          : this.parameters.USERNAME
    , password          : this.parameters.PASSWORD
    , database          : this.parameters.DATABASE
    }
     
  }

  executeSQL(sqlStatement,args) {
    
    let attemptReconnect = this.attemptReconnection;

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
      , complete       : async function(err,statement,rows) {
		                   const sqlEndTime = performance.now()
                           if (err) {
              		         const cause = new SnowflakeError(err,stack,sqlStatement)
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
    this.spatialFormat = "GeoJSON"
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
	
     await this.executeSQL(sqlBeginTransaction,[]);
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

     await this.executeSQL(sqlCommitTransaction,[]);
	 super.commitTransaction();
	
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
	
    const sqlStatement =  `rollback transaction`
	 
	try {
      await this.executeSQL(sqlRollbackTransaction,[]);
      super.rollbackTransaction()
    } catch (newIssue) {
	  this.checkCause(cause,newIssue);								   
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
  ** The following methods are used by JSON_TABLE() style import operations  
  **
  */

  /*
  **
  **  Upload a JSON File to the server. Optionally return a handle that can be used to process the file
  **
  */
  
  async loadStagingTable(importFilePath) {
	// Process a JSON file that has been uploaded to the server using the database's native JSON capabilities. In most use cases 
	// using client side implementaions are faster, more efficient and can handle much larger files. 
	// The default implementation throws an unsupport feature exception
	super.loadStagingTable()
  }
  
  async uploadFile(importFilePath) {
	// Upload a JSON file to the server so it can be parsed and processed using the database's native JSON capabilities. In most use cases 
	// using client side implementaions are faster, more efficient and can handle much larger files. 
	// The default implementation throws an unsupport feature exception
	super.uploadFile()
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  processLog(log,operation) {
    super.processLog(log, operation, this.status, this.yadamuLogger)
    return log
  }

  async processStagingTable(schema) {  	
  	const sqlStatement = `select ${this.useBinaryJSON ? 'import_jsonb' : 'import_json'}(data,$1) from "YADAMU_STAGING"`;
							   
														   
		 
  	var results = await this.executeSQL(sqlStatement,[schema]);
    if (results.rows.length > 0) {
      if (this.useBinaryJSON  === true) {
	    return this.processLog(results.rows[0].import_jsonb,'JSONB_EACH');  
      }
      else {
	    return this.processLog(results.rows[0].import_json,'JSON_EACH');  
      }
    }
    else {
      this.yadamuLogger.error([`${this.constructor.name}.processStagingTable()`],`Unexpected Error. No response from ${ this.useBinaryJSON === true ? 'CALL IMPORT_JSONB()' : 'CALL_IMPORT_JSON()'}. Please ensure file is valid JSON and NOT pretty printed.`);
      // Return value will be parsed....
      return [];
    }
  }

  async processFile(hndl) {
     return await this.processStagingTable(this.parameters.TO_USER)
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
	
  
    const sqlStatement = 'SELECT CURRENT_WAREHOUSE() WAREHOUSE, CURRENT_DATABASE() DATABASE_NAME, CURRENT_SCHEMA() SCHEMA, CURRENT_ACCOUNT() ACCOUNT, CURRENT_VERSION() DATABASE_VERSION, CURRENT_CLIENT() CLIENT';
    
    const results = await this.executeSQL(sqlStatement,[]);
    
    const sysInfo = results[0];

    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     //,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.OWNER
     ,exportVersion      : this.EXPORT_VERSION
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
   
  generateTableInfo() {
    
    // Psuedo Code shown below..
	
    const tableInfo = Object.keys(this.metadata).map((value) => {
      return {TABLE_NAME : value, SQL_STATEMENT : this.metadata[value].sqlStatemeent}
    })
    return tableInfo;    
    
  }

  async getSchemaInfo(schemaKey) {
    return await this.executeSQL(sqlTableInfo,[this.parameters[schemaKey]])
  }

  createParser(tableInfo,objectMode) {
    return new SnowflakeParser(tableInfo,objectMode,this.yadamuLogger);
  }  
  
  streamingError(e,stack,tableInfo) {
    return new SnowflakeError(e,stack,tableInfo.SQL_STATEMENT)
  }
  
  async getInputStream(tableInfo,parser) {        

    // Get an input stream from a SQL result set.
	
	if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))
    }
        
    const readStream = new Readable({objectMode: true });
    readStream._read = () => {};
    
    const statement = this.connection.execute({sqlText: tableInfo.SQL_STATEMENT,  fetchAsString: ['Number','Date'], streamResult: true})
    const snowflakeStream = statement.streamRows();
    snowflakeStream.on('data',(row) => {readStream.push(row)})
    snowflakeStream.on('end',(result) => {readStream.push(null)});
    return readStream;      
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
 
  getOutputStream(primary) {
	 // Get an instance of the YadamuWriter implementation associated for this database
	 return super.getOutputStream(SnowflakeWriter,primary)
  }
 
  async workerDBI(workerNumber) {
	// Create a worker DBI that has it's own connection to the database (eg can begin and end transactions of it's own. 
	// Ideally the connection should come from the same connection pool that provided the connection to this DBI.
	const dbi = new SnowflakeDBI(this.yadamu)
	return await super.workerDBI(workerNumber,dbi)
  }
 
  async getConnectionID() {
	// Get a uniqueID for the current connection
    throw new Error('Unimplemented Method')
  }
	  
}

module.exports = SnowflakeDBI
