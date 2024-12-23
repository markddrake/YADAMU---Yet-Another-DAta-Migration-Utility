
import fs                             from 'fs';

import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    
		
import snowflake                      from 'snowflake-sdk';
import snowflakeParameters            from 'snowflake-sdk/lib/parameters.js';
/* Yadamu Core */                                    

import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */  

import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */  

import Comparitor                     from './snowflakeCompare.js'
import DatabaseError                  from './snowflakeException.js'
import DataTypes                      from './snowflakeDataTypes.js'
import Parser                         from './snowflakeParser.js'
import StatementGenerator             from './snowflakeStatementGenerator.js'
import StatementLibrary               from './snowflakeStatementLibrary.js'
import OutputManager                  from './snowflakeOutputManager.js'
import Writer                         from './snowflakeWriter.js'

import SnowflakeConstants             from './snowflakeConstants.js'

class SnowflakeDBI extends YadamuDBI {

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...SnowflakeConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return SnowflakeDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_KEY()           { return SnowflakeConstants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return SnowflakeConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return SnowflakeConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()    { return true }
  get STATEMENT_TERMINATOR()   { return SnowflakeConstants.STATEMENT_TERMINATOR };

  get CURRENT_DATABASE()       { return this.parameters.DATABASE ||this.CONNECTION_PROPERTIES.database }

  // Enable configuration via command line parameters

  // SNOWFLAKE XML TYPE can be set to TEXT to avoid multiple XML Fidelity issues with persisting XML data as VARIANT
  
  get STAGING_PLATFORM()       { return this.parameters.STAGING_PLATFORM || SnowflakeConstants.STAGING_PLATFORM } 

  get SPATIAL_SERIALIZER()     { return this._SPATIAL_SERIALIZER || "ST_AsWKB"; }

  set SPATIAL_SERIALIZER(spatialFormat) {      
    switch (spatialFormat) {
      case "WKB":
        this._SPATIAL_SERIALIZER = "ST_AsWKB";
        break;
        this._SPATIAL_SERIALIZER = "ST_AsEWKB";
        break;
      case "WKT":
        this._SPATIAL_SERIALIZER = "ST_AsWKT";
        break;
      case "EWKT":
        this._SPATIAL_SERIALIZER = "ST_AsEWKT";
        break;
       case "GeoJSON":
         this._SPATIAL_SERIALIZER = "ST_AsGeoJSON"
         break;
     default:
        this._SPATIAL_SERIALIZER = "ST_AsWKB";
    }  
  }    

  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.CLOUD_STAGING }
  
  get ARRAY_BINDING_THRESHOLD() { return this.CONNECTION_PROPERTIES.arrayBindingThreshold }
  
  addVendorExtensions(connectionProperties) {

	// Convert supplied parameters to format expected by connection mechansim
		
    this.parameters.DATABASE = this.parameters.DATABASE || this.parameters.DATABASE
	
    connectionProperties.account                = this.parameters.ACCOUNT                    || connectionProperties.account 
    connectionProperties.username               = this.parameters.USERNAME                   || connectionProperties.username 
    connectionProperties.password               = this.parameters.PASSWORD                   || connectionProperties.password  
    connectionProperties.warehouse              = this.parameters.WAREHOUSE                  || connectionProperties.warehouse                       || this.DBI_PARAMETERS.DEFAULT_WAREHOUSE
    connectionProperties.database               = this.parameters.DATABASE                   || connectionProperties.database                        || this.DBI_PARAMETERS.DEFAULT_DATABASE
	connectionProperties.arrayBindingThreshold  = this.parameters.SNOWFLAKE_BUFFER_SIZE      || connectionProperties.arrayBindingThreshold || 100000      
    connectionProperties.insecureConnect        = this.parameters.SNOWFLAKE_INSECURE_CONNECT || connectionProperties.insecureConnect || false      

	this.parameters.DATABASE = connectionProperties.database
	
	return connectionProperties

  }
    
  constructor(yadamu,manager,connectionSettings,parameters) {	  
  
    super(yadamu,manager,connectionSettings,parameters)
	
    
	this.COMPARITOR_CLASS = Comparitor
	this.DATABASE_ERROR_CLASS = DatabaseError
    this.PARSER_CLASS = Parser
    this.STATEMENT_GENERATOR_CLASS = StatementGenerator
    this.STATEMENT_LIBRARY_CLASS = StatementLibrary
    this.OUTPUT_MANAGER_CLASS = OutputManager
    this.WRITER_CLASS = Writer	
	
	this.DATA_TYPES = DataTypes   
	this.DATA_TYPES.storageOptions.XML_TYPE     = this.parameters.SNOWFLAKE_XML_STORAGE_OPTION      || this.DBI_PARAMETERS.XML_STORAGE_OPTION      || this.DATA_TYPES.storageOptions.XML_TYPE
	this.DATA_TYPES.storageOptions.JSON_TYPE    = this.parameters.SNOWFLAKE_JSON_STORAGE_OPTION     || this.DBI_PARAMETERS.JSON_STORAGE_OPTION     || this.DATA_TYPES.storageOptions.JSON_TYPE
	
  }

  /*
  **
  ** Local methods 
  **
  */
  
  setSchema(schema,key) {
	
	switch (true) {
      case (schema.hasOwnProperty('owner')):
        this.parameters[key] = schema.owner
   	    this.parameters.DATABASE = schema.database 
   	    break
      case (schema.hasOwnProperty('database')):
        this.parameters[key] = schema.schema
   	    this.parameters.DATABASE = schema.database 
   	    break
      default:
   	    this.parameters[key] = schema.schema
	    this.parameters.DATABASE = this.parameters.DATABASE || this.CONNECTION_SETTINGS[this.DATABASE_KEY].database || this.DBI_PARAMETERS.DEFAULT_DATABASE
    }
	this.DESCRIPTION = `"${this.parameters.DATABASE}"."${this.CURRENT_SCHEMA}"`
  }  
  
  getSchema(schema) {
     
	 switch (true) {
	   case (schema.hasOwnProperty('owner')):
	     return {schema : schema.owner, database: schema.database}
		 break
	   case (schema.hasOwnProperty('database')):
	     return schema
	   default:
	     return {schema : schema.schema, database: this.parameters.DATABASE}
	 }

  }  
  
  establishConnection(connection) {
      
    return new Promise((resolve,reject) => {
	  const stack = new Error().stack
      connection.connect((err,connection) => {
        if (err) {
          reject(this.getDatabaseException(err,stack,`snowflake-sdk.Connection.connect()`))
        }
        resolve(connection)
      })
    })
  } 

  async _reconnect() {
    await super._reconnect()
	await this.useDatabase(this.CURRENT_DATABASE)
  }

  async testConnection() {   
    let stack
    this.setDatabase()
	try {
      stack = new Error().stack
	  let connection = snowflake.createConnection(this.CONNECTION_PROPERTIES)
      connection = await this.establishConnection(connection)
      connection.destroy()
	} catch (e) {
	  throw this.createDatabaseError(e,stack,'testConnection.getConnection()')
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
  	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`getConnectionFromPool()`)
    snowflake.configure({ logLevel: "OFF" });
    this.setDatabase()
    this.logConnectionProperties()
    let connection = snowflake.createConnection(this.CONNECTION_PROPERTIES)
    connection = await this.establishConnection(connection)
    const sqlStartTime = performance.now()
    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
    return connection
  }

  async getConnection() {
    // Get a direct connection to the database bypassing the connection pool.
    throw new Error('Unimplemented Method')
  }
  
  async configureConnection() {    

    // Perform connection specific configuration such as setting sesssion time zone to UTC...
	let results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_CONFIGURE_CONNECTION)
    results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SYSTEM_INFORMATION,[])    
    this._DATABASE_VERSION = results[0].DATABASE_VERSION

    if ((this.isManager()) && (this.DATA_TYPES.storageOptions.XML_TYPE !== SnowflakeConstants.SNOWFLAKE_XML_TYPE )) {
       this.LOGGER.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`XMLType storage model is ${this.DATA_TYPES.storageOptions.XML_TYPE}.`)
    }	

  }

  async closeConnection(options) {
	  
  	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection()`)
	  
    if (this.connection !== undefined && this.connection.destroy) {
      await this.connection.destroy()
	}
	
  }
	
  async closePool(options) {
  	// Snowflake-SDK does not support connection pooling
  }
    
  executeSQL(sqlStatement,args) {
    
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    return new Promise((resolve,reject) => {

      this.SQL_TRACE.traceSQL(sqlStatement)

	  const stack = new Error().stack;
      const sqlStartTime = performance.now()
	  const statement = {
        sqlText        : sqlStatement
	  , fetchAsString  : ['Number','Date','JSON']
      , complete       : async (err,statement,rows) => {
		                   const sqlEndTime = performance.now()
                           if (err) {
              		         const cause = this.getDatabaseException(err,stack,sqlStatement)
    		                 if (attemptReconnect && cause.lostConnection()) {
	      				       attemptReconnect = false
			   			       try {
                                 await this.reconnect(cause,'SQL')
                                 const results = await this.executeSQL(sqlStatement,args)
							     resolve(results)
						       } catch (e) {
                                 reject(e)
                               } 							 
                             }
                             else {
                               reject(cause)
                             }
						   }
						   else {
					         // this.SQL_TRACE.traceTiming(sqlStartTime,sqlEndTime)
                             resolve(rows)
						   }
				         }    
      }
	  // console.log(JSON.stringify(statement))
      statement.binds = args
	  this.connection.execute(statement)
    })
  }  

  async _executeDDL(ddl) {
	let results = []
	try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%YADAMU_DATABASE%%/g,this.parameters.DATABASE)
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA)
        return this.executeSQL(ddlStatement,[])
      }))
    } catch (e) { 
	  this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'DDL'],e)
	  results = e
    }
    return results;
  }

  setDatabase() {  
    if ((this.parameters.DATABASE) && (this.parameters.DATABASE !== this.CONNECTION_PROPERTIES.database)) {
      this.CONNECTION_PROPERTIES.database = this.parameters.DATABASE
    }
  }  
  
  async initialize() {
    await super.initialize(true)   
    await this.useDatabase(this.CURRENT_DATABASE)
	this.statementLibrary = new this.STATEMENT_LIBRARY_CLASS(this)
	this.SPATIAL_SERIALIZER = this.SPATIAL_FORMAT
  }
    
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

    // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
	 
	/*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.beginTransaction() is called after the transaction has been started
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
     await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_BEGIN_TRANSACTION,[])
     super.beginTransaction()

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.LOGGER.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.commitTransaction() is called after the transaction has been committed
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/

	 super.commitTransaction()
     await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_COMMIT_TRANSACTION,[])
	
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

   // this.LOGGER.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber()],``)

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
      await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_ROLLBACK_TRANSACTION,[])
    } catch (newIssue) {
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)								   
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
	
  
    
    const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SYSTEM_INFORMATION,[])
    const yadamuInstanceId = await this.executeSQL(`call YADAMU_SYSTEM.PUBLIC.YADAMU_INSTANCE_ID()`,[])
    const yadamuInstallationTimestamp = await this.executeSQL(`call YADAMU_SYSTEM.PUBLIC.YADAMU_INSTALLATION_TIMESTAMP()`,[])
    
    const sysInfo = results[0];

    return Object.assign(
	  super.getSystemInformation()
	, {
        account                     : sysInfo.ACCOUNT
      , warehouse                   : sysInfo.WAREHOUSE
      , dbName                      : sysInfo.DATABASE_NAME
      , databaseVersion             : sysInfo.DATABASE_VERSION
      , client                      : sysInfo.CLIENT
	  , yadamuInstanceID            : yadamuInstanceId[0].YADAMU_INSTANCE_ID
	  , yadamuInstallationTimestamp : yadamuInstallationTimestamp[0].YADAMU_INSTALLATION_TIMESTAMP
      }  
	)
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
    
      const SQL_DESCRIBE_TABLE = `desc table "${this.parameters.DATABASE}"."${tableInfo.TABLE_SCHEMA}"."${tableInfo.TABLE_NAME}"`
	  
      const columnNames = JSON.parse(tableInfo.COLUMN_NAME_ARRAY)
      let dataTypes = JSON.parse(tableInfo.DATA_TYPE_ARRAY)
      const sizeConstraints = JSON.parse(tableInfo.SIZE_CONSTRAINT_ARRAY)
      
      if (dataTypes.includes('USER_DEFINED_TYPE') || dataTypes.includes('BINARY')) {
	    // Perform a describe to get more info about the USER_DEFINED_TYPE and BINARY
	    const descOutput = await this.executeSQL(SQL_DESCRIBE_TABLE)
	    dataTypes.forEach((dataType,idx) => {
          dataTypes[idx] = dataType === 'USER_DEFINED_TYPE' ? descOutput[idx].type : dataType
          sizeConstraints[idx] = dataType === 'BINARY' ? [ + DataTypes.decomposeDataType(descOutput[idx].type).length ] : sizeConstraints[idx] 
        })
	  }
      /*
      **
      ** Sucky DUCK-TYPING of VARIANT columns.. Basically if 1000 random rows contain JSON it's JSON otherwise it's XML !
      **
      */
      if (dataTypes.includes(this.DATA_TYPES.SNOWFLAKE_VARIANT_TYPE)) {
	    // -- Use TRY_PARSE_JSON test a random sample of non null columns to see of they contain valid JSON, if so, assume JSON otherwise assume XML.
	    dataTypes = await Promise.all(dataTypes.map(async (dataType,idx) => {
           if (dataType === this.DATA_TYPES.SNOWFLAKE_VARIANT_TYPE) {
             const columnName = columnNames[idx]
             const SQL_ANALYZE_VARIANT = `with SAMPLE_DATA_SET as (
  select "${columnName}" from "${this.parameters.DATABASE}"."${tableInfo.TABLE_SCHEMA}"."${tableInfo.TABLE_NAME}" 
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

  async getSchemaMetadata() {
	  
	let schemaInfo = await this.executeSQL(this.statementLibrary.SQL_SCHEMA_INFORMATION,[this.CURRENT_SCHEMA])
	schemaInfo = await this.describeVariantColumns(schemaInfo)
	// console.dir(schemaInfo,{depth:null})
    return schemaInfo
  }
  
  generateQueryInformation(tableMetadata) { 
    const tableInfo = super.generateQueryInformation(tableMetadata)
	tableInfo.SQL_STATEMENT = `select ${tableMetadata.CLIENT_SELECT_LIST} from "${this.parameters.DATABASE}"."${tableMetadata.TABLE_SCHEMA}"."${tableMetadata.TABLE_NAME}" t`; 
	return tableInfo
  }     
  
  async _getInputStream(queryInfo) {
    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],queryInfo.TABLE_NAME)
	
	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
		stack = new Error().stack
        const sqlStartTime = performance.now()
        const statement = this.connection.execute({sqlText: queryInfo.SQL_STATEMENT,  fetchAsString: ['Number','Date'], streamResult: true})
    	const is = statement.streamRows();	
	    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return is;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,sqlStatement)
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          continue;
        }
        throw cause
      }      
    } 	
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
      
  async initializeWorker(manager) {
	await super.initializeWorker(manager)
	await this.useDatabase(this.CURRENT_DATABASE)
  }
  
  classFactory(yadamu) {
	return new SnowflakeDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
    const results = await this.executeSQL(`select CURRENT_SESSION() "pid"`)
    const pid = results[0].pid;
    return pid
  }
 
  async generateCopyStatements(metadata,credentials) {
	 this.statementLibrary.STAGE_CREDENTIALS = credentials
	 return super.generateCopyStatements(metadata) 
  }
 
  async reportCopyErrors(tableName,copyState) {
	  
    const err = new Error(`Errors detected durng COPY operation: ${copyState.skipped} records rejected.`)
    err.sql = copyState.sql;
	err.tags = []

	try {
	  const results = await this.executeSQL(`select * from table(validate("${this.parameters.DATABASE}"."${this.CURRENT_SCHEMA}"."${tableName}", job_id => '_last'))`)
      err.cause = results.map((err) => {
	    const loadError = new Error(err.error)
		Object.assign(loadError,err)
		return loadError
	  })
	} catch (e) {
      err.cause = e
	}
	this.LOGGER.handleException([...err.tags,this.DATABASE_VENDOR,tableName],err)	   	
  }  
    
  async initializeCopy() {
    await super.initializeCopy()
    let results = await this.executeSQL(this.statementLibrary.SQL_CREATE_STAGE)
  }
  
  async copyOperation(tableName,copyOperation,copyState) {
	
	try {
	  copyState.startTime = performance.now()
	  let results = await this.beginTransaction()
	  results = await this.executeSQL(`alter session set TIME_INPUT_FORMAT='${SnowflakeConstants.TIME_INPUT_FORMAT[this.systemInformation.vendor]}'`)
	  results = await this.executeSQL(copyOperation.dml)
	  results.forEach((file) => {
	    copyState.read += parseInt(file.rows_parsed)
	    copyState.written += parseInt(file.rows_loaded)
	    copyState.skipped += parseInt(file.errors_seen)
	  })
	  copyState.endTime = performance.now()
	  results = await this.commitTransaction()
	  copyState.committed = copyState.written 
	  copyState.written = 0
  	} catch(e) {
	  copyState.writerError = e
	  try {
  	    this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'COPY',tableName],e)
	    let results = await this.rollbackTransaction()
	  } catch (e) {
		e.cause = copyState.writerError
		copyState.writerError = e
	  }
	}
	return copyState
  }
  
  async finalizeCopy() {
    await super.finalizeCopy()
	const sqlStatement = this.statementLibrary.SQL_DROP_STAGE
    const  results = await this.executeSQL(sqlStatement)
  }

  countBinding(binds) {

    if(!Array.isArray(binds))   {
      return 0;
    }
  
    var count = 0;
    for(var index = 0; index < binds.length; index++) {
      if(binds[index] != null) {
        count += binds[index].length;
      }
    }
    return count;
  }
   
}

export { SnowflakeDBI as default }