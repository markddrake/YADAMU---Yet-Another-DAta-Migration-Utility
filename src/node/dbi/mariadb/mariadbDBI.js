
import fs                             from 'fs';

import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    

import mariadb                        from 'mariadb';
	          
/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                   

import Comparitor                     from './mariadbCompare.js'
import DatabaseError                  from './mariadbException.js'
import DataTypes                      from './mariadbDataTypes.js'
import Parser                         from './mariadbParser.js'
import StatementGenerator             from './mariadbStatementGenerator.js'
import StatementLibrary               from './mariadbStatementLibrary.js'
import OutputManager                  from './mariadbOutputManager.js'
import Writer                         from './mariadbWriter.js'
							          
import MariadbConstants               from './mariadbConstants.js'

class MariadbDBI extends YadamuDBI {
    
  // Until we have static constants

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_GET_CONNECTION_INFORMATION()                 { return _SQL_GET_CONNECTION_INFORMATION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_CREATE_SAVE_POINT()                          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()                         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()                         { return _SQL_RELEASE_SAVE_POINT }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...MariadbConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return MariadbDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_KEY()                 { return MariadbConstants.DATABASE_KEY};
  get DATABASE_VENDOR()              { return MariadbConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()              { return MariadbConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()          { return true }
  get STATEMENT_TERMINATOR()         { return MariadbConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  // Not available until configureConnection() has been called 

  get LOWER_CASE_TABLE_NAMES()       { this._LOWER_CASE_TABLE_NAMES }
  set LOWER_CASE_TABLE_NAMES(v)      { this._LOWER_CASE_TABLE_NAMES = v }
  get IDENTIFIER_TRANSFORMATION()    { return (this._LOWER_CASE_TABLE_NAMES> 0) ? 'LOWERCASE_TABLE_NAMES' : super.IDENTIFIER_TRANSFORMATION }

  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.LOADER_STAGING }

  addVendorExtensions(connectionProperties) {

    Object.assign(connectionProperties,MariadbConstants.CONNECTION_PROPERTY_DEFAULTS)
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
	this.DATA_TYPES.storageOptions.BOOLEAN_TYPE = this.parameters.MARIADB_BOOLEAN_STORAGE_OPTION || this.DBI_PARAMETERS.BOOLEAN_STORAGE_OPTION || this.DATA_TYPES.storageOptions.BOOLEAN_TYPE
	this.DATA_TYPES.storageOptions.SET_TYPE     = this.parameters.MARIADB_SET_STORAGE_OPTION     || this.DBI_PARAMETERS.SET_STORAGE_OPTION     || this.DATA_TYPES.storageOptions.SET_TYPE
	this.DATA_TYPES.storageOptions.ENUM_TYPE    = this.parameters.MARIADB_ENUM_STORAGE_OPTION    || this.DBI_PARAMETERS.ENUM_STORAGE_OPTION    || this.DATA_TYPES.storageOptions.ENUM_TYPE
	this.DATA_TYPES.storageOptions.XML_TYPE     = this.parameters.MARIADB_XML_STORAGE_OPTION     || this.DBI_PARAMETERS.XML_STORAGE_OPTION     || this.DATA_TYPES.storageOptions.XML_TYPE

    this.pool = undefined;
	
  }
  
  async testConnection() {   
    let stack
    try {
      stack = new Error().stack
	  this.connection = await mariadb.createConnection(this.CONNECTION_PROPERTIES)
	  await this.connection.end()
	  this.connection = undefined;
	} catch (e) {
      throw this.createDatabaseError(e,stack,'testConnection.getConnection()')
	} 
  }	
	     
  async configureConnection() {  

    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_CONFIGURE_CONNECTION)

    let results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_GET_CONNECTION_INFORMATION)
    this._DATABASE_VERSION = results[0]

    results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SHOW_SYSTEM_VARIABLES)
	results.forEach((row,i) => { 
	  switch (row[0]) {
		case 'lower_case_table_names':
          this.LOWER_CASE_TABLE_NAMES = parseInt(row[1])
          if (this.isManager() && (this.LOWERCASE_TABLE_NAMES > 0)) {
	        this.LOGGER.info([this.DATABASE_VENDOR,`LOWER_CASE_TABLE_NAMES`],`Table names mapped to lowercase`)
	      }
        break;
	  }
	})
  }

  async checkMaxAllowedPacketSize() {
	  
    const existingConnection = this.connection !== undefined
	  
    if (!existingConnection) {
	  this.connection = await this.getConnectionFromPool()
	}
    
    let results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_GET_MAX_ALLOWED_PACKET)
    if (parseInt(results[0][0]) <  MariadbConstants.MAX_ALLOWED_PACKET) {
		
	  // Need to change the setting.
		
      this.LOGGER.qaInfo([this.DATABASE_VENDOR,this.ROLE],`Increasing MAX_ALLOWED_PACKET to ${MariadbConstants.MAX_ALLOWED_PACKET}.`)
      results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SET_MAX_ALLOWED_PACKET)
	  
	  if (existingConnection) {
		// Need to repalce the existsing connection to pick up the change. 
		this.connection = await this.getConnectionFromPool()
	  }	  
    }    
        
	if (!existingConnection) {    
      await this.closeConnection()
	}

  }
  
  async createConnectionPool() {
    this.logConnectionProperties()
	let sqlStartTime = performance.now()
	this.pool = mariadb.createPool(this.CONNECTION_PROPERTIES)
	this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
    await this.checkMaxAllowedPacketSize()
  }
  
  async getConnectionFromPool() {

	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,,this.getWorkerNumber()],`getConnectionFromPool()`)

    let stack
    this.SQL_TRACE.comment(`Gettting Connection From Pool.`)
	try {
      const sqlStartTime = performance.now()
	  stack = new Error().stack
	  const connection = await this.pool.getConnection()
	  this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	  connection.ping()
      return connection
	} catch (e) {
	  throw this.getDatabaseException(e,stack,'mariadb.Pool.getConnection()')
	}
  }

  async closeConnection(options) {
	  
	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,,this.getWorkerNumber()],`closeConnection(${(this.connection === undefined) ? undefined : this.connection.isValid()})`)
	  
    if ((this.connection !== undefined) && (typeof this.connection.end === 'function')) {
      let stack;
      try {
        stack = new Error().stac
        await this.connection.isValid() ? this.connection.release() : this.connection.destroy()
        // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,,this.getWorkerNumber()],`Connection Closed`)
	    this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
  	    throw this.getDatabaseException(e,stack,'Mariadb.Connection.end()')
	  }
	}
  };
   
  async closePool(options) {
	  
	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,],`closePool(${this.pool !== undefined},${(typeof this.pool.end === 'function')})`)
	  
    if ((this.pool !== undefined) && (typeof this.pool.end === 'function')) {
      let stack;
      try {
        stack = new Error().stack
        await this.pool.end()
        // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,,this.getWorkerNumber()],`Connection Closed`)
		this.pool = undefined;
      } catch (e) {
        this.pool = undefined;
  	    throw this.getDatabaseException(e,stack,'Mariadb.Pool.end()')
	  }
	}
	
  };
   
  async _reconnect() {
	  
	await super._reconnect()
	await this.checkMaxAllowedPacketSize()
	
  }
  async createSchema(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema)
	return results;
    
  }
  
  async executeSQL(sqlStatement,args) {
     
	
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(sqlStatement)
    
	while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
		stack = new Error().stack
		const results = await this.connection.query(sqlStatement,args)
		this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
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
  
  async batch(sqlStatement,data) {
     
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(sqlStatement)
    
	while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
		stack = new Error().stack
		const results = await this.connection.batch(sqlStatement,data)
		this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
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

  async _executeDDL(ddl) {
    await this.createSchema(this.CURRENT_SCHEMA)
    const ddlResults = await Promise.all(ddl.map((ddlStatement) => {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA)
      return this.executeSQL(ddlStatement) 
    }))
	return ddlResults;
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  setSpatialSerializer(spatialFormat) {      
    switch (spatialFormat) {
      case "WKB":
        this.SPATIAL_SERIALIZER = "ST_AsBinary(";
        break;
      case "EWKB":
        this.SPATIAL_SERIALIZER = "ST_AsBinary(";
        break;
      case "WKT":
        this.SPATIAL_SERIALIZER = "ST_AsText(";
        break;
      case "EWKT":
        this.SPATIAL_SERIALIZER = "ST_AsText(";
        break;
       case "GeoJSON":
	     this.SPATIAL_SERIALIZER = "ST_AsGeoJSON("
		 break;
     default:
        this.SPATIAL_SERIALIZER = "ST_AsBinary(";
    }  
  }  
    
  async initialize() {
    await super.initialize(true)
    this.setSpatialSerializer(this.SPATIAL_FORMAT)
	this.statementLibrary = new this.STATEMENT_LIBRARY_CLASS(this)
	
  }
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

    // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(`begin transaction`)
    
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
		stack = new Error().stack
        await this.connection.beginTransaction()
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		super.beginTransaction()
		break
      } catch (e) {
		const cause = this.createDatabaseError(e,stack,'mariadb.Connection.beginTransaction()')
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'BEGIN TRANSACTION')
          continue;
        }
        throw cause
      }      
    } 
	
  }  
 
  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	    
    // this.LOGGER.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    let stack
    this.SQL_TRACE.traceSQL(`commit transaction`)
   
    try {
	  super.commitTransaction()
      const sqlStartTime = performance.now()
      stack = new Error().stack
      await this.connection.commit()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  const cause = this.getDatabaseException(e,stack,'mariadb.Connection.commit()')
	  if (cause.lostConnection()) {
        await this.reconnect(cause,'COMMIT TRANSACTION')
	  }
	  else {
        throw cause
      }      
    } 
	
  }  

  /*
  **
  ** Abort the current transaction
  **
  */
    
  async rollbackTransaction(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber()],``)
	
	this.checkConnectionState(cause)
	
	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.

    let stack
    this.SQL_TRACE.traceSQL(`rollback transaction`)
    try {
	  super.rollbackTransaction()
      const sqlStartTime = performance.now()
      stack = new Error().stack
      await this.connection.rollback()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  const newIssue = this.getDatabaseException(e,stack,'mariadb.Connection.rollback()')
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
    } 
	
  }
  
  async createSavePoint() {

    // this.LOGGER.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_CREATE_SAVE_POINT)
	super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState()

	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
	  await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_RESTORE_SAVE_POINT)
	  super.restoreSavePoint()
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint() {

    // this.LOGGER.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_RELEASE_SAVE_POINT)    
	super.releaseSavePoint()
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
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

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
  
    const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SYSTEM_INFORMATION) 
    const sysInfo = results[0];
	
	return Object.assign(
	  super.getSystemInformation()
	, {
        currentUser                 : sysInfo[1]
      , sessionUser                 : sysInfo[2]
      , dbName                      : sysInfo[0]
      , databaseVersion             : sysInfo[3]
      , serverVendor                : sysInfo[4]
	  , yadamuInstanceID            : sysInfo[8]
	  , yadamuInstallationTimestamp : sysInfo[9]
      , nls_parameters              : {
          serverCharacterSet        : sysInfo[6]
        , databaseCharacterSet      : sysInfo[7]
        }
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
    
  async getSchemaMetadata() {

    // Cannot use JSON_ARRAYAGG for DATA_TYPES and SIZE_CONSTRAINTS beacuse MYSQL implementation of JSON_ARRAYAGG does not support ordering

    const results = await this.executeSQL(this.statementLibrary.SQL_SCHEMA_INFORMATION,[this.CURRENT_SCHEMA])
    const schemaInfo = this.generateSchemaInfo(results) 
    return schemaInfo

  }

  async _getInputStream(queryInfo) {
	  
    if (this.ACTIVE_INPUT_STREAM === true) {
	  this.LOGGER.warning([this.DATABASE_VENDOR,'INPUT STREAM',queryInfo.TABLE_NAME],'Pipeline Aborted. Switching database connection')
	  await this.closeConnection()
	  this.connection = this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
	  await this.configureConnection()
	}
 		
	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
		stack = new Error().stack
        const sqlStartTime = performance.now()
		const is = this.connection.queryStream(queryInfo.SQL_STATEMENT)
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

  classFactory(yadamu) {
	return new MariadbDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select connection_id() "pid"`)
	const pid = results[0][0];
    return pid
  }

  
}

export { MariadbDBI as default }

const _SQL_CONFIGURE_CONNECTION = `SET AUTOCOMMIT = 0, TIME_ZONE = '+00:00',SESSION INTERACTIVE_TIMEOUT = 600000, WAIT_TIMEOUT = 600000, SQL_MODE='ANSI_QUOTES,PAD_CHAR_TO_FULL_LENGTH', GROUP_CONCAT_MAX_LEN = 1024000, GLOBAL LOCAL_INFILE = 'ON'`

const _SQL_GET_CONNECTION_INFORMATION = `select substring(version(),1,instr(version(),'-Maria')-1) "DATABASE_VERSION"`

const _SQL_SYSTEM_INFORMATION   = `select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET"`;                     
 
const _SQL_CREATE_SAVE_POINT    = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT   = `ROLLBACK TO SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT   = `RELEASE SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;