
import fs                             from 'fs';

import {
  finished
}                                     from 'stream/promises';

import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    

import mysql from 'mysql2/promise';	          

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'
import ExportFileHeader               from '../file/exportFileHeader.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                     from '../file/fileException.js'

/* Vendor Specific DBI Implimentation */                                   

import DataTypes                      from './mysqlDataTypes.js'
import DatabaseError                  from './mysqlException.js'
import Comparitor                     from './mysqlCompare.js'
import Parser                         from './mysqlParser.js'
import StatementGenerator             from './mysqlStatementGenerator.js'
import StatementLibrary               from './mysqlStatementLibrary.js'
import OutputManager                  from './mysqlOutputManager.js'
import Writer                         from './mysqlWriter.js'

import StatementLibrary57             from './57/mysqlStatementLibrary.js'
	
import MySQLConstants                 from './mysqlConstants.js'


class MySQLDBI extends YadamuDBI {

  
  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...MySQLConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
  
  // Track MySQL2 double issue select  -1.7976931348623157e+308 returns -1.7976931348623155e+308
  
  #MYSQL2_DOUBLE_ISSUE = false;
  get MYSQL2_DOUBLE_ISSUE()  { return this.#MYSQL2_DOUBLE_ISSUE }
  set MYSQL2_DOUBLE_ISSUE(v) { this.#MYSQL2_DOUBLE_ISSUE = v }
   
  get DBI_PARAMETERS() {
	return MySQLDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_KEY()                 { return MySQLConstants.DATABASE_KEY};
  get DATABASE_VENDOR()              { return MySQLConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()              { return MySQLConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()          { return true }
  get STATEMENT_TERMINATOR()         { return MySQLConstants.STATEMENT_TERMINATOR };
  
  // Enable configuration via command line parameters
  get READ_KEEP_ALIVE()              { return this.parameters.READ_KEEP_ALIVE           || MySQLConstants.READ_KEEP_ALIVE}
  
  // Not available until configureConnection() has been called 

  get LOWER_CASE_TABLE_NAMES()       { this._LOWER_CASE_TABLE_NAMES }
  set LOWER_CASE_TABLE_NAMES(v)      { this._LOWER_CASE_TABLE_NAMES = v }
  get IDENTIFIER_TRANSFORMATION()    { return (this._LOWER_CASE_TABLE_NAMES> 0) ? 'LOWERCASE_TABLE_NAMES' : super.IDENTIFIER_TRANSFORMATION }
  
  static get DEFAULT_STAGING_PLATFORM() { return DBIConstants.LOADER_STAGING[0]}
  get SUPPORTED_STAGING_PLATFORMS()     { return DBIConstants.LOADER_STAGING }
  get SUPPORTED_STAGING_FORMATS()       { return DBIConstants.CSV_FORMAT }
    
  redactPasswords() {

    const infileStreamFactory = this.CONNECTION_PROPERTIES.infileStreamFactory
	delete this.CONNECTION_PROPERTIES.infileStreamFactory
	const connectionProperties = structuredClone(this.CONNECTION_PROPERTIES)
	this.CONNECTION_PROPERTIES.infileStreamFactory = infileStreamFactory
	connectionProperties.infileStreamFactory = infileStreamFactory
	connectionProperties.password = '#REDACTED'
	return connectionProperties
  }


  addVendorExtensions(connectionProperties) {

    Object.assign(connectionProperties,MySQLConstants.CONNECTION_PROPERTY_DEFAULTS)
	return connectionProperties

  }  
  
  constructor(yadamu,manager,connectionSettings,parameters) {

    super(yadamu,manager,connectionSettings,parameters)
	
	if (manager) {
      this.COMPARITOR_CLASS          = manager.COMPARITOR_CLASS         
      this.DATABASE_ERROR_CLASS      = manager.DATABASE_ERROR_CLASS     
      this.PARSER_CLASS              = manager.PARSER_CLASS             
      this.STATEMENT_GENERATOR_CLASS = manager.STATEMENT_GENERATOR_CLASS
      this.STATEMENT_LIBRARY_CLASS   = manager.STATEMENT_LIBRARY_CLASS  
	  this.OUTPUT_MANAGER_CLASS      = manager.OUTPUT_MANAGER_CLASS     
      this.WRITER_CLASS              = manager.WRITER_CLASS             
	} else {
      this.COMPARITOR_CLASS          = Comparitor
      this.DATABASE_ERROR_CLASS      = DatabaseError
      this.PARSER_CLASS              = Parser
      this.STATEMENT_GENERATOR_CLASS = StatementGenerator
      this.STATEMENT_LIBRARY_CLASS   = StatementLibrary	
	  this.OUTPUT_MANAGER_CLASS      = OutputManager
      this.WRITER_CLASS              = Writer	
	}
	
	this.DATA_TYPES = DataTypes
	this.DATA_TYPES.storageOptions.BOOLEAN_TYPE = this.parameters.MYSQL_BOOLEAN_STORAGE_OPTION || this.DBI_PARAMETERS.BOOLEAN_STORAGE_OPTION || this.DATA_TYPES.storageOptions.BOOLEAN_TYPE
	this.DATA_TYPES.storageOptions.SET_TYPE     = this.parameters.MYSQL_SET_STORAGE_OPTION     || this.DBI_PARAMETERS.SET_STORAGE_OPTION     || this.DATA_TYPES.storageOptions.SET_TYPE
	this.DATA_TYPES.storageOptions.ENUM_TYPE    = this.parameters.MYSQL_ENUM_STORAGE_OPTION    || this.DBI_PARAMETERS.ENUM_STORAGE_OPTION    || this.DATA_TYPES.storageOptions.ENUM_TYPE
	this.DATA_TYPES.storageOptions.XML_TYPE     = this.parameters.MYSQL_XML_STORAGE_OPTION     || this.DBI_PARAMETERS.XML_STORAGE_OPTION     || this.DATA_TYPES.storageOptions.XML_TYPE
	
    this.keepAliveInterval = this.parameters.READ_KEEP_ALIVE ? this.parameters.READ_KEEP_ALIVE : 0
    this.keepAliveHdl = undefined
	
  }

  /*
  **
  ** Local methods 
  **
  */
  
  initializeManager() {
	super.initializeManager()
  }	 

  async testConnection() {   
    let stack
    try {
      stack = new Error().stack
	  this.connection = await mysql.createConnection(this.CONNECTION_PROPERTIES)
	  await this.executeSQL('select 1');
	  await this.connection.end()
  	  this.connection = undefined
    } catch (e) {
      throw this.createDatabaseError(e,stack,'testConnection.getConnection()')
    } 
  }
  
  async configureConnection() {

    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_CONFIGURE_CONNECTION)
    let results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_GET_CONNECTION_INFORMATION)
	this._DATABASE_VERSION = results[0].DATABASE_VERSION
    
    results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SHOW_SYSTEM_VARIABLES)
    results.forEach((row) => {
      switch (row[0]) {
        case 'lower_case_table_names':
          this.LOWER_CASE_TABLE_NAMES = row[1]
          if (this.isManager() && (this.LOWERCASE_TABLE_NAMES > 0)) {
			this.LOGGER.info([this.DATABASE_VENDOR,`LOWER_CASE_TABLE_NAMES`],`Table names mapped to lowercase`)
	      }
          break;
       }
    })
  }
  
  async checkMySQL2DoubleIssue() {
	 
     const minVal = -1.7976931348623157e308
	 const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_MYSQL2_DOUBLE_ISSUE,minVal)
	 this.MYSQL2_DOUBLE_ISSUE = parseFloat(results[0].DOUBLE_VALUE) !== minVal
	 // this.LOGGER.qa([this.DATABASE_VENDOR,`DOUBLE_ISSUE`],this.MYSQL2_DOUBLE_ISSUE)
  }
  
 
  async checkMaxAllowedPacketSize() {

    const existingConnection = this.connection !== undefined
	  
    if (!existingConnection) {
	  this.connection = await this.getConnectionFromPool()
	}
    
    let results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_GET_MAX_ALLOWED_PACKET)
	if (parseInt(results[0]['@@MAX_ALLOWED_PACKET']) <  MySQLConstants.MAX_ALLOWED_PACKET) {
		
	  // Need to change the setting.
		
      this.LOGGER.qaInfo([this.DATABASE_VENDOR,this.ROLE],`Increasing MAX_ALLOWED_PACKET to ${MySQLConstants.MAX_ALLOWED_PACKET}.`)
      results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SET_MAX_ALLOWED_PACKET)
	  
	  if (existingConnection) {
		// Need to repalce the existsing connection to pick up the change. 
		this.connection = await this.getConnectionFromPool()
	  }	  
    }    
        
	await this.checkMySQL2DoubleIssue()
	
	if (!existingConnection) {    
      await this.closeConnection()
	}
	
  }
  
  

  async createConnectionPool() {
     
    // MySQL.createPool() is synchronous     
    this.logConnectionProperties()

    let stack, operation
	
    try {
      stack = new Error().stack;
      operation = 'mysql.createPool()'  
      const sqlStartTime = performance.now()
      this.pool = mysql.createPool(this.CONNECTION_PROPERTIES)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      await this.checkMaxAllowedPacketSize()
    } catch (e) {
      throw this.getDatabaseException(e,stack,operation)
    }
    
    
  }
  
  async getConnectionFromPool() {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`getConnectionFromPool()`)

    let stack
    
    this.SQL_TRACE.comment(`Gettting Connection From Pool.`)

    try {    
      stack = new Error().stack;
      const sqlStartTime = performance.now()
      const connection = await this.pool.getConnection()
      return connection
      this.SQL_TRACE.traceTiming(sqlStartTime,perfrmance.now())
	} catch (err) {
	  throw this.getDatabaseException(err,stack,'mysql.Pool.getConnection()')
    }
  }
  
  async closeConnection(options) {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined)},${(typeof this.connection.release === 'function')})`)

    if (this.keepAliveHdl) {
      clearInterval(this.keepAliveHdl)
    }

    if ((this.connection !== undefined) && (typeof this.connection.release === 'function')) {
      let stack;
      try {
        stack = new Error().stack
		if (this.PREVIOUS_PIPELINE_ABORTED) {
          await this.connection.destroy()
	    }
		else {
          await this.connection.release()
		}
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
        throw this.getDatabaseException(e,stack,'MySQL.Connection.end()')
      }
    }
  };
      
  async closePool(options) {
      
	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,],`closePool(${this.pool !== undefined)},${(typeof this.pool.end === 'function')})`)
	      
    if ((this.pool !== undefined) && (typeof this.pool.end === 'function')) {
      let stack;
      try {
        stack = new Error().stack
        await this.pool.end()
        this.pool = undefined;
      } catch (e) {
        this.pool = undefined;
        throw this.getDatabaseException(e,stack,'MySQL.Pool.end()')
      }
    }
    
  };      

  async _reconnect() {
	  
	await super._reconnect()
    await this.connection.ping()
	await this.checkMaxAllowedPacketSize()
	
	/*
	**
	** If the lost connection reesulted from a Servcer Crash, need to reset MAX_ALLOWED_PACKET. 
	**
	*/
	 
	await this.checkMaxAllowedPacketSize()
	
  }

  async executeSQL(sqlStatement,args) {
    
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
	
    let stack
	let results
    this.SQL_TRACE.traceSQL(sqlStatement)

    while (true) {
      // Exit with result or exception.
      try {
        const sqlStartTime = performance.now() 
        stack = new Error().stack;
		const [results,fields] = await this.connection.query(sqlStatement,args)
		this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,sqlStatement,args)
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
  
  async createSchema(schema) {      
  
    const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;                      
    const results = await this.executeSQL(sqlStatement,schema)
    return results;
    
  }
  
  async createStagingTable() {      
    const sqlStatement = `CREATE TEMPORARY TABLE IF NOT EXISTS "YADAMU_STAGING"("DATA" JSON)`;                     
    const results = await this.executeSQL(sqlStatement)
    return results;
  }

  async loadStagingTable(importFilePath,retryUpload) { 
  
    importFilePath = importFilePath.replace(/\\/g, "\\\\")
	
	try {
      const sqlStatement = `LOAD DATA LOCAL INFILE '${importFilePath}' INTO TABLE "YADAMU_STAGING" FIELDS ESCAPED BY ''`;                    
      const results = await this.executeSQL(sqlStatement)
      return results;
    } catch (e) {
	  if (retryUpload && (e instanceof DatabaseError) && e.missingTable()) {
	    // Assume the staging table was lost as a result of a lost connection.
	    await this.createStagingTable() 
		const results = await this.loadStagingTable(importFilePath,false)
		return results
  	  }
	  throw e
	}
  }

  async verifyDataLoad() {      
    const sqlStatement = `SELECT COUNT(*) FROM "YADAMU_STAGING"`;               
    const results = await  this.executeSQL(sqlStatement)
    return results;
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

  setLibraries() {
	this.setSpatialSerializer(this.SPATIAL_FORMAT)
	switch (true) {
	  case (this.DATABASE_VERSION < 8.0):
        this.STATEMENT_LIBRARY_CLASS = StatementLibrary57
	    break;
      default:
	}
	this.setSpatialSerializer(this.SPATIAL_FORMAT)
	this.STATEMENT_LIBRARY = new this.STATEMENT_LIBRARY_CLASS(this)
  }
  
  async initialize() {
    await super.initialize(true)   
  }
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {
      
    // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

    let stack
    this.SQL_TRACE.traceSQL(`begin transaction`)

    try {
      stack = new Error().stack
      await this.connection.beginTransaction()
      super.beginTransaction()
    } catch (e) {
      throw this.getDatabaseException(e,stack,'mysql.Connection.beginTransaction()')
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
      stack = new Error().stack
      await this.connection.commit()
    } catch (e) {
      throw this.getDatabaseException(e,stack,'mysql.Connection.commit()')
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
            
    let stack;       
    this.SQL_TRACE.traceSQL(`rollback transaction`)
    
    try {
      super.rollbackTransaction()
      stack = new Error().stack
      await this.connection.rollback()
    } catch (e) {
      const newIssue = this.getDatabaseException(e,stack,'mysql.Connection.rollback()')
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

    this.checkConnectionState(cause)

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
 
  async uploadFile(importFilePath) {

	const is = await new Promise((resolve,reject) => {
      const stack = new Error().stack
      const inputStream = fs.createReadStream(importFilePath);
      inputStream.once('open',() => {resolve(inputStream)}).once('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this,err,stack,importFilePath) : new FileError(this,err,stack,importFilePath) )})
    })
	
    const exportFileHeader = new ExportFileHeader (is, importFilePath, this.LOGGER)

	try {
	  await finished(exportFileHeader);
	} catch (e) { /* Expected to throw Premature Close */}

    this.setSystemInformation(exportFileHeader.SYSTEM_INFORMATION)
    this.setMetadata(exportFileHeader.METADATA)
    
    let results = await this.createStagingTable()
    results = await this.loadStagingTable(importFilePath,true)
    return results;
  }

  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */
  
  processLog(results,operation) {
    if (results[0].logRecords !== null) {
      const log = JSON.parse(results[0].logRecords)
      super.processLog(log, operation, this.status, this.LOGGER)
      return log
    }
    else {
      return null
    }
  }

  async processFile(hndl) {

    const options = {
      booleanStorgeOption  : this.DATA_TYPES.storageOptions.BOOLEAN_TYPE
	, xmlStorageOption     : this.DATA_TYPES.storageOptions.XML_TYPE
	}
	
	const typeMappings = await this.getVendorDataTypeMappings()
	 
    const sqlStatement = `SET @RESULTS = ''; CALL YADAMU_IMPORT(?,?,?,@RESULTS); SELECT @RESULTS "logRecords";`;                     
    let results = await  this.executeSQL(sqlStatement,[typeMappings,this.CURRENT_SCHEMA, JSON.stringify(options)])
    results = results.pop()
    return this.processLog(results,'JSON_TABLE')
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
  
    const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SYSTEM_INFORMATION) 
    const sysInfo = results[0];

    return Object.assign(
	  super.getSystemInformation()
	, {
        sessionUser                 : sysInfo.SESSION_USER
      , dbName                      : sysInfo.DATABASE_NAME
      , serverHostName              : sysInfo.SERVER_HOST
      , databaseVersion             : sysInfo.DATABASE_VERSION
      , serverVendor                : sysInfo.SERVER_VENDOR_ID
	  , yadamuInstanceID            : sysInfo.YADAMU_INSTANCE_ID
	  , yadamuInstallationTimestamp : sysInfo.YADAMU_INSTALLATION_TIMESTAMP
      , nls_parameters              : {
          serverCharacterSet        : sysInfo.SERVER_CHARACTER_SET
        , databaseCharacterSet      : sysInfo.DATABASE_CHARACTER_SET
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
    return await this.executeSQL(this.STATEMENT_LIBRARY.SQL_SCHEMA_INFORMATION,[this.CURRENT_SCHEMA])

  }

  async _getInputStream(queryInfo) {
	  
    /*
    **
    **	If the previous pipleline operation failed, it appears that the driver will hang when creating a new QueryStream...
	**
	*/
		
    if (this.PREVIOUS_PIPELINE_ABORTED === true) {
	  this.LOGGER.warning([this.DATABASE_VENDOR,'INPUT STREAM',queryInfo.TABLE_NAME],'Pipeline Aborted. Switching database connection')
	  await this.closeConnection()
	  this.connection = this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
	  await this.configureConnection()
	}

    /*
    **
    ** Intermittant Timeout problem with MySQL causes premature abort on Input Stream
    ** If this occures set READ_KEEP_ALIVE to a value >  0 Use a KeepAlive query to prevent Timeouts on the MySQL Connection.
    ** Use a local keepAliveHdl to allow for parallel operaitons
    **
    ** Use setInterval.. 
    ** It appears that the keepAlive Promises do not resolve until input stream has been emptied.
    **
    **
    */

    let keepAliveHdl = undefined
   
    if (this.keepAliveInterval > 0) {
      this.LOGGER.info([`${this.constructor.name}.getInputStream()`],`Stating Keep Alive. Interval ${this.keepAliveInterval}ms.`)
      keepAliveHdl = setInterval(this.keepAlive,this.keepAliveInterval,this)
    }

    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
        stack = new Error().stack
        const sqlStartTime = performance.now()
        const is = this.connection.connection.query({sql: queryInfo.SQL_STATEMENT, rowsAsArray: true}).stream()
        is.on('end',async () => {
		  // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,`${is.constructor.name}.onEnd()`,`${queryInfo.TABLE_NAME}`],``) 
          if (keepAliveHdl !== undefined) {
            clearInterval(keepAliveHdl)
            keepAliveHdl = undefined
          }
        })
        return is;
      } catch (e) {
        const cause = this.getDatabaseException(e,stack,queryInfo.SQL_STATEMENT)
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

  async keepAlive(dbi) {
    // Prevent Connections with Long Running streaming operations from timing out..
    dbi.yadamuLogger.info([`${this.constructor.name}.keepAlive()`],`Row [${dbi.parser.getCounter()}]`)
    try {
      const results = await dbi.executeSQL('select 1')
    } catch (e) {
      // Don't care of timeout query fails
    }
  }

  classFactory(yadamu) {
    return new MySQLDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
    const results = await this.executeSQL(`select connection_id() "pid"`)
    const pid = results[0].pid
    return pid
  }
    
}

export { MySQLDBI as default }