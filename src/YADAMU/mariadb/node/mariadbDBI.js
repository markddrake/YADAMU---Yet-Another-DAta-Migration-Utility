"use strict" 

const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const mariadb = require('mariadb');

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')

const MariadbConstants = require('./mariadbConstants.js')
const MariadbError = require('./mariadbException.js')
const MariadbParser = require('./mariadbParser.js');
const MariadbWriter = require('./mariadbWriter.js');
const StatementGenerator = require('../../dbShared/mysql/57/statementGenerator.js');
const MariadbStatementLibrary = require('./mariadbStatementLibrary.js');

class MariadbDBI extends YadamuDBI {
    
  // Until we have static constants

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_GET_CONNECTION_INFORMATION()                 { return _SQL_GET_CONNECTION_INFORMATION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_CREATE_SAVE_POINT()                          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()                         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()                         { return _SQL_RELEASE_SAVE_POINT }

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,MariadbConstants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return MariadbDBI.YADAMU_DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_KEY()           { return MariadbConstants.DATABASE_KEY};
  get DATABASE_VENDOR()              { return MariadbConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()              { return MariadbConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()          { return true }
  get STATEMENT_TERMINATOR()         { return MariadbConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  get SPATIAL_FORMAT()               { return this.parameters.SPATIAL_FORMAT            || MariadbConstants.SPATIAL_FORMAT }
  get TREAT_TINYINT1_AS_BOOLEAN()    { return this.parameters.TREAT_TINYINT1_AS_BOOLEAN || MariadbConstants.TREAT_TINYINT1_AS_BOOLEAN }

  // Not available until configureConnection() has been called 

  get LOWER_CASE_TABLE_NAMES()       { this._LOWER_CASE_TABLE_NAMES }
  set LOWER_CASE_TABLE_NAMES(v)      { this._LOWER_CASE_TABLE_NAMES = v }
  get IDENTIFIER_TRANSFORMATION()    { return (this._LOWER_CASE_TABLE_NAMES> 0) ? 'LOWERCASE_TABLE_NAMES' : super.IDENTIFIER_TRANSFORMATION }

  constructor(yadamu,settings,parameters) {

    super(yadamu,settings,parameters);
    this.pool = undefined;
	
    this.StatementLibrary = MariadbStatementLibrary
    this.statementLibrary = undefined
  }
  
  async testConnection(connectionProperties,parameters) {   
    try {
	  this.setConnectionProperties(connectionProperties);
      this.connection = await mariadb.createConnection(this.vendorProperties);
	  await this.connection.end();
	  super.setParameters(parameters)
	} catch (e) {
	  throw (e)
	} 
  }	
	     
  async configureConnection() {  

    await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION);

    let results = await this.executeSQL(this.StatementLibrary.SQL_GET_CONNECTION_INFORMATION);
    this._DB_VERSION = results[0]

    results = await this.executeSQL(this.StatementLibrary.SQL_SHOW_SYSTEM_VARIABLES)
	results.forEach((row,i) => { 
	  switch (row[0]) {
		case 'lower_case_table_names':
          this.LOWER_CASE_TABLE_NAMES = parseInt(row[1])
          if (this.isManager() && (this.LOWERCASE_TABLE_NAMES > 0)) {
	        this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`LOWER_CASE_TABLE_NAMES`],`Table names mapped to lowercase`);
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
    
    const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
    const sqlQueryPacketSize = `SELECT @@max_allowed_packet`;
    const sqlSetPacketSize = `SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`
      
    let results = await this.executeSQL(sqlQueryPacketSize);
    if (parseInt(results[0][0]) <  maxAllowedPacketSize) {
		
	  // Need to change the setting.
		
      this.yadamuLogger.info([`${this.DATABASE_VENDOR}`],`Increasing MAX_ALLOWED_PACKET to 1G.`);
      results = await this.executeSQL(sqlSetPacketSize);
	  
	  if (existingConnection) {
		// Need to repalce the existsing connection to pick up the change. 
		this.connection = await this.getConnectionFromPool()
	  }	  
    }    
    
	if (!existingConnection) {    
      await this.closeConnection();
	}

  }
  
  async createConnectionPool() {
    this.logConnectionProperties();
	let sqlStartTime = performance.now();
	this.pool = mariadb.createPool(this.vendorProperties);
	this.traceTiming(sqlStartTime,performance.now())
    await this.checkMaxAllowedPacketSize();
  }
  
  async getConnectionFromPool() {

    let stack
    this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
	try {
      const sqlStartTime = performance.now();
	  stack = new Error().stack
	  const connection = await this.pool.getConnection();
	  this.traceTiming(sqlStartTime,performance.now())
	  connection.ping()
      return connection
	} catch (e) {
	  throw this.trackExceptions(new MariadbError(e,stack,'mariadb.Pool.getConnection()'))
	}
  }

  async closeConnection(options) {
	  
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined && this.connection.end)})`)
	  
    if (this.connection !== undefined && this.connection.end) {
      let stack;
      try {
        stack = new Error().stack
        await this.connection.end();
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
  	    throw this.trackExceptions(new MariadbError(e,stack,'Mariadb.Connection.end()'))
	  }
	}
  };
   
  async closePool(options) {
	  
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool !== undefined && this.pool.end)})`)
	  
    if (this.pool !== undefined && this.pool.end) {
      let stack;
      try {
        stack = new Error().stack
        await this.pool.end();
        this.pool = undefined;
      } catch (e) {
        this.pool = undefined;
  	    throw this.trackExceptions(new MariadbError(e,stack,'Mariadb.Pool.end()'))
	  }
	}
	
  };
   
  async _reconnect() {
	  
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
	await this.checkMaxAllowedPacketSize()
	
  }
  async createSchema(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema);
	return results;
    
  }
  
  async executeSQL(sqlStatement,args) {
     
	
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(sqlStatement));
    
	while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
        const results = await this.connection.query(sqlStatement,args)
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.trackExceptions(new MariadbError(e,stack,sqlStatement))
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
    await this.createSchema(this.parameters.TO_USER);
    const ddlResults = await Promise.all(ddl.map((ddlStatement) => {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      return this.executeSQL(ddlStatement) 
    }))
	return ddlResults;
  }

  setVendorProperties(connectionProperties) {
	super.setVendorProperties(connectionProperties)
    this.vendorProperties = Object.assign(
	  {},
	  MariadbConstants.CONNECTION_PROPERTY_DEFAULTS,
	  this.vendorProperties
    )
  }	 

  updateVendorProperties(vendorProperties) {

    vendorProperties.host              = this.parameters.HOSTNAME || vendorProperties.host
    vendorProperties.user              = this.parameters.USERNAME || vendorProperties.user 
    vendorProperties. password         = this.parameters.PASSWORD || vendorProperties. password
    vendorProperties.database          = this.parameters.DATABASE || vendorProperties.database
    vendorProperties.port              = this.parameters.PORT     || vendorProperties.port
    
	Object.assign(vendorProperties,MariadbConstants.CONNECTION_PROPERTY_DEFAULTS)
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
    await super.initialize(true);
    this.setSpatialSerializer(this.SPATIAL_FORMAT);
	this.statementLibrary = new this.StatementLibrary(this)
  }

  async finalizeRead(tableInfo) {
    this.checkConnectionState(this.latestError) 
    await this.executeSQL(`FLUSH TABLE "${tableInfo.TABLE_SCHEMA}"."${tableInfo.TABLE_NAME}"`)
  }

  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(`begin transaction`));
    
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
        await this.connection.beginTransaction();
        this.traceTiming(sqlStartTime,performance.now())
		super.beginTransaction()
		break
      } catch (e) {
		const cause = MariadbError(e,stack,'mariadb.Connection.beginTransaction()');
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
	    
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    let stack
    this.status.sqlTrace.write(this.traceSQL(`commit transaction`));
   
    try {
	  super.commitTransaction()
      const sqlStartTime = performance.now();
      stack = new Error().stack
      await this.connection.commit();
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  const cause = this.trackExceptions(new MariadbError(e,stack,'mariadb.Connection.commit()'))
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

    // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber()],``)
	
	this.checkConnectionState(cause)
	
	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.

    let stack
    this.status.sqlTrace.write(this.traceSQL(`rollback transaction`));
    try {
	  super.rollbackTransaction()
      const sqlStartTime = performance.now();
      stack = new Error().stack
      await this.connection.rollback();
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  const newIssue = this.trackExceptions(new MariadbError(e,stack,'mariadb.Connection.rollback()'))
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
    } 
	
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
    await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT);
	super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState();

	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
      await this.executeSQL(this.StatementLibrary.SQL_RESTORE_SAVE_POINT);
	  super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(this.StatementLibrary.SQL_RELEASE_SAVE_POINT);    
	super.releaseSavePoint();
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
  
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION); 
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
    
  async getSchemaInfo(keyName) {

    // Cannot use JSON_ARRAYAGG for DATA_TYPES and SIZE_CONSTRAINTS beacuse MYSQL implementation of JSON_ARRAYAGG does not support ordering

    const results = await this.executeSQL(this.statementLibrary.SQL_SCHEMA_INFORMATION,[this.parameters[keyName]]);
    const schemaInfo = this.generateSchemaInfo(results) 
    return schemaInfo

  }

  inputStreamError(err,sqlStatement) {
	 return this.trackExceptions(new MariadbError(err,this.streamingStackTrace,sqlStatement))
  }
    
  async getInputStream(tableInfo) {
    
	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT));
    
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		this.streamingStackTrace = new Error().stack
		const is = this.connection.queryStream(tableInfo.SQL_STATEMENT);
	    this.traceTiming(sqlStartTime,performance.now())
		return is;
      } catch (e) {
		const cause = this.trackExceptions(new MariadbError(e,this.streamingStackTrace,sqlStatement))
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
  
  async generateStatementCache(schema) {
    return await super.generateStatementCache(StatementGenerator,schema) 
  }

  createParser(tableInfo) {
    return new MariadbParser(tableInfo,this.yadamuLogger);
  }  

  getOutputStream(tableName,ddlComplete) {
	 return super.getOutputStream(MariadbWriter,tableName,ddlComplete)
  }

  classFactory(yadamu) {
	return new MariadbDBI(yadamu)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select connection_id() "pid"`)
	const pid = results[0][0];
    return pid
  }
  validStagedDataSet(vendor,controlFilePath,controlFile) {

    /*
	**
	** Return true if, based on te contents of the control file, the data set can be consumed directly by the RDBMS using a COPY operation.
	** Return false if the data set cannot be consumed using a Copy operation
	** Do not throw errors if the data set cannot be used for a COPY operatio
	** Generate Info messages to explain why COPY cannot be used.
	**
	*/

    if (!MariadbConstants.STAGED_DATA_SOURCES.includes(vendor)) {
       return false;
	}
	
	return this.reportCopyOperationMode(controlFile.settings.contentType === 'CSV',controlFilePath,controlFile.settings.contentType)
  }
   
  
  async copyOperation(tableName,copy) {
	
    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
	
	try {
	  const stack = new Error().stack
	  let results = await this.beginTransaction();
	  const startTime = performance.now();
	  results = await this.executeSQL(copy.dml);
	  const rowsRead = results.affectedRows
	  const endTime = performance.now();
	  results = await this.commitTransaction()
	  await this.reportCopyResults(tableName,rowsRead,0,startTime,endTime,copy,stack)
	} catch(e) {
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'COPY',tableName],e)
	  let results = await this.rollbackTransaction()
	}
  }
 
 }

module.exports = MariadbDBI

const _SQL_CONFIGURE_CONNECTION = `SET AUTOCOMMIT = 0, TIME_ZONE = '+00:00',SESSION INTERACTIVE_TIMEOUT = 600000, WAIT_TIMEOUT = 600000, SQL_MODE='ANSI_QUOTES,PAD_CHAR_TO_FULL_LENGTH', GROUP_CONCAT_MAX_LEN = 1024000, GLOBAL LOCAL_INFILE = 'ON'`

const _SQL_GET_CONNECTION_INFORMATION = `select substring(version(),1,instr(version(),'-Maria')-1) "DATABASE_VERSION"`

const _SQL_SYSTEM_INFORMATION   = `select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET"`;                     
 
const _SQL_CREATE_SAVE_POINT    = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT   = `ROLLBACK TO SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT   = `RELEASE SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;