"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

const util = require('util')
const stream = require('stream')
// const pipeline = util.promisify(stream.pipeline);
const { pipeline } = require('stream/promises');

/* 
**
** Require Database Vendors API 
**
*/
const {Client,Pool} = require('pg')
const CopyFrom = require('pg-copy-streams').from;
const QueryStream = require('pg-query-stream')
const types = require('pg').types;

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')

const PostgresConstants = require('./postgresConstants.js')
const PostgresError = require('./postgresException.js')
const PostgresParser = require('./postgresParser.js');
const PostgresWriter = require('./postgresWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const PostgresStatementLibrary = require('./postgresStatementLibrary.js');

const {YadamuError} = require('../../common/yadamuException.js');
const {FileError, FileNotFound, DirectoryNotFound} = require('../../file/node/fileException.js');

class PostgresDBI extends YadamuDBI {
    
  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,PostgresConstants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return PostgresDBI.YADAMU_DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  get DATABASE_KEY()           { return PostgresConstants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return PostgresConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return PostgresConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()    { return true }
  get STATEMENT_TERMINATOR()   { return PostgresConstants.STATEMENT_TERMINATOR };
   
  // Enable configuration via command line parameters
  
  get CIRCLE_FORMAT()          { return this.parameters.CIRCLE_FORMAT || PostgresConstants.CIRCLE_FORMAT }
  get BYTEA_SIZING_MODEL()     { return this.parameters.BYTEA_SIZING_MODEL || PostgresConstants.BYTEA_SIZING_MODEL }
  get COPY_SERVER_NAME()       { return this.parameters.COPY_SERVER_NAME || PostgresConstants.COPY_SERVER_NAME }
   
  get POSTGIS_VERSION()        { return this._POSTGIS_VERSION || "Not Installed" }
  set POSTGIS_VERSION(v)       { this._POSTGIS_VERSION = v }
  
  get POSTGIS_INSTALLED()      { return this.POSTGIS_VERSION !== "Not Installed" }

  // Standard Spatial formatting only available when PostGIS is installed.

  get SPATIAL_FORMAT()         { return this.POSTGIS_INSTALLED === true ? this.parameters.SPATIAL_FORMAT || DBIConstants.SPATIAL_FORMAT :  "Native" };
  get INBOUND_CIRCLE_FORMAT()  { return this.systemInformation?.typeMappings?.circleFormat || this.CIRCLE_FORMAT};

  get JSON_DATA_TYPE()         { return this.parameters.POSTGRES_JSON_TYPE || PostgresConstants.POSTGRES_JSON_TYPE }
  

  constructor(yadamu,settings,parameters) {
    super(yadamu,settings,parameters);
       
    this.pgClient = undefined;
    this.useBinaryJSON = false
    
    /*
    FETCH_AS_STRING.forEach((PGOID) => {
      types.setTypeParser(PGOID, (v) => {return v})
    })
    */
	
    this.StatementLibrary = PostgresStatementLibrary
    this.statementLibrary = undefined
    this.pipelineAborted = false;
   
  }

  /*
  **
  ** Local methods 
  **
  */
  
  async testConnection(connectionProperties) {   
    super.setConnectionProperties(connectionProperties);
	try {
      const pgClient = new Client(this.vendorProperties);
      await pgClient.connect();
      await pgClient.end();     
								  
	} catch (e) {
      throw e;
	}
	
  }
  
  async createConnectionPool() {
	
    this.logConnectionProperties();
	let sqlStartTime = performance.now();
	this.pool = new Pool(this.vendorProperties);
    this.traceTiming(sqlStartTime,performance.now())
	
	this.pool.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  const pgErr = this.trackExceptions(new PostgresError(err,this.postgresStack,this.postgressOperation))
      this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,`POOL_ON_ERROR`],pgErr);
      // throw pgErr
    })

  }
  
  async getConnectionFromPool() {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)

	
	let stack
    this.status.sqlTrace.write(this.traceComment(`Getting Connection From Pool.`));

	try {
      const sqlStartTime = performance.now();
	  stack = new Error().stack;
	  const connection = await this.pool.connect();
      this.traceTiming(sqlStartTime,performance.now())
      return connection
	} catch (e) {
	  throw this.trackExceptions(new PostgresError(e,stack,'pg.Pool.connect()'))
	}
  }

  async getConnection() {
	this.logConnectionProperties();
    const sqlStartTime = performance.now();
	
	let stack 
	let operation
	
	try {
	  operation = 'pg.Client()'
	  stack = new Error().stack;
      const pgClient = new Client(this.vendorProperties);
					
	  operation = 'Client.connect()'
	  stack = new Error().stack;
      this.connection = await pgClient.connect();
    
	  this.traceTiming(sqlStartTime,performance.now())
	} catch (e) {
      throw this.trackExceptions(new PostgresException(e,stack,operation))
	}
    await configureConnection();
  }
  
  async getPostgisInfo() {
  
    try {
      const results = await this.executeSQL(this.StatementLibrary.SQL_POSTGIS_INFO)
	  return results.rows[0][0];
	} catch (e) {
      if ((e instanceof PostgresError) && e.postgisUnavailable()) {
        // ### What to do about SystemInfo.SPATIAL_FORMAT There can be no Geography or Geometry columns without POSTGIS
        return "Not Installed"
      }
      else {
        throw e;
      }
    }
  }

  async configureConnection() {
     
    this.connection.on('notice',(n) => { 
      const notice = JSON.parse(JSON.stringify(n));
      switch (notice.code) {
        case '42P07': // Table exists on Create Table if not exists
          break;
        case '00000': // Table not found on Drop Table if exists
	      break;
        default:
          this.yadamuLogger.info([this.DATABASE_VENDOR,`NOTICE`],`${n.message ? n.message : JSON.stringify(n)}`);
      }
    })  
  
	this.connection.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  const pgErr = this.trackExceptions(new PostgresError(err,this.postgresStack,this.postgressOperation))
      this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,`CONNECTION_ON_ERROR`],pgErr);
      // throw pgErr
    })
   
    await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION);				
	
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION)
	this._DB_VERSION = results.rows[0][3];
	
	this.POSTGIS_VERSION = await this.getPostgisInfo();
	
	if (this.isManager()) {
      this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`${this.DB_VERSION}`,`Configuration`],`PostGIS Version: ${this.POSTGIS_VERSION}.`)
	}
  }
  
  async closeConnection(options) {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection()`)
	  
    if (this.connection !== undefined && this.connection.release) {
	  let stack
      try {
     	stack = new Error().stack;
        await this.connection.release();
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
		const err = this.trackExceptions(new PostgresError(e,stack,'Client.release()'))
		throw err
      }
	}
  };
  
  async closePool(options) {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool !== undefined && this.pool.end)})`)

    if (this.pool !== undefined && this.pool.end) {
      let stack
	  try {
	    stack = new Error().stack
	    await this.pool.end();
        this.pool = undefined
  	  } catch (e) {
        this.pool = undefined
	    throw this.trackExceptions(new PostgresError(e,stack,'pg.Pool.close()'))
	  }
	}
  }
  
  async _reconnect() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
    const results = await this.executeSQL('select now()')
  }
  
  updateVendorProperties(vendorProperties) {

    vendorProperties.user      = this.parameters.USERNAME  || vendorProperties.user
    vendorProperties.host      = this.parameters.HOSTNAME  || vendorProperties.host
    vendorProperties.database  = this.parameters.DATABASE  || vendorProperties.database 
    vendorProperties.password  = this.parameters.PASSWORD  || vendorProperties.password
    vendorProperties.port      = this.parameters.PORT      || vendorProperties.port 
	
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async executeSQL(sqlStatement,args) {
	
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

	if (this.status.sqlTrace  &&(typeof sqlStatement === 'string')){
	  let sql = sqlStatement
	  if (sql.indexOf('),($') > 0) {
	    const startElipises = sql.indexOf('),($') + 2 
	    const endElipises =  sql.lastIndexOf('),($') + 2
	    sql = sql.substring(0,startElipises) + '(...),' + sql.substring(endElipises);
	  }
      this.status.sqlTrace.write(this.traceSQL(sql));
    }

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
        const sqlQuery = typeof sqlStatement === 'string' ? {text : sqlStatement, values: args, rowMode : 'array'} : sqlStatement
        const results = await this.connection.query(sqlQuery)
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.trackExceptions(new PostgresError(e,stack,sqlStatement))
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
  
  async insertBatch(sqlStatement,batch) {
	 
    const result = await this.executeSQL(sqlStatement,batch)
    return result;
  }

  async initialize() {
    await super.initialize(true);
  }
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

     await this.executeSQL(this.StatementLibrary.SQL_BEGIN_TRANSACTION);
	 super.beginTransaction();

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

	super.commitTransaction()
    await this.executeSQL(this.StatementLibrary.SQL_COMMIT_TRANSACTION);
	
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

   // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber(),YadamuError.lostConnection(cause)],``)

    this.checkConnectionState(cause)

	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.

	try {
      super.rollbackTransaction()
      await this.executeSQL(this.StatementLibrary.SQL_ROLLBACK_TRANSACTION);
	} catch (newIssue) {
	  this.checkCause('ROLBACK TRANSACTION',cause,newIssue);								   
	}
  }

  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
															
    await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT);
    super.createSavePoint();
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)
																 
    this.checkConnectionState(cause)
	 
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
		
    let stack
    try {
      await this.executeSQL(this.StatementLibrary.SQL_RESTORE_SAVE_POINT);
      super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVEPOINT',cause,newIssue);
	}
  }  

  async releaseSavePoint(cause) {

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

  async createStagingTable() {
  	let sqlStatement = `drop table if exists "YADAMU_STAGING"`;		
  	await this.executeSQL(sqlStatement);
  	sqlStatement = `create temporary table if not exists "YADAMU_STAGING" (data ${this.useBinaryJSON === true ? 'jsonb' : 'json'}) on commit preserve rows`;					   
  	await this.executeSQL(sqlStatement);
  }
  
  async loadStagingTable(importFilePath) {

    const copyStatement = `copy "YADAMU_STAGING" from STDIN csv quote e'\x01' delimiter e'\x02'`;
    this.status.sqlTrace.write(this.traceSQL(copyStatement))

    const inputStream = await new Promise((resolve,reject) => {
      const inputStream = fs.createReadStream(importFilePath);
      inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(err,stack,importFilePath) : new FileError(err,stack,importFilePath) )})
    })
    const outputStream = await this.executeSQL(CopyFrom(copyStatement));    
    const startTime = performance.now();
    await pipeline(inputStream,outputStream)
    const elapsedTime = performance.now() - startTime
    inputStream.close()
    return elapsedTime;
  }
  
  async uploadFile(importFilePath) {

    this.DESCRIPTION = this.getSchemaIdentifer('TO_USER')

    let elapsedTime;
    try {
      await this.createStagingTable();    
      elapsedTime = await this.loadStagingTable(importFilePath)
    }
    catch (e) {
      if (e.code && (e.code === '54000')) {
        this.yadamuLogger.info([`${this.constructor.name}.uploadFile()`],`Cannot process file using Binary JSON. Switching to textual JSON.`)
        this.useBinaryJSON = false;
        await this.createStagingTable();
        elapsedTime = await this.loadStagingTable(importFilePath);	
      }      
      else {
        throw e
      }
    }
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
  	const sqlStatement = `select ${this.useBinaryJSON ? 'YADAMU_IMPORT_JSONB' : 'YADAMU_IMPORT_JSON'}(data,$1) from "YADAMU_STAGING"`;
  	var results = await this.executeSQL(sqlStatement,[schema]);
    if (results.rows.length > 0) {
      if (this.useBinaryJSON  === true) {
	    return this.processLog(results.rows[0][0],'JSONB_EACH');  
      }
      else {
	    return this.processLog(results.rows[0][0],'JSON_EACH');  
      }
    }
    else {
      this.yadamuLogger.error([`${this.constructor.name}.processStagingTable()`],`Unexpected Error. No response from ${ this.useBinaryJSON === true ? 'CALL YADAMU_IMPORT_JSONB()' : 'CALL_YADAMU_IMPORT_JSON()'}. Please ensure file is valid JSON and NOT pretty printed.`);
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
  
  getTypeMappings() {
   
    const typeMappings = super.getTypeMappings();
	typeMappings.circleFormat = this.CIRCLE_FORMAT 
    return typeMappings; 
  }
  
  async getSystemInformation() {     
  
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION)
    const sysInfo = results.rows[0];
	
    return Object.assign(
	  super.getSystemInformation()
	, {
	    currentUser                 : sysInfo[1]
      , sessionUser                 : sysInfo[2]
	  , dbName                      : sysInfo[0]
      , databaseVersion             : sysInfo[3]
	  , timezone                    : sysInfo[4]
      , postgisInfo                 : this.POSTGIS_VERSION
	  , yadamuInstanceID            : sysInfo[5]
	  , yadamuInstallationTimestamp : sysInfo[6]
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
    
    const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION,[this.parameters[keyName],this.SPATIAL_FORMAT,{"circleAsPolygon": this.INBOUND_CIRCLE_FORMAT === 'POLYGON',"calculateByteaSize":true}]);
	if ((results.rowCount === 1) && Array.isArray(results.rows[0][6])) { // EXPORT_JSON returned Errors
       this.processLog(results.rows[0][6],`EXPORT_JSON('${this.parameters[keyName]}','${this.SPATIAL_FORMAT}')`)
	}
	// console.dir(results,{depth:null})
    return this.generateSchemaInfo(results.rows)
  }

  createParser(tableInfo) {
    return new PostgresParser(tableInfo,this.yadamuLogger);
  }  
  
  inputStreamError(e,sqlStatement) {
    return this.trackExceptions(new PostgresError(e,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(tableInfo) {        
  
    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,tableInfo.TABLE_NAME],'')
    
    /*
    **
    **	If the previous pipleline operation failed, it appears that the postgres driver will hang when creating a new QueryStream...
	**
	*/
  
    if (this.failedPrematureClose) {
	  await this.reconnect(new Error('Previous Pipeline Aborted. Switching database connection'),'INPUT STREAM')
	}
 		
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		this.streamingStackTrace = new Error().stack
        const queryStream = new QueryStream(tableInfo.SQL_STATEMENT,[],{rowMode : "array"})
        this.traceTiming(sqlStartTime,performance.now())
        const inputStream = await this.connection.query(queryStream)   
		
		/*
		**
		** After Upgrading to pg-query-stream 4.0.0 the query-stream does terminate when the connection is lost.
		** This causes the pipeline operation to hang. The following code adds an event listener to the connection that will
		** terminate the query-stream when a connection error occurs, and removes it when the input stream terminates`
		**
		** In theory the listener should call destroy(err) on the input stream, but this does not appear to work. The workaround
		** is to call streams destroy(err) method and then have the stream emit the error...
		**
		*/
		
        const handleConnectionError = (err) => {inputStream.destroy(err); inputStream.emit('error',err)}
	    this.connection.on('error',handleConnectionError)
        inputStream.on('end',() => { this.connection.removeListener('end',handleConnectionError)}).on('error',() => { this.connection.removeListener('error',handleConnectionError)})  
  		
		return inputStream
      } catch (e) {
		const cause = this.trackExceptions(new PostgresError(e,this.streamingStackTrace,tableInfo.SQL_STATEMENT))
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'CREATE INPUT STREAM')
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
    const createSchema = `create schema if not exists "${schema}"`;
	await this.executeSQL(createSchema);   
  }
  
  async _executeDDL(ddl) {
	
	let results = []
    await this.createSchema(this.parameters.TO_USER);
	
	try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
		// this.status.sqlTrace.write(this.traceSQL(ddlStatement));
        return this.executeSQL(ddlStatement);
      }))
    } catch (e) {
	 this.yadamuLogger.handleException([this.DATABASE_VENDOR,'DDL'],e)
	 results = e;
    }
	return results;
  }
   
  async generateStatementCache(schema) {
    return await super.generateStatementCache(StatementGenerator, schema)
  }

  getOutputStream(tableName,ddlComplete) {
	 return super.getOutputStream(PostgresWriter,tableName,ddlComplete)
  }
 
  classFactory(yadamu) {
	return new PostgresDBI(yadamu)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select pg_backend_pid()`)
	const pid = results.rows[0][0];
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

    if (!PostgresConstants.STAGED_DATA_SOURCES.includes(vendor)) {
       return false;
	}
	
	return this.reportCopyOperationMode(controlFile.settings.contentType === 'CSV',controlFilePath,controlFile.settings.contentType)
  }
  
  async initializeCopy() {
	 await this.executeSQL(`create server if not exists "${this.COPY_SERVER_NAME}" FOREIGN DATA WRAPPER file_fdw`)
  }
  
  async copyOperation(tableName,copy) {
	
    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
	
	try {
	  const startTime = performance.now();
	  const stack = new Error().stack
	  let results = await this.beginTransaction();
	  results = await this.executeSQL(copy.ddl);
	  results = await this.executeSQL(copy.dml);
	  const rowsRead = results.rowCount
	  results = await this.executeSQL(copy.drop);
	  const endTime = performance.now()
	  results = await this.commitTransaction()
  	  await this.reportCopyResults(tableName,rowsRead,0,startTime,endTime,copy,stack)
	} catch(e) {
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'COPY',tableName],e)
	  let results = await this.rollbackTransaction()
	}
  }

  async finalizeCopy() {
	 await this.executeSQL(`drop server "${this.COPY_SERVER_NAME}" `);
  }

}

module.exports = PostgresDBI
