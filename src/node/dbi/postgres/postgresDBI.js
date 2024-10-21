
import fs                             from 'fs';
import path                           from 'path';
import readline                       from 'readline';

import { 
  once 
}                                     from 'events';

import {
  Readable,
  PassThrough
}                                     from 'stream';

import {
  pipeline
}                                     from 'stream/promises';

import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    
							          
import pg                             from 'pg';
const {Client,Pool} = pg;

import QueryStream                    from 'pg-query-stream'
import types                          from 'pg-types';

import pgCopyStreams                  from 'pg-copy-streams'
const CopyFrom = pgCopyStreams.from

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

import {
  YadamuError
}                                    from '../../core/yadamuException.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'
import ExportFileHeader               from '../file/exportFileHeader.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                    from '../file/fileException.js'

/* Vendor Specific DBI Implimentation */                                   
						          
import PostgresConstants             from './postgresConstants.js'
import PostgresDataTypes             from './postgresDataTypes.js'
import PostgresError                 from './postgresException.js'
import PostgresParser                from './postgresParser.js'
import PostgresOutputManager         from './postgresOutputManager.js'
import PostgresWriter                from './postgresWriter.js'
import PostgresStatementGenerator    from './postgresStatementGenerator.js'
import PostgresStatementLibrary      from './postgresStatementLibrary.js'
import PostgresCompare               from './postgresCompare.js'

class PostgresDBI extends YadamuDBI {
    
  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...PostgresConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return PostgresDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  get DATABASE_KEY()                  { return PostgresConstants.DATABASE_KEY};
  get DATABASE_VENDOR()               { return PostgresConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()               { return PostgresConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()           { return true }
  get STATEMENT_TERMINATOR()          { return PostgresConstants.STATEMENT_TERMINATOR };
   
  // Enable configuration via command line parameters
  
  get CIRCLE_FORMAT()                 { return this.parameters.CIRCLE_FORMAT || PostgresConstants.CIRCLE_FORMAT }
  get BYTEA_SIZING_MODEL()            { return this.parameters.BYTEA_SIZING_MODEL || PostgresConstants.BYTEA_SIZING_MODEL }
  get COPY_SERVER_NAME()              { return this.parameters.COPY_SERVER_NAME || PostgresConstants.COPY_SERVER_NAME }
							         
  get POSTGIS_VERSION()               { return this._POSTGIS_VERSION || "Not Installed" }
  set POSTGIS_VERSION(v)              { this._POSTGIS_VERSION = v }
							         
  get POSTGIS_INSTALLED()             { return this.POSTGIS_VERSION !== "Not Installed" }
  // get POSTGIS_INSTALLED()          { return false }

  // Standard Spatial formatting only available when PostGIS is installed.

  // If PostGIS is not available SPATIAL_FORMAT is set to 'GeoJSON' and the following rules apply for Export
 
  //   Geography and Geometry data types are not available to there is no native Storage of EWKT/WKT/EWKB/EWKB/GeoJSON
  //   POINT, LINE_SEGMENT, PATH, BOX, POLYGON are converted from native format to GeoJSON using PG/PLSQL functions
  //   LINE_EQUATION and CIRCLE are are converted from native format to GeoJSON using PG/PLSQL functions
  
  // If PostGIS is available SPATIAL_FORMAT is set based on this.parameters.SPATIAL_FORMAT and this.DATA_TYPES.storageOptions.SPATIAL_FORMAT
 
  //   Geography and Geometry data types are available. Spatial data is converted to SPATIAL_FORMAT using PostGIS functions
  //   POINT, LINE_SEGMENT, PATH, BOX, POLYGON are converted from native format to GeoJSON using PG/PLSQL functions and then to SPATIAL_FORMAT using PostGIS functions
  //   LINE_EQUATION is converted from native format to GeoJSON using PG/PLSQL functions
  //   CIRCLE is converted from native format to GeoJSON using PG/PLSQL functions or to POLYGON and SPATIAL_FORMAT
 
  // If PostGIS is not available the following rules apply for Import

  //   Geography and Geometry types are not available, WKT/EWKT is stored as CLOB, WKB,EWKB is stored as BLOB, GeoJSON is stored as JSON
  //   POINT, LINE_SEGMENT, PATH, BOX and POLYGON will be converted to GeoJSON by the Driver and converted from GeoJSON to 'native' format using PG/PLSQL functions
  //   LINE_EQUATION and CIRCLE are converted from GeoJSON to 'native' format using PG/PLSQL functions
  
  // If PostGIS is available the following rules apply for Import

  //   Geography and Geometry data types are available. Spatial data is converted from SPATIAL_FORMAT using PostGIS functions
  //   POINT, LINE_SEGMENT, PATH, BOX, POLYGON are converted from SPATIAL_FORMAT to native format
  //   LINE_EQUATION is converted from SPATIAL_FORMAT to native format
  //   CIRCLE is converted from native format to GeoJSON using PG/PLSQL functions or to POLYGON and SPATIAL_FORMAT

  // get SPATIAL_FORMAT()                { return this.POSTGIS_INSTALLED === true ? this.parameters.SPATIAL_FORMAT || this.DATA_TYPES.storageOptions.SPATIAL_FORMAT :  "Native" };
  
  get SPATIAL_FORMAT()                { return this.POSTGIS_INSTALLED ? this.parameters.SPATIAL_FORMAT || this.DATA_TYPES.storageOptions.SPATIAL_FORMAT : 'GeoJSON' };
  get INBOUND_CIRCLE_FORMAT()         { return this.systemInformation?.typeMappings?.circleFormat || this.CIRCLE_FORMAT};
							          
  get JSON_DATA_TYPE()                { return this.parameters.POSTGRES_JSON_TYPE || PostgresDataTypes.storageOptions.JSON_TYPE }
  
  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.LOADER_STAGING }

  addVendorExtensions(connectionProperties) {

	// PG Environment variables override command line parameters or configuration file supplied variables

    connectionProperties.username     = process.env.PGUSER             || connectionProperties.user
    connectionProperties.host         = process.env.PGHOST             || connectionProperties.host
    connectionProperties.database     = process.env.PGDATABASE         || connectionProperties.database 
    connectionProperties.password     = process.env.PGPASSWORD         || connectionProperties.password
    connectionProperties.port         = process.env.PGHPORT            || connectionProperties.port 	
    connectionProperties.sslrootcert  = process.env.PGSSLROOTCERT      || this.parameters.SSL_ROOT_CERT  || connectionProperties.sslrootcert 

    if (connectionProperties.sslrootcert) {
	  const sslCert = fs.readFileSync(path.resolve(connectionProperties.sslrootcert),'ascii')
	  connectionProperties.ssl = {
        ca: [sslCert]
	  }
	  if (process.env.PGSSLMODE) {
	    connectionProperties.sslmode = process.env.PGSSLMODE
      }
	}	
	
	if (connectionProperties.sslmode === 'disable') {
	  delete connectionProperties.sslmode
	  delete connectionProperties.sslrootcert
	  delete connectionProperties.ssl
	}
	
	return connectionProperties

  }	 
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
	this.DATA_TYPES = PostgresDataTypes

	this.DATA_TYPES.storageOptions.JSON_TYPE    = this.parameters.PGSQL_JSON_STORAGE_OPTION    || this.DBI_PARAMETERS.JSON_STORAGE_OPTION    || this.DATA_TYPES.storageOptions.JSON_TYPE

    this.pgClient = undefined;
    this.useBinaryJSON = false
    
    this.StatementLibrary = PostgresStatementLibrary
    this.statementLibrary = undefined
	
	this.postgresStack = new Error().stack
    this.postgresOperation = undefined
  }

  /*
  **
  ** Local methods 
  **
  */
  createDatabaseError(driverId,cause,stack,sql) {
    return new PostgresError(driverId,cause,stack,sql)
  }
  
  
  async testConnection() {   
    try {
      const pgClient = new Client(this.CONNECTION_PROPERTIES)
      await pgClient.connect()
      await pgClient.end()     							  
	} catch (e) {
      throw e;
	}
	
  }
  
  async createConnectionPool() {
	
    this.logConnectionProperties()
	let sqlStartTime = performance.now()
	this.pool = new Pool(this.CONNECTION_PROPERTIES)
    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	
	this.pool.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  // const pgErr = this.createDatabaseException(this.DRIVER_ID,err,this.postgresStack,this.postgresOperation)
      this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE,'ON ERROR','POOL'],err.message)
    })

  }
  
  async getConnectionFromPool() {

	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`getConnectionFromPool()`)

	
	let stack
    this.SQL_TRACE.comment(`Getting Connection From Pool.`)

	try {
      const sqlStartTime = performance.now()
	  stack = new Error().stack;
	  const connection = await this.pool.connect()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return connection
	} catch (e) {
	  throw this.getDatabaseException(this.DRIVER_ID,e,stack,'pg.Pool.connect()')
	}
  }

  async getConnection() {
	this.logConnectionProperties()
    const sqlStartTime = performance.now()
	
	let stack 
	let operation
	
	try {
	  operation = 'pg.Client()'
	  stack = new Error().stack;
      this.connection = new Client(this.CONNECTION_PROPERTIES)
	  operation = 'Client.connect()'
	  stack = new Error().stack;
      await this.connection.connect()
	  this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      await this.configureConnection()
      return this.connection
	} catch (e) {
      throw this.getDatabaseException(this.DRIVER_ID,e,stack,operation)
	}
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
      const notice = JSON.parse(JSON.stringify(n))
      switch (notice.code) {
        case '42P07': // Table exists on Create Table if not exists
          break;
        case '00000': // Table not found on Drop Table if exists
	      break;
        default:
          this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE,`NOTICE`],`${n.message ? n.message : JSON.stringify(n)}`)
      }
    })  
  
	this.connection.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  // const pgErr = this.createDatabaseException(this.DRIVER_ID,err,this.postgresStack,this.postgresOperation)
      this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE,'ON ERROR','CONNECTION'],err.message)
	})
   
    await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION)				
	
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION)
	this._DATABASE_VERSION = results.rows[0][3];
	
	this.POSTGIS_VERSION = await this.getPostgisInfo()
	
	if (this.isManager()) {
      this.LOGGER.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`PostGIS Version: ${this.POSTGIS_VERSION}.`)
	}
  }
  
  async closeConnection(options) {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection()`)
	  
    if (this.connection !== undefined && this.connection.release) {
	  let stack
      try {
     	stack = new Error().stack;
        await this.connection.release()
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
		const err = this.getDatabaseException(this.DRIVER_ID,e,stack,'Client.release()')
		this.LOGGER.handleWarning([this.DATABASE_VENDOR,this.DATABASE_VERSION,this.ROLE,this.getWorkerNumber(),`closeConnection`],err)
		throw err
      }
	}
  };
  
  async closePool(options) {

	// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE],`closePool(${(this.pool !== undefined && this.pool.end)})`)

    if (this.pool !== undefined && this.pool.end) {
      let stack
	  try {
	    stack = new Error().stack
	    await this.pool.end()
        this.pool = undefined
  	  } catch (e) {
        this.pool = undefined
	    throw this.getDatabaseException(this.DRIVER_ID,e,stack,'pg.Pool.close()')
	  }
	}
  }
  
  async _reconnect() {
    await super._reconnect()
    const results = await this.executeSQL('select now()')
  }
 
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async executeSQL(sqlStatement,args) {
	
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

	if (typeof sqlStatement === 'string') {
	  let sql = sqlStatement
	  if (sql.indexOf('),($') > 0) {
	    const startElipises = sql.indexOf('),($') + 2 
	    const endElipises =  sql.lastIndexOf('),($') + 2
	    sql = sql.substring(0,startElipises) + '(...),' + sql.substring(endElipises)
	  }
      this.SQL_TRACE.traceSQL(sql)
    }

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
		stack = new Error().stack
        const sqlQuery = typeof sqlStatement === 'string' ? {text : sqlStatement, values: args, rowMode : 'array'} : sqlStatement
        const results = await this.connection.query(sqlQuery)
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.getDatabaseException(this.DRIVER_ID,e,stack,sqlStatement)
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
    await super.initialize(true)
  }
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

     await this.executeSQL(this.StatementLibrary.SQL_BEGIN_TRANSACTION)
	 super.beginTransaction()

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.LOGGER.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

	super.commitTransaction()
    await this.executeSQL(this.StatementLibrary.SQL_COMMIT_TRANSACTION)
	
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

   // this.LOGGER.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber(),YadamuError.lostConnection(cause)],``)

    this.checkConnectionState(cause)

	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.

	try {
      super.rollbackTransaction()
      await this.executeSQL(this.StatementLibrary.SQL_ROLLBACK_TRANSACTION)
	} catch (newIssue) {
	  this.checkCause('ROLBACK TRANSACTION',cause,newIssue)								   
	}
  }

  async createSavePoint() {

    // this.LOGGER.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
															
    await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT)
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)
																 
    this.checkConnectionState(cause)
	 
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
		
    let stack
    try {
      await this.executeSQL(this.StatementLibrary.SQL_RESTORE_SAVE_POINT)
      super.restoreSavePoint()
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVEPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(this.StatementLibrary.SQL_RELEASE_SAVE_POINT)    
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

  async createStagingTable() {
  	let sqlStatement = `drop table if exists "YADAMU_STAGING"`;		
  	await this.executeSQL(sqlStatement)
  	sqlStatement = `create temporary table if not exists "YADAMU_STAGING" (data ${this.useBinaryJSON === true ? 'jsonb' : 'json'}) on commit preserve rows`;					   
  	await this.executeSQL(sqlStatement)
  }
  
  async loadStagingTable(importFilePath) {

	let stack 
    const copyStatement = `copy "YADAMU_STAGING" from STDIN csv quote e'\x01' delimiter e'\x02'`;
    this.SQL_TRACE.traceSQL(copyStatement)
	try {
	  // Create an Readable stream from an async interator based on the readline interface. This avoids issue with uploading Pretty Printed JSON 
   	  const is = await new Promise((resolve,reject) => {
        const stack = new Error().stack
        const inputStream = fs.createReadStream(importFilePath);
        inputStream.once('open',() => {resolve(inputStream)}).once('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(err,stack,importFilePath) : new FileError(err,stack,importFilePath) )})
      })
	  
	  const rl = readline.createInterface({input: is, crlfDelay: Infinity})
      const rli = rl[Symbol.asyncIterator]()
      const rlis = Readable.from(rli)

      const multiplexor = new PassThrough()
	  const exportFileHeader = new ExportFileHeader (multiplexor, importFilePath, this.LOGGER)

	  stack = new Error().stack		
      const outputStream = await this.executeSQL(CopyFrom(copyStatement))    
      const startTime = performance.now()
	  await pipeline(rlis,multiplexor,outputStream)

      this.setSystemInformation(exportFileHeader.SYSTEM_INFORMATION)
	  this.setMetadata(exportFileHeader.METADATA)
	  const ddl = exportFileHeader.DDL
      
      const elapsedTime = performance.now() - startTime
      is.close()
      return elapsedTime;
    }
    catch (e) {
      const cause = this.getDatabaseException(this.DRIVER_ID,e,stack,copyStatement)
	  throw cause
	}
  }
  
  async uploadFile(importFilePath) {

    let elapsedTime;
    try {
      await this.createStagingTable()    
      elapsedTime = await this.loadStagingTable(importFilePath)
    }
    catch (e) {
	  if ((e instanceof PostgresError) && e.bjsonTooLarge()) {
        this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE,`UPLOAD`],`Cannot process file using Binary JSON. Switching to textual JSON.`)
        this.useBinaryJSON = false;
        await this.createStagingTable()
        elapsedTime = await this.loadStagingTable(importFilePath)	
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
    super.processLog(log, operation, this.status, this.LOGGER)
    return log
  }

  async processStagingTable(schema) {  	
  
	const options = {
	  jsonStorageOption    : this.DATA_TYPES.storageOptions.JSON_TYPE
	}
	
  	const sqlStatement = `select ${this.useBinaryJSON ? 'YADAMU.YADAMU_IMPORT_JSONB' : 'YADAMU.YADAMU_IMPORT_JSON'}(data,$1,$2,$3) from "YADAMU_STAGING"`;

	const typeMappings = await this.getVendorDataTypeMappings(PostgresStatementGenerator)
    
  	var results = await this.executeSQL(sqlStatement,[typeMappings,schema,JSON.stringify(options)])
    if (results.rows.length > 0) {
      if (this.useBinaryJSON  === true) {
	    return this.processLog(results.rows[0][0],'JSONB_EACH')  
      }
      else {
	    return this.processLog(results.rows[0][0],'JSON_EACH')  
      }
    }
    else {
      this.LOGGER.error([this.DATABASE_VENDOR,this.ROLE,`UPLOAD`],`Unexpected Error. No response from ${ this.useBinaryJSON === true ? 'CALL YADAMU_IMPORT_JSONB()' : 'CALL_YADAMU_IMPORT_JSON()'}. Please ensure file is valid JSON and NOT pretty printed.`)
      // Return value will be parsed....
      return [];
    }
  }

  async processFile(hndl) {
     return await this.processStagingTable(this.CURRENT_SCHEMA)
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
  
  
  getDriverSettings() {
    return Object.assign(super.getDriverSettings(),{circleFormat : this.DATA_TYPES.storageOptions.CIRCLE_FORMAT})
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
  
  async getSchemaMetadata() {
    
	const options = {
	  "circleAsPolygon"    : this.INBOUND_CIRCLE_FORMAT === 'POLYGON'
	, "calculateByteaSize" : true
	, "postgisInstalled"   : this.POSTGIS_INSTALLED
	}
	
    const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION,[this.CURRENT_SCHEMA,this.SPATIAL_FORMAT,options])
	if ((results.rowCount === 1) && Array.isArray(results.rows[0][6])) { // EXPORT_JSON returned Errors
       this.processLog(results.rows[0][6],`EXPORT_JSON('${this.CURRENT_SCHEMA}','${this.SPATIAL_FORMAT}')`)
	}
	// console.dir(results,{depth:null})
    return this.generateSchemaInfo(results.rows)
  }

  _getParser(queryInfo,pipelineState) {
    return new PostgresParser(this,queryInfo,pipelineState,this.LOGGER)
  }  

  async _getInputStream(queryInfo) {        
  
    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,queryInfo.TABLE_NAME],'')
    
    /*
    **
    **	If the previous pipleline operation failed, it appears that the driver will hang when creating a new QueryStream...
	**
	*/
  
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
		const queryStream = new QueryStream(queryInfo.SQL_STATEMENT,[],{rowMode : "array"})
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
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
		** Note that the streams finished() operator will only consider the stream as 'finished' if finished() function is listening 
		** at the time the emit takes place. 
		**
		*/
		
        const destroyStream = (err) => {
		  try {
		    // console.log('onError',this.connection.constructor.name,inputStream.constructor.name,err.message)
	        inputStream.destroy(err) 
		    inputStream.emit('error',err)
		  } catch (e) {console.log(e) }
		}
		
	    this.connection.on('error',destroyStream)
		
        inputStream.on('end',() => { 
		  this.connection?.removeListener('error',destroyStream)
		}).on('error',() => { 
		  this.connection?.removeListener('error',destroyStream)
		})    		
		
		return inputStream
      } catch (e) {
		const cause = this.getDatabaseException(this.DRIVER_ID,e,stack,queryInfo.SQL_STATEMENT)
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
	await this.executeSQL(createSchema)   
  }
  
  async _executeDDL(ddl) {
	
   let results = []
   //  await this.createSchema(this.CURRENT_SCHEMA)
	
	try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA)
		// this.SQL_TRACE.traceSQL(ddlStatement))
        return this.executeSQL(ddlStatement)
      }))
    } catch (e) {
	 this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'DDL'],e)
	 results = e;
    }
	return results;
  }
   
  async generateStatementCache(schema) {
    if (!this.POSTGIS_INSTALLED) {
      switch (this.INBOUND_SPATIAL_FORMAT) {
        case "WKB":
        case "EWKB":
          this.DATA_TYPES.SPATIAL_TYPE = this.DATA_TYPES.BLOB_TYPE
		  break;
        case "WKT":
	    case "EWKT":
  	      this.DATA_TYPES.SPATIAL_TYPE = this.DATA_TYPES.CLOB_TYPE
		  break;
        case "GeoJSON":
  	      this.DATA_TYPES.SPATIAL_TYPE = this.DATA_TYPES.JSON_TYPE
		  break;
	  }
	  this.DATA_TYPES.GEOGRAPHY_TYPE = this.DATA_TYPES.SPATIAL_TYPE
	  this.DATA_TYPES.GEOMETRY_TYPE = this.DATA_TYPES.SPATIAL_TYPE
	}	  
    return await super.generateStatementCache(PostgresStatementGenerator, schema)
  }

  getOutputManager(tableName,pipelineState) {
	 return super.getOutputManager(PostgresOutputManager,tableName,pipelineState)
  }

  getOutputStream(tableName,pipelineState) {
	 return super.getOutputStream(PostgresWriter,tableName,pipelineState)
  }
 
  classFactory(yadamu) {
	return new PostgresDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select pg_backend_pid()`)
	const pid = results.rows[0][0];
    return pid
  }

  async initializeCopy() {
	 await super.initializeCopy()
	 await this.executeSQL(`create server if not exists "${this.COPY_SERVER_NAME}" FOREIGN DATA WRAPPER file_fdw`)
  }
  
  async copyOperation(tableName,copyOperation,copyState) {
	
	try {
	  copyState.startTime = performance.now()
	  let results = await this.beginTransaction()
	  results = await this.executeSQL(copyOperation.ddl)
	  results = await this.executeSQL(copyOperation.dml)
	  copyState.read = results.rowCount
	  copyState.written = results.rowCount
	  results = await this.executeSQL(copyOperation.drop)
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
	 await this.executeSQL(`drop server "${this.COPY_SERVER_NAME}" `)
  }

  async getComparator(configuration) {
	 await this.initialize()
	 return new PostgresCompare(this,configuration)
  }

}

export { PostgresDBI as default }