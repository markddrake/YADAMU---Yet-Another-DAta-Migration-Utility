
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
  YadamuError,
  YadamuShutdown
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

import Comparitor                    from './postgresCompare.js'
import DatabaseError                 from './postgresException.js'
import DataTypes                     from './postgresDataTypes.js'
import Parser                        from './postgresParser.js'
import StatementGenerator            from './postgresStatementGenerator.js'
import StatementLibrary              from './postgresStatementLibrary.js'
import OutputManager                 from './postgresOutputManager.js'
import Writer                        from './postgresWriter.js'
						          
import PostgresConstants             from './postgresConstants.js'

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
  get MAINTENANCE_WORK_MEM()          { return this.parameters.MAINTENANCE_WORK_MEM || PostgresConstants.MAINTENANCE_WORK_MEM }
							         
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
							          
  get JSON_DATA_TYPE()                { return this.parameters.POSTGRES_JSON_TYPE || DataTypes.storageOptions.JSON_TYPE }
  
  static get DEFAULT_STAGING_PLATFORM() { return DBIConstants.LOADER_STAGING[0]}
  get SUPPORTED_STAGING_PLATFORMS()     { return DBIConstants.LOADER_STAGING }
  get SUPPORTED_STAGING_FORMATS()       { return DBIConstants.CSV_FORMAT }
  
  addVendorExtensions(connectionProperties) {

	// PG Environment variables override command line parameters or configuration file supplied variables

    connectionProperties.username     = process.env.PGUSER             || connectionProperties.user
    connectionProperties.host         = process.env.PGHOST             || connectionProperties.host
    connectionProperties.database     = process.env.PGDATABASE         || connectionProperties.database 
    connectionProperties.password     = process.env.PGPASSWORD         || connectionProperties.password
    connectionProperties.port         = process.env.PGHPORT            || connectionProperties.port 	
    connectionProperties.sslrootcert  = process.env.PGSSLROOTCERT      || this.parameters.SSL_ROOT_CERT  || connectionProperties.sslrootcert 
    
    const opts = process.env.PGOPTIONS || connectionProperties.options || []
    connectionProperties.options = Array.isArray(opts) ? opts.join(' ') : opts
	
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
	
	this.COMPARITOR_CLASS = Comparitor
    this.DATABASE_ERROR_CLASS = DatabaseError
    this.PARSER_CLASS = Parser
    this.STATEMENT_GENERATOR_CLASS = StatementGenerator
    this.STATEMENT_LIBRARY_CLASS = StatementLibrary
    this.OUTPUT_MANAGER_CLASS = OutputManager
    this.WRITER_CLASS = Writer
	
	this.DATA_TYPES = DataTypes
	this.DATA_TYPES.storageOptions.JSON_TYPE    = this.parameters.PGSQL_JSON_STORAGE_OPTION    || this.DBI_PARAMETERS.JSON_STORAGE_OPTION    || this.DATA_TYPES.storageOptions.JSON_TYPE

    this.pgClient = undefined;
    this.useBinaryJSON = false
    
    this.postgresStack = new Error().stack
    this.postgresOperation = undefined
  }

  /*
  **
  ** Local methods 
  **
  */

  async testConnection() {   
    let stack
    try {
	  stack = new Error().stack
      const pgClient = new Client(this.CONNECTION_PROPERTIES)
      await pgClient.connect()
      await pgClient.end()     							  
	} catch (e) {
      throw this.createDatabaseError(e,stack,'testConnection.getConnection()')
	}
	
  }
  
  async createConnectionPool() {
	
    this.logConnectionProperties()
	let sqlStartTime = performance.now()
	this.pool = new Pool(this.CONNECTION_PROPERTIES)
    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	
	this.pool.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  // const pgErr = this.createDatabaseException(err,this.postgresStack,this.postgresOperation)
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
	  throw this.getDatabaseException(e,stack,'pg.Pool.connect()')
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
      throw this.getDatabaseException(e,stack,operation)
	}
  }
  
  async getPostgisInfo() {
  
    let stack
    try {
	  stack = new Error().stack
      const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_POSTGIS_INFO)
	  return results.rows[0][0];
	} catch (sqlError) {
      if ((sqlError instanceof DatabaseError) && sqlError.postgisUnavailable()) {
        // ### What to do about SystemInfo.SPATIAL_FORMAT There can be no Geography or Geometry columns without POSTGIS
        return "Not Installed"
      }
      else {
        throw sqlError
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
	  // const pgErr = this.createDatabaseException(err,this.postgresStack,this.postgresOperation)
      this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE,'ON ERROR','CONNECTION'],err.message)
	})
   
    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_CONFIGURE_CONNECTION)				
	
    const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SYSTEM_INFORMATION)
	this._DATABASE_VERSION = results.rows[0][3];
	
	this.POSTGIS_VERSION = await this.getPostgisInfo()
	
	if (this.isManager()) {
      this.LOGGER.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`PostGIS Version: ${this.POSTGIS_VERSION}.`)
	}

	if (this.EXPORT_SNAPSHOT) {
	  await this.importSnapshot(this.EXPORT_SNAPSHOT)
	}
  }

  /*
  **
  ** Consistent Export (CONSISTENT_EXPORT=true, the default).
  **
  ** As pg_dump does, the manager starts a repeatable read transaction and exports its snapshot, and every other connection used to read the schema imports it,
  ** so the DDL, the metadata and every table are read as of the same moment, even while the database is being updated.
  **
  ** The snapshot can only be imported while the manager's transaction is open, so it remains open until finalizeExport(). If the manager loses its connection
  ** the export fails, rather than continuing with inconsistent reads.
  **
  ** The cost is that the transaction holds back the vacuum horizon for the duration of the export. On a database that is not being updated it gains nothing,
  ** and CONSISTENT_EXPORT=false reads each table as of the moment it is read.
  **
  */

  get EXPORT_SNAPSHOT()               { return this.isManager() ? this._EXPORT_SNAPSHOT : this.manager.EXPORT_SNAPSHOT }
  // Workers generate their own queries, but only the manager loads the schema metadata.
  get INHERITANCE_PARENTS()           { return this.isManager() ? this._INHERITANCE_PARENTS : this.manager.INHERITANCE_PARENTS }
  set INHERITANCE_PARENTS(v)          { this._INHERITANCE_PARENTS = v }

  async initializeExport() {
	await super.initializeExport()
	if (this.CONSISTENT_EXPORT) {
	  await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_BEGIN_CONSISTENT_EXPORT)
	  this.SNAPSHOT_TRANSACTION_ACTIVE = true
	  this._EXPORT_SNAPSHOT = (await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_EXPORT_SNAPSHOT)).rows[0][0]
	  this.LOGGER.info([this.DATABASE_VENDOR,'EXPORT'],`Consistent export: all tables are read from snapshot "${this._EXPORT_SNAPSHOT}".`)
	}
  }

  async importSnapshot(snapshotId) {
	if (!/^[0-9A-F]+-[0-9A-F]+-[0-9]+$/i.test(snapshotId)) {
	  throw new Error(`Invalid snapshot identifier "${snapshotId}".`)
	}
	await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_BEGIN_CONSISTENT_EXPORT)
	this.SNAPSHOT_TRANSACTION_ACTIVE = true
	await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SET_TRANSACTION_SNAPSHOT(snapshotId))
  }

  async endSnapshotTransaction() {
	if (this.SNAPSHOT_TRANSACTION_ACTIVE) {
	  this.SNAPSHOT_TRANSACTION_ACTIVE = false
	  await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_END_CONSISTENT_EXPORT)
	}
  }

  async finalizeExport() {
	this._EXPORT_SNAPSHOT = undefined
	await this.endSnapshotTransaction()
	await super.finalizeExport()
  }
  
  async closeConnection(options) {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection(${this.connection === undefined})`)
	  
    if (this.connection !== undefined && this.connection.release) {
	  let stack
	  try {
	    await this.endSnapshotTransaction()
	  } catch (e) {
	    // The connection is being released. A failed transaction is rolled back by the server.
	  }
      try {
     	stack = new Error().stack;
        await this.connection.release()
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
		const err = this.getDatabaseException(e,stack,'Client.release()')
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
	    throw this.getDatabaseException(e,stack,'pg.Pool.close()')
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

  async truncateTable(schema,tableName) {
	 
	 
	const activeTransaction = this.TRANSACTION_IN_PROGRESS
	if (this.TRANSACTION_IN_PROGRESS) {
	  await this.rollbackTransaction()
	}
	
	const sqlStatement = `${this.SQL_TRUNCATE_TABLE_OPERATION} "${schema}"."${tableName}" cascade`
	
	await this.beginTransaction()
	await this.executeSQL(sqlStatement)
	await this.commitTransaction()
	
	if (activeTransaction) {
	  await this.beginTransaction()
	}
	
  }  
  
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
		const cause = this.getDatabaseException(e,stack,sqlStatement)
		if (cause.connectionTerminated() && this.DESTROYED) throw new YadamuShutdown(this,cause,stack,sqlStatement)
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

  async resetIdentityColumns() {
	const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_GET_IDENTITY_COLUMNS,[this.CURRENT_SCHEMA])
	const updatedIdentityValues = await Promise.all(results.rows.map((row) => {
	  return this.executeSQL(row[0])
	}))
  }

  async finalizeImport() {
	if (this.RESET_IDENTITY) {
	  await this.resetIdentityColumns()
	}
  }
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

     await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_BEGIN_TRANSACTION)
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
    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_COMMIT_TRANSACTION)
	
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
      await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_ROLLBACK_TRANSACTION)
	} catch (newIssue) {
	  this.checkCause('ROLBACK TRANSACTION',cause,newIssue)								   
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
		
    let stack
    try {
      await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_RESTORE_SAVE_POINT)
      super.restoreSavePoint()
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVEPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint(cause) {

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
        inputStream.once('open',() => {resolve(inputStream)}).once('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this,err,stack,importFilePath) : new FileError(this,err,stack,importFilePath) )})
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
      const cause = this.getDatabaseException(e,stack,copyStatement)
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
	  if ((e instanceof DatabaseError) && e.bjsonTooLarge()) {
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

	const typeMappings = await this.getVendorDataTypeMappings()
    
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
  
    const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SYSTEM_INFORMATION)
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
	return await this.generateDDL(this.CURRENT_SCHEMA)
  }

  async generateDDL(schema,...searchPath) {
	// With the search_path set to the schema being exported, the pg_get_*def() functions qualify every object that is not in the schema.
	const currentSearchPath = await this.setSearchPath([schema,...searchPath],false)
	try {
	  const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_GET_DDL_STATEMENTS,[schema])
	  return results.rows.map((row) => { return row[0] })
	} finally {
	  await this.restoreSearchPath(currentSearchPath)
	}
  }

  async setSearchPath(schemas,appendCurrentSearchPath) {
	// Returns the current search_path, so that it can be restored.
	const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_GET_SEARCH_PATH)
	const searchPath = schemas.filter((schema) => { return schema }).map((schema) => { return `"${schema.replaceAll('"','""')}"` })
	if (appendCurrentSearchPath) {
	  searchPath.push(results.rows[0][0])
	}
	await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SET_SEARCH_PATH,[searchPath.join(', ')])
	return results.rows[0][0]
  }

  async restoreSearchPath(searchPath) {
	await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SET_SEARCH_PATH,[searchPath])
  }
  
  async getSchemaMetadata() {
    
	const options = {
	  "circleAsPolygon"    : this.INBOUND_CIRCLE_FORMAT === 'POLYGON'
	, "calculateByteaSize" : true
	, "postgisInstalled"   : this.POSTGIS_INSTALLED
	}
	
    const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SCHEMA_INFORMATION,[this.CURRENT_SCHEMA,this.SPATIAL_FORMAT,options])
	if ((results.rowCount === 1) && Array.isArray(results.rows[0][6])) { // EXPORT_JSON returned Errors
       this.processLog(results.rows[0][6],`EXPORT_JSON('${this.CURRENT_SCHEMA}','${this.SPATIAL_FORMAT}')`)
	}
	// console.dir(results,{depth:null})
	// A query on an inheritance parent also returns its children's rows. The children are exported separately.
	this.INHERITANCE_PARENTS = (await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_INHERITANCE_PARENTS,[this.CURRENT_SCHEMA])).rows.map((row) => { return row[0] })
    return this.generateSchemaInfo(results.rows)
  }

  generateSQLQuery(tableMetadata) {
    const queryInfo = super.generateSQLQuery(tableMetadata)
	if (this.INHERITANCE_PARENTS?.includes(tableMetadata.TABLE_NAME)) {
	  queryInfo.SQL_STATEMENT = queryInfo.SQL_STATEMENT.replace(' from "',' from only "')
	}
	return queryInfo
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
		const cause = this.getDatabaseException(e,stack,queryInfo.SQL_STATEMENT)
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

	/*
	**
	** Statements marked POST_DATA (indexes, foreign keys and triggers) and DATA (sequence values and materialized view contents) are deferred until finalizeData(), so the data is loaded without them.
	** A DDL_ONLY operation applies the POST_DATA statements immediately, and never applies the DATA statements.
	**
	*/

	const schema = `"${this.CURRENT_SCHEMA.replaceAll('"','""')}"`
	const statements = ddl.filter((ddlStatement) => { return ddlStatement }).map((ddlStatement) => { return ddlStatement.replace(/%%SCHEMA%%/g,schema) })

	const isPostData = (ddlStatement) => { return ddlStatement.startsWith(this.STATEMENT_LIBRARY_CLASS.POST_DATA_MARKER) }
	const isData     = (ddlStatement) => { return ddlStatement.startsWith(this.STATEMENT_LIBRARY_CLASS.DATA_MARKER) }

	const immediate = statements.filter((ddlStatement) => { return !isPostData(ddlStatement) && !isData(ddlStatement) })
	const deferred = statements.filter((ddlStatement) => { return isPostData(ddlStatement) || isData(ddlStatement) })

	if (this.MODE === 'DDL_ONLY') {
	  immediate.push(...deferred.filter(isPostData))
	  this.DEFERRED_DDL = []
	}
	else {
	  this.DEFERRED_DDL = deferred
	}

	return await this.applyDDL(immediate)
  }

  async applyDDL(statements) {

    let results = []

    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.DISABLE_FUNCTION_COMPILATION)

	// Unqualified names in replayed DDL refer either to objects in the target schema, or to objects, such as extension types, that remain in the source schema.
	// Generated DDL relies on the connection's search_path, e.g. to find PostGIS types in public.
	const searchPath = await this.setSearchPath([this.CURRENT_SCHEMA,this.systemInformation?.schema],true)

    try {
	  for (const ddlStatement of statements) {
		results.push(await this.executeSQL(ddlStatement))
      }
    } catch (e) {
	  this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'DDL'],e)
	  results = e;
    }

	await this.restoreSearchPath(searchPath)
    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.ENABLE_FUNCTION_COMPILATION)

	return results;
  }

  async finalizeData() {
	if (this.DEFERRED_DDL?.length > 0) {
	  const startTime = performance.now()
	  await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SET_MAINTENANCE_WORK_MEM,[this.MAINTENANCE_WORK_MEM])
	  const results = await this.applyDDL(this.DEFERRED_DDL)
	  await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_RESET_MAINTENANCE_WORK_MEM)
	  this.DEFERRED_DDL = []
	  if (results instanceof Error) {
	    throw results
	  }
	  this.LOGGER.ddl([this.DATABASE_VENDOR],`Executed ${results.length} deferred DDL operations. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`)
	}
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
    return await super.generateStatementCache(schema)
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


}

export { PostgresDBI as default }