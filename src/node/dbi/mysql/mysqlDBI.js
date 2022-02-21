"use strict" 

import fs from 'fs';
import { performance } from 'perf_hooks';

/* 
**
** from  Database Vendors API 
**
*/
import mysql from 'mysql';

import YadamuDBI from '../../common/yadamuDBI.js';
import DBIConstants from '../../common/dbiConstants.js';
import YadamuConstants from '../../common/yadamuConstants.js';
import YadamuLibrary from '../../common/yadamuLibrary.js'
import {CopyOperationAborted} from '../../common/yadamuException.js'

import MySQLConstants from './mysqlConstants.js'
import MySQLError from './mysqlException.js'
import MySQLParser from './mysqlParser.js';
import MySQLWriter from './mysqlWriter.js';
import MySQLOutputManager from './mysqlOutputManager.js';
import StatementGenerator from './statementGenerator.js';
import MySQLStatementLibrary from './mysqlStatementLibrary.js';

class MySQLDBI extends YadamuDBI {
   
  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,MySQLConstants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return MySQLDBI.YADAMU_DBI_PARAMETERS
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
  get SPATIAL_FORMAT()               { return this.parameters.SPATIAL_FORMAT            || MySQLConstants.SPATIAL_FORMAT }
  get READ_KEEP_ALIVE()              { return this.parameters.READ_KEEP_ALIVE           || MySQLConstants.READ_KEEP_ALIVE}
  get TREAT_TINYINT1_AS_BOOLEAN()    { return this.parameters.TREAT_TINYINT1_AS_BOOLEAN || MySQLConstants.TREAT_TINYINT1_AS_BOOLEAN }

  // Not available until configureConnection() has been called 

  get LOWER_CASE_TABLE_NAMES()       { this._LOWER_CASE_TABLE_NAMES }
  set LOWER_CASE_TABLE_NAMES(v)      { this._LOWER_CASE_TABLE_NAMES = v }
  get IDENTIFIER_TRANSFORMATION()    { return (this._LOWER_CASE_TABLE_NAMES> 0) ? 'LOWERCASE_TABLE_NAMES' : super.IDENTIFIER_TRANSFORMATION }
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
    this.keepAliveInterval = this.parameters.READ_KEEP_ALIVE ? this.parameters.READ_KEEP_ALIVE : 0
    this.keepAliveHdl = undefined
	
	this.StatementGenerator = StatementGenerator
    this.StatementLibrary = MySQLStatementLibrary
    this.statementLibrary = undefined
	
	this.activeInputStream = false;

  }

  /*
  **
  ** Local methods 
  **
  */
   
  async testConnection(connectionProperties,parameters) {   
    try {
      this.setConnectionProperties(connectionProperties);
      this.connection = this.getConnectionFromPool();
      await this.openDatabaseConnection();
      await this.connection.end();
      super.setParameters(parameters)
    } catch (e) {
      throw (e)
    } 
  }
  
  async configureConnection() {

    await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION);
    let results = await this.executeSQL(this.StatementLibrary.SQL_GET_CONNECTION_INFORMATION);
	this._DB_VERSION = results[0].DATABASE_VERSION
    
    results = await this.executeSQL(this.StatementLibrary.SQL_SHOW_SYSTEM_VARIABLES);
    results.forEach((row) => {
      switch (row.Variable_name) {
        case 'lower_case_table_names':
          this.LOWER_CASE_TABLE_NAMES = row.Value
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
    
    if (parseInt(results[0]['@@max_allowed_packet']) <  maxAllowedPacketSize) {
		
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
     
    // MySQL.createPool() is synchronous     
      
    this.logConnectionProperties();
    
    let stack, operation
    try {
      stack = new Error().stack;
      operation = 'mysql.createPool()'  
      const sqlStartTime = performance.now();
      this.pool = new mysql.createPool(this.vendorProperties);
      this.traceTiming(sqlStartTime,performance.now())
      await this.checkMaxAllowedPacketSize()
    } catch (e) {
      throw this.trackExceptions(new MySQLError(this.DRIVER_ID,e,stack,operation))
    }
    
    
  }
  
  async getConnectionFromPool() {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)
    
    this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
    
    const stack = new Error().stack;
    const connection = await new Promise((resolve,reject) => {
      const sqlStartTime = performance.now();
      this.pool.getConnection((err,connection) => {
        this.traceTiming(sqlStartTime,performance.now())
        if (err) {
          reject(this.trackExceptions(new MySQLError(this.DRIVER_ID,err,stack,'mysql.Pool.getConnection()')))
        }
        resolve(connection);
      })
    })
    return connection
  }
  
  async closeConnection(options) {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${((this.connection !== undefined) && (typeof this.connection.release === 'function'))})`)

    if (this.keepAliveHdl) {
      clearInterval(this.keepAliveHdl)
    }

    if ((this.connection !== undefined) && (typeof this.connection.release === 'function')) {
      let stack;
      try {
        stack = new Error().stack
		if (this.activeInputStream) {
          await this.connection.destroy();
	    }
		else {
          await this.connection.release();
		}
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
        throw this.trackExceptions(new MySQLError(this.DRIVER_ID,e,stack,'MySQL.Connection.end()'))
      }
    }
  };
      
  async closePool(options) {
      
    // this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool !== undefined && this.pool.end)})`)
      
    if ((this.pool !== undefined) && (typeof this.pool.end === 'function')) {
      let stack;
      try {
        stack = new Error().stack
        await this.pool.end();
        this.pool = undefined;
      } catch (e) {
        this.pool = undefined;
        throw this.trackExceptions(new MySQLError(this.DRIVER_ID,e,stack,'MySQL.Pool.end()'))
      }
    }
    
  };      

  async _reconnect() {
	  
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
    await this.connection.ping()
	
	/*
	**
	** If the lost connection reesulted from a Servcer Crash, need to reset MAX_ALLOWED_PACKET. 
	**
	*/
	 
	await this.checkMaxAllowedPacketSize()
	
  }

  executeSQL(sqlStatement,args) {
    
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    return new Promise((resolve,reject) => {
      this.status.sqlTrace.write(this.traceSQL(sqlStatement));

      const stack = new Error().stack;
      const sqlStartTime = performance.now(); 
      this.connection.query(sqlStatement,args,async (err,results,fields) => {
        const sqlEndTime = performance.now()
        if (err) {
          const cause = this.trackExceptions(new MySQLError(this.DRIVER_ID,err,stack,sqlStatement))
          if (attemptReconnect && cause.lostConnection()) {
            attemptReconnect = false
            try {
              await this.reconnect(cause,'SQL')
              results = await this.executeSQL(sqlStatement,args);
              resolve(results);
            } catch (e) {
              reject(e);
            }                            
          }
          else {
            reject(cause);
          }
        }
		else {
          this.traceTiming(sqlStartTime,sqlEndTime)
          resolve(results);
		}
      })
    })
  }  
  
  async createSchema(schema) {      
  
    const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;                      
    const results = await this.executeSQL(sqlStatement,schema);
    return results;
    
  }
  
  async createStagingTable() {      
    const sqlStatement = `CREATE TEMPORARY TABLE IF NOT EXISTS "YADAMU_STAGING"("DATA" JSON)`;                     
    const results = await this.executeSQL(sqlStatement);
    return results;
  }

  async loadStagingTable(importFilePath,retryUpload) { 
  
    importFilePath = importFilePath.replace(/\\/g, "\\\\");
	
	try {
      const sqlStatement = `LOAD DATA LOCAL INFILE '${importFilePath}' INTO TABLE "YADAMU_STAGING" FIELDS ESCAPED BY ''`;                    
      const results = await this.executeSQL(sqlStatement);
      return results;
    } catch (e) {
	  if (retryUpload && (e instanceof MySQLError) && e.missingTable()) {
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
    const results = await  this.executeSQL(sqlStatement);
    return results;
  }
  
  async _executeDDL(ddl) {
    await this.createSchema(this.CURRENT_SCHEMA);
    const ddlResults = await Promise.all(ddl.map((ddlStatement) => {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA);
      return this.executeSQL(ddlStatement) 
    })) 
    return ddlResults;
  }
  
  setVendorProperties(connectionProperties) {
	super.setVendorProperties(connectionProperties)
    this.vendorProperties = Object.assign(
	  {},
	  MySQLConstants.CONNECTION_PROPERTY_DEFAULTS,
	  this.vendorProperties
    )
  }	 

  updateVendorProperties(vendorProperties) {

    vendorProperties.host              = this.parameters.HOSTNAME || vendorProperties.host
    vendorProperties.user              = this.parameters.USERNAME || vendorProperties.user 
    vendorProperties. password         = this.parameters.PASSWORD || vendorProperties. password
    vendorProperties.database          = this.parameters.DATABASE || vendorProperties.database
    vendorProperties.port              = this.parameters.PORT     || vendorProperties.port
    
	Object.assign(vendorProperties,MySQLConstants.CONNECTION_PROPERTY_DEFAULTS);
	
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
	switch (true) {
	  case (this.DB_VERSION < 8.0):
	    this.StatementLibrary = (await import('./57/mssqlStatementLibrary.js')).default
		this.StatementGenerator = (await import('../../dbShared/mysql/57statementGenerator.js')).default;
	    break;
      default:
	}
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
    this.status.sqlTrace.write(this.traceSQL(`begin transaction`));

    try {
      stack = new Error().stack
      await this.connection.beginTransaction();
      super.beginTransaction();
    } catch (e) {
      throw this.trackExceptions(new MySQLError(this.DRIVER_ID,e,stack,'mysql.Connection.beginTransaction()'))
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
      super.commitTransaction();
      stack = new Error().stack
      await this.connection.commit();
    } catch (e) {
      throw this.trackExceptions(new MySQLError(this.DRIVER_ID,e,stack,'mysql.Connection.commit()'))
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
            
    let stack;       
    this.status.sqlTrace.write(this.traceSQL(`rollback transaction`));
    
    try {
      super.rollbackTransaction();
      stack = new Error().stack
      await this.connection.rollback();
    } catch (e) {
      const newIssue = this.trackExceptions(new MySQLError(this.DRIVER_ID,e,stack,'mysql.Connection.rollback()'))
      this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
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
 
  async uploadFile(importFilePath) {

    let results = await this.createStagingTable();
    results = await this.loadStagingTable(importFilePath,true);
    return results;
  }

  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */
  
  processLog(results,operation) {
    if (results[0].logRecords !== null) {
      const log = JSON.parse(results[0].logRecords);
      super.processLog(log, operation, this.status, this.yadamuLogger)
      return log
    }
    else {
      return null
    }
  }

  async processFile(hndl) {
    const sqlStatement = `SET @RESULTS = ''; CALL YADAMU_IMPORT(?,@RESULTS); SELECT @RESULTS "logRecords";`;                     
    let results = await  this.executeSQL(sqlStatement,this.CURRENT_SCHEMA);
    results = results.pop();
    return this.processLog(results,'JSON_TABLE');
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
  
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION); 
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
      
    return await this.executeSQL(this.statementLibrary.SQL_SCHEMA_INFORMATION,[this.CURRENT_SCHEMA]);

  }
  
  inputStreamError(cause,sqlStatement) {
     return this.trackExceptions(((cause instanceof MySQLError) || (cause instanceof CopyOperationAborted)) ? cause : new MySQLError(this.DRIVER_ID,cause,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(queryInfo) {
	  
    // if the previous pipleline operation failed, it appears that the postgres driver will hang when creating a new QueryStream...
    
    if (this.failedPrematureClose) {
	  await this.reconnect(new Error('Previous Pipeline Aborted. Switching database connection'),'INPUT STREAM')
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
      this.yadamuLogger.info([`${this.constructor.name}.getInputStream()`],`Stating Keep Alive. Interval ${this.keepAliveInterval}ms.`)
      keepAliveHdl = setInterval(this.keepAlive,this.keepAliveInterval,this);
    }

    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(queryInfo.SQL_STATEMENT))
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        this.streamingStackTrace = new Error().stack
        this.activeInputStream = true;
        const is = this.connection.query(queryInfo.SQL_STATEMENT).stream();
        is.on('end',() => {
		  this.activeInputStream = false;
          // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,`${is.constructor.name}.onEnd()`,`${queryInfo.TABLE_NAME}`],``); 
          if (keepAliveHdl !== undefined) {
            clearInterval(keepAliveHdl);
            keepAliveHdl = undefined
          }
        })
        return is;
      } catch (e) {
        const cause = this.trackExceptions(new MySQLError(this.DRIVER_ID,e,this.streamingStackTrace,queryInfo.SQL_STATEMENT))
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
    return await super.generateStatementCache(this.StatementGenerator, schema)
  }

  getOutputManager(tableName,metrics) {
	 return super.getOutputManager(MySQLOutputManager,tableName,metrics)
  }
  
  getOutputStream(tableName,metrics) {
     return super.getOutputStream(MySQLWriter,tableName,metrics)
  }

  createParser(queryInfo) {
    this.parser = new MySQLParser(queryInfo,this.yadamuLogger,this);
    return this.parser;
  }  
    
  async keepAlive(dbi) {
    // Prevent Connections with Long Running streaming operations from timing out..
    dbi.yadamuLogger.info([`${this.constructor.name}.keepAlive()`],`Row [${dbi.parser.getCounter()}]`)
    try {
      this.results = await dbi.executeSQL('select 1');
    } catch (e) {
      // Don't care of timeout query fails
    }
  }

  classFactory(yadamu) {
    return new MySQLDBI(yadamu,this)
  }
  
  async getConnectionID() {
    const results = await this.executeSQL(`select connection_id() "pid"`)
    const pid = results[0].pid;
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

    if (!MySQLConstants.STAGED_DATA_SOURCES.includes(vendor)) {
       return false;
	}
	
	return this.reportCopyOperationMode(controlFile.settings.contentType === 'CSV',controlFilePath,controlFile.settings.contentType)
  }
   
}

export { MySQLDBI as default }