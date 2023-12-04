
import fs                             from 'fs';
import readline                       from 'readline';

import { 
  once 
}                                     from 'events';

import {
  Readable
}                                     from 'stream';

import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    
							          
import pg                             from 'pg';
const {Client,Pool} = pg;

import QueryStream                    from 'pg-query-stream'
import types                          from 'pg-types';

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

import {
  YadamuError,
  CopyOperationAborted
}                                    from '../../core/yadamuException.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'
import YadamuDataTypes                from '../base/yadamuDataTypes.js'

import {
	
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                    from '../file/fileException.js'

import AWSS3Constants                from '../awsS3/awsS3Constants.js'

/* Vendor Specific DBI Implimentation */                                   
						          
import RedshiftConstants             from './redshiftConstants.js'
import RedshiftDataTypes             from './redshiftDataTypes.js'
import RedshiftError                 from './redshiftException.js'
import RedshiftParser                from './redshiftParser.js'
import RedshiftWriter                from './redshiftWriter.js'
import RedshiftOutputManager         from './redshiftOutputManager.js'
import RedshiftStatementGenerator    from './redshiftStatementGenerator.js'
import RedshiftStatementLibrary      from './redshiftStatementLibrary.js'
import RedshiftCompare               from './redshiftCompare.js'

class RedshiftDBI extends YadamuDBI {
    
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.DBI_PARAMETERS,RedshiftConstants.DBI_PARAMETERS))
	return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return RedshiftDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  get DATABASE_KEY()             { return RedshiftConstants.DATABASE_KEY};
  get DATABASE_VENDOR()          { return RedshiftConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()          { return RedshiftConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()      { return true }
  get STATEMENT_TERMINATOR()     { return RedshiftConstants.STATEMENT_TERMINATOR };
   
  // Enable configuration via command line parameters
  
  get BYTEA_SIZING_MODEL()     { return this.parameters.BYTEA_SIZING_MODEL || RedshiftConstants.BYTEA_SIZING_MODEL }
  get STAGING_PLATFORM()       { return this.parameters.STAGING_PLATFORM   || RedshiftConstants.STAGING_PLATFORM } 
  
  get INBOUND_CIRCLE_FORMAT()  { return this.systemInformation?.typeMappings?.circleFormat || this.CIRCLE_FORMAT};

  get JSON_DATA_TYPE()         { return this.parameters.REDSHIFT_JSON_TYPE || RedshiftConstants.REDSHIFT_JSON_TYPE }
  
  get BUCKET() {
    this._BUCKET = this._BUCKET || (() => { 
	  const bucket = this.parameters.BUCKET || AWSS3Constants.BUCKET
	  this._BUCKET = YadamuLibrary.macroSubstitions(bucket, this.yadamu.MACROS)
	  return this._BUCKET
	})()
	return this._BUCKET
  }

  get SUPPORTED_STAGING_PLATFORMS()   { return RedshiftConstants.STAGED_DATA_SOURCES }

  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
	this.DATA_TYPES = RedshiftDataTypes
       
    this.pgClient = undefined;
    this.useBinaryJSON = false
    
    /*
    FETCH_AS_STRING.forEach((PGOID) => {
      types.setTypeParser(PGOID, (v) => {return v})
    })
    */
	
    this.StatementLibrary = RedshiftStatementLibrary
    this.statementLibrary = undefined
    this.pipelineAborted = false;
   
  }

  /*
  **
  ** Local methods 
  **
  */
  
  async testConnection(connectionProperties) {   
    super.setConnectionProperties(connectionProperties)
	try {
      const pgClient = new Client(this.vendorProperties)
      await pgClient.connect()
      await pgClient.end()    							  
	} catch (e) {
      throw e;
	}
	
  }
  
  async createConnectionPool() {
	
    this.logConnectionProperties()
	let sqlStartTime = performance.now()
	this.pool = new Pool(this.vendorProperties)
    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	
	this.pool.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  const pgErr = this.trackExceptions(new RedshiftError(this.DRIVER_ID,err,this.redshiftStack,this.redshiftsOperation))
      this.LOGGER.handleWarning([this.DATABASE_VENDOR,this.ROLE,`POOL_ON_ERROR`],pgErr)
      // throw pgErr
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
	  throw this.trackExceptions(new RedshiftError(this.DRIVER_ID,e,stack,'pg.Pool.connect()'))
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
      this.connection = new Client(this.vendorProperties)
	  operation = 'Client.connect()'
	  stack = new Error().stack;
      await this.connection.connect()
	  this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      await this.configureConnection()
      return this.connection
	} catch (e) {
      throw this.trackExceptions(new RedshiftError(this.DRIVER_ID,e,stack,operation))
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
	  const pgErr = this.trackExceptions(new RedshiftError(this.DRIVER_ID,err,this.redshiftStack,this.redshiftsOperation))
      this.LOGGER.handleWarning([this.DATABASE_VENDOR,this.ROLE,`CONNECTION_ON_ERROR`],pgErr)
      // throw pgErr
    })
   
    await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION)				
	
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION)
	this._DATABASE_VERSION = results.rows[0][3];
	
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
		const err = this.trackExceptions(new RedshiftError(this.DRIVER_ID,e,stack,'Client.release()'))
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
	    throw this.trackExceptions(new RedshiftError(this.DRIVER_ID,e,stack,'pg.Pool.close()'))
	  }
	}
  }
  
  async _reconnect() {
	await super._reconnect()
    const results = await this.executeSQL('select now()')
  }
  
  updateVendorProperties(vendorProperties) {

    vendorProperties.user      = this.parameters.USERNAME || vendorProperties.user
    vendorProperties.host      = this.parameters.HOSTNAME || vendorProperties.host 
    vendorProperties.database  = this.parameters.DATABASE || vendorProperties.database
    vendorProperties.password  = this.parameters.PASSWORD || vendorProperties.password 
    vendorProperties.port      = this.parameters.PORT     || vendorProperties.port

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
		const cause = this.trackExceptions(new RedshiftError(this.DRIVER_ID,e,stack,sqlStatement))
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
															
    // await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT)
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)
																 
    this.checkConnectionState(cause)
	 
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
		
    let stack
    try {
      // await this.executeSQL(this.StatementLibrary.SQL_RESTORE_SAVE_POINT)
      super.restoreSavePoint()
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVEPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await // this.executeSQL(this.StatementLibrary.SQL_RELEASE_SAVE_POINT)    
    super.releaseSavePoint()

  } 
  
  /*
  **
  ** The following methods are used by JSON_TABLE() style import operations  
  **
  */

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
  	const sqlStatement = `select ${this.useBinaryJSON ? 'YADAMU_IMPORT_JSONB' : 'YADAMU_IMPORT_JSON'}(data,$1) from "YADAMU_STAGING"`;
  	var results = await this.executeSQL(sqlStatement,[schema])
    if (results.rows.length > 0) {
      if (this.useBinaryJSON  === true) {
	    return this.processLog(results.rows[0][0],'JSONB_EACH')  
      }
      else {
	    return this.processLog(results.rows[0][0],'JSON_EACH')  
      }
    }
    else {
      this.LOGGER.error([`${this.constructor.name}.processStagingTable()`],`Unexpected Error. No response from ${ this.useBinaryJSON === true ? 'CALL YADAMU_IMPORT_JSONB()' : 'CALL_YADAMU_IMPORT_JSON()'}. Please ensure file is valid JSON and NOT pretty printed.`)
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
  
  generateSelectListEntry(columnInfo) {
	const dataType = RedshiftDataTypes.decomposeDataType(columnInfo[3])
	switch (dataType.type) {
	  case 'date':
		return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS"Z"') "${columnInfo[2]}"`
	  case 'geography':
	    return `ST_AsBinary("${columnInfo[2]}") "${columnInfo[2]}"` 
	  case 'timestamp wihtout time zone':
		return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS.US') "${columnInfo[2]}"`
	  case 'time':
	  case 'timestamp':
	    switch (dataType.typeQualifier) {
		   case 'without time zone':
		     return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS.US') "${columnInfo[2]}"`
		   default:
		     return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS.USOF') "${columnInfo[2]}"`
	    }
	  case 'timetz':
	  default:
	    return `"${columnInfo[2]}"`
    }
  }
  
  async getSchemaMetadata() {
    const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION,[]) // ,[this.CURRENT_SCHEMA,this.SPATIAL_FORMAT,{"circleAsPolygon": this.INBOUND_CIRCLE_FORMAT === 'POLYGON',"calculateByteaSize":true}])
	const schemaInfo = this.buildSchemaInfo(results.rows)
	return schemaInfo
  }

  createParser(tableInfo,parseDelay) {
    return new RedshiftParser(this,tableInfo,this.LOGGER,parseDelay)
  }  
  
  inputStreamError(cause,sqlStatement) {
    return this.trackExceptions(((cause instanceof RedshiftError) || (cause instanceof CopyOperationAborted)) ? cause : new RedshiftError(this.DRIVER_ID,cause,new Error().stack,sqlStatement))
  }

  async getInputStream(queryInfo) {       

    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,queryInfo.TABLE_NAME],'')
    
    /*
    **
    **	If the previous pipleline operation failed, it appears that the redshift driver will hang when creating a new QueryStream...
	**
	*/
	
    if (this.failedPrematureClose) {
	  await this.reconnect(new Error('Previous Pipeline Aborted. Switching database connection'),'INPUT STREAM')
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
		*/
		
        const handleConnectionError = (err) => {inputStream.destroy(err); inputStream.emit('error',err)}
	    this.connection.on('error',handleConnectionError)
        inputStream.on('end',() => { this.connection.removeListener('end',handleConnectionError)}).on('error',() => { this.connection.removeListener('error',handleConnectionError)})  
  		
		return inputStream
      } catch (e) {
		const cause = this.trackExceptions(new RedshiftError(this.DRIVER_ID,e,stack,queryInfo.SQL_STATEMENT))
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
    await this.createSchema(this.CURRENT_SCHEMA)
	
	try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA)
		// this.SQL_TRACE.traceSQL(ddlStatement)
        return this.executeSQL(ddlStatement)
      }))
    } catch (e) {
	 this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'DDL'],e)
	 results = e;
    }
	return results;
  }
   
  async generateStatementCache(schema) {
    return await super.generateStatementCache(RedshiftStatementGenerator, schema)
  }

  getOutputStream(tableName,metrics) {
	 return super.getOutputStream(RedshiftWriter,tableName,metrics)
  }

  getOutputStream(tableName,metrics) {
	 return super.getOutputStream(RedshiftOutputManager,tableName,metrics)
  }
 
  classFactory(yadamu) {
	return new RedshiftDBI(yadamu,this,this.connectionParameters,this.parameters)
  }

  async reportCopyErrors(tableName,metrics) {
	  
	 const causes = []
	 let sizeIssue = 0;
	 
     let results = await this.executeSQL(this.StatementLibrary.SQL_COPY_ERROR_SUMMARY)
	 if (results.rows.length === 0) {
       // Special error handling for tables with one or more columns of type 'SUPER'
       results = await this.executeSQL(this.StatementLibrary.SQL_SUPER_ERROR_SUMMARY)
     }
	 
	 results.rows.forEach((r) => {
	   const err = new Error()
	   err.code = r[3]
	   err.stack =  `${metrics.stack.slice(0,5)}: ${r[2]}${stack.slice(5)}`
	   err.columnName = r[1]
	   err.recordNumber = r[0]
	   err.dataLength = parseInt(r[4])
	   err.tags = []
	   if (r[2].trim().endsWith(' exceeds DDL length')) {
	     err.tags.push("CONTENT_TOO_LARGE")
		 sizeIssue++
	   }
  	   causes.push(err)
	 })
     const err = new Error(`Errors detected durng COPY operation: ${results.rows.length} records rejected.`)
	 err.tags = []
	 if (causes.length === sizeIssue) {
	    err.tags.push("CONTENT_TOO_LARGE")
	 } 
     err.cause = causes;	 
	 err.sql = metrics.sql;
	 this.LOGGER.handleException([...err.tags,this.DATABASE_VENDOR,tableName],err)
  }

  async copyOperation(tableName,copyOperation,metrics) {
	
	try {
	  metrics.stack = new Error().stack;
	  metrics.writerStartTime = performance.now()
	  let results = await this.beginTransaction()
	  results = await this.executeSQL(copyOperation.dml)
  	  results = await this.commitTransaction()
	  metrics.writerEndTime = performance.now()
	  results = await this.executeSQL(this.StatementLibrary.SQL_COPY_STATUS)
	  metrics.committed = parseInt(results.rows[0][0])
	  results = await this.executeSQL(this.StatementLibrary.SQL_COPY_ERRORS)
	  metrics.skipped = parseInt(results.rows[0][0])
	  metrics.read = metrics.committed + metrics.skipped
  	} catch(cause) {
	  metrics.writerError = cause
	  try {
        if ((cause instanceof RedshiftError) && cause.detailedErrorAvailable()) {
		  try {
	        await this.reportCopyErrors(tableName,copy.dml,metrics.failed)
	  	    return
	      } catch (e) {
  		    cause.cause = e
	      }
        }
        this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'Copy',tableName],cause)
	    let results = await this.rollbackTransaction()
	  } catch (e) {
		e.cause = metrics.writerError
		metrics.writerError = e
	  }
	}
	return metrics
  }
  
  async getComparator(configuration) {
	 await this.initialize()
	 return new RedshiftCompare(this,configuration)
  }

}

export { RedshiftDBI as default }