
import fs                             from 'fs';
import fsp                            from 'fs/promises';
import crypto                         from 'crypto';

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
  CopyOperationAborted
}                                     from '../../core/yadamuException.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                    from '../file/fileException.js'

/* Vendor Specific DBI Implimentation */                                   						          

import VerticaConstants              from './verticaConstants.js'
import VerticaDataTypes              from './verticaDataTypes.js'
import VerticaInputStream            from './verticaReader.js'
import VerticaParser                 from './verticaParser.js'
import VerticaWriter                 from './verticaWriter.js'
import VerticaOutputManager          from './verticaOutputManager.js'
import VerticaStatementGenerator     from './verticaStatementGenerator.js'
import VerticaStatementLibrary       from './verticaStatementLibrary.js'

import { 
  VerticaError,
  VertiaCopyOperationFailure
}                                    from './verticaException.js'



class VerticaDBI extends YadamuDBI {
    
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.DBI_PARAMETERS,VerticaConstants.DBI_PARAMETERS))
	return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return VerticaDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  get DATABASE_KEY()           { return VerticaConstants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return VerticaConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return VerticaConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()    { return true }
  get STATEMENT_TERMINATOR()   { return VerticaConstants.STATEMENT_TERMINATOR };
   
  // Enable configuration via command line parameters
  
  get COPY_TRIM_WHITEPSPACE()  { return this.parameters.COPY_TRIM_WHITEPSPACE || VerticaConstants.COPY_TRIM_WHITEPSPACE }
  get MERGEOUT_INSERT_COUNT()  { return this.parameters.MERGEOUT_INSERT_COUNT || VerticaConstants.MERGEOUT_INSERT_COUNT }
  
  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.LOADER_STAGING }

 constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
	this.DATA_TYPES = VerticaDataTypes
       
	this.DATA_TYPES.storageOptions.JSON_TYPE    = this.parameters.VERTICA_JSON_STORAGE_OPTION     || this.DBI_PARAMETERS.JSON_STORAGE_OPTION     || this.DATA_TYPES.storageOptions.JSON_TYPE
	this.DATA_TYPES.storageOptions.XML_TYPE     = this.parameters.VERTICA_XML_STORAGE_OPTION      || this.DBI_PARAMETERS.XML_STORAGE_OPTION      || this.DATA_TYPES.storageOptions.XML_TYPE
	   
    this.pgClient = undefined;
    
    /*
    FETCH_AS_STRING.forEach((PGOID) => {
      types.setTypeParser(PGOID, (v) => {return v})
    })
    */
	
    this.StatementLibrary = VerticaStatementLibrary
    this.statementLibrary = undefined
    this.pipelineAborted = false;
	
    this.verticaStack = new Error().stack
    this.verticaOperation = undefined

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
	  const verticaError = this.trackExceptions(new VerticaError(this.DRIVER_ID,err,this.verticaStack,this.verticaOperation))
      this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,this.ROLE,`POOL_ON_ERROR`],verticaError)
      // throw verticaError
    })

  }
  
  async getConnectionFromPool() {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`getConnectionFromPool()`)
	  
	let stack
    this.SQL_TRACE.comment(`Getting Connection From Pool.`)

	try {
      const sqlStartTime = performance.now()
	  stack = new Error().stack;
	  const connection = await this.pool.connect()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return connection
	} catch (e) {
	  throw this.trackExceptions(new VerticaError(this.DRIVER_ID,e,stack,'pg.Pool.connect()'))
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
      throw this.trackExceptions(new VerticaError(this.DRIVER_ID,e,stack,operation))
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
          this.yadamuLogger.info([this.DATABASE_VENDOR,this.ROLE,`NOTICE`],`${n.message ? n.message : JSON.stringify(n)}`)
      }
    })  
  
	this.connection.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  const verticaError = this.trackExceptions(new VerticaError(this.DRIVER_ID,err,this.verticaStack,this.verticaOperation))
      this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,this.ROLE,`CONNECTION_ON_ERROR`],verticaError)
      // throw verticaError
    })
   
    await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION)				
	
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION)
	this._DATABASE_VERSION = results.rows[0][3];
	
  }
  
  async closeConnection(options) {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection()`)
	  
    if (this.connection !== undefined && this.connection.release) {
	  let stack
      try {
     	stack = new Error().stack;
        await this.connection.release()
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
		const err = this.trackExceptions(new VerticaError(this.DRIVER_ID,e,stack,'Client.release()'))
		throw err
      }
	}
  };
  
  async closePool(options) {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],`closePool(${(this.pool !== undefined && this.pool.end)})`)

    if (this.pool !== undefined && this.pool.end) {
      let stack
	  try {
	    stack = new Error().stack
	    await this.pool.end()
        this.pool = undefined
  	  } catch (e) {
        this.pool = undefined
	    throw this.trackExceptions(new VerticaError(this.DRIVER_ID,e,stack,'pg.Pool.close()'))
	  }
	}
  }
  
  async _reconnect() {
    await super._reconnect()
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
	if (this.SQL_TRACE.enabled  &&(typeof sqlStatement === 'string')){
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
		const cause = this.trackExceptions(new VerticaError(this.DRIVER_ID,e,stack,sqlStatement))
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
  
  async insertBatch(sqlStatement,rejectedRecordsTableName) {

    const stack = new Error().stack
    let result = await this.executeSQL(sqlStatement)
	
	const copyResults = `select get_num_accepted_rows(),get_num_rejected_rows()`
    result = await this.executeSQL(copyResults)
	const inserted = parseInt(result.rows[0][0])
	const rejected = parseInt(result.rows[0][1])
	let errors = []
	if (rejected > 0) {
      const results = await this.executeSQL(`select row_number, substring(rejected_reason,1, 256), rejected_data_orig_length from "${rejectedRecordsTableName}" where transaction_id = current_trans_id()`)
      errors = results.rows
    }
	// await this.executeSQL(`drop table if exists "${rejectedRecordsTableName}"`)
    return {
	  inserted : inserted
	, rejected : rejected
	, errors   : errors
	}
  }
  
  async initialize() {
    await super.initialize(true)
  }
  
  async initializeImport() {
	 super.initializeImport()
	 await fsp.mkdir(this.LOCAL_STAGING_AREA,{recursive: true})
  }
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

     // ### ANSI-92 Transaction model - Transaction is always in progress 
     // await this.executeSQL(this.StatementLibrary.SQL_BEGIN_TRANSACTION)
	 
	 super.beginTransaction()

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],new Error().stack)

	super.commitTransaction()
    await this.executeSQL(this.StatementLibrary.SQL_COMMIT_TRANSACTION)
	
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
      await this.executeSQL(this.StatementLibrary.SQL_ROLLBACK_TRANSACTION)
	} catch (newIssue) {
	  this.checkCause('ROLBACK TRANSACTION',cause,newIssue)								   
	}
  }

  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
															
    await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT)
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)
																 
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

    // this.yadamuLogger.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(this.StatementLibrary.SQL_RELEASE_SAVE_POINT)    
    super.releaseSavePoint()

  } 
  
  processLog(log,operation) {
    super.processLog(log, operation, this.status, this.yadamuLogger)
    return log
  }

  async processStagingTable(schema) {  	
  	const sqlStatement = `select YADAMU_IMPORT_JSON(data,$1) from "YADAMU_STAGING"`;
  	var results = await this.executeSQL(sqlStatement,[schema])
    if (results.rows.length > 0) {
      return this.processLog(results.rows[0][0],'JSON_EACH')  
    }
    else {
      this.yadamuLogger.error([`${this.constructor.name}.processStagingTable()`],`Unexpected Error. No response from CALL_YADAMU_IMPORT_JSON(). Please ensure file is valid JSON and NOT pretty printed.`)
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
    
  async getSchemaMetadata() {
    const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION(this.CURRENT_SCHEMA))
	const schemaInfo = this.buildSchemaInfo(results.rows)
	return schemaInfo
  }

  createParser(queryInfo,parseDelay) {
    return new VerticaParser(this,queryInfo,this.yadamuLogger,parseDelay)
  }  
  
  inputStreamError(cause,sqlStatement) {
    return this.trackExceptions(((cause instanceof VerticaError) || (cause instanceof CopyOperationAborted)) ? cause : new VerticaError(this.DRIVER_ID,cause,this.streamingStackTrace,sqlStatement))
  }

  generateSelectListEntry(columnInfo) {
	const dataType = VerticaDataTypes.decomposeDataType(columnInfo[3])
	switch (dataType.type) {
	  case this.DATA_TYPES.INTERVAL_DAY_TO_SECOND_TYPE:
	  case this.DATA_TYPES.INTERVAL_YEAR_TO_MONTH_TYPE:
	    return `CAST("${columnInfo[2]}" AS VARCHAR) "${columnInfo[2]}"` 
	  /*
	  case 'numeric':
	    return `CAST("${columnInfo[2]}" AS VARCHAR) "${columnInfo[2]}"` 
	  */
	  case this.DATA_TYPES.DATE_TYPE:
		return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS"Z"') "${columnInfo[2]}"`
	  case this.DATA_TYPES.TIME_TYPE:
	  case this.DATA_TYPES.TIMESTAMP_TYPE:
	    return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"') "${columnInfo[2]}"`
	  case this.DATA_TYPES.TIME_TZ_TYPE:
	  case this.DATA_TYPES.TIMESTAMP_TZ_TYPE:
	    return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM') "${columnInfo[2]}"`
	  case this.DATA_TYPES.GEOMETRY_TYPE:
	  case this.DATA_TYPES.GEOGRAPHY_TYPE:
	    // TODO : Support Text / GeoJSON
	    return `ST_AsBinary("${columnInfo[2]}") "${columnInfo[2]}"` 
	  case this.DATA_TYPES.DOUBLE_TYPE:
	  case this.DATA_TYPES.DOUBLE_TYPE:
	    // return `case when to_char("${columnInfo[2]}") <> "${columnInfo[2]}" then YADAMU.RENDER_FLOAT("${columnInfo[2]}") else to_char("${columnInfo[2]}") end "${columnInfo[2]}"` 
	    return `YADAMU.RENDER_FLOAT("${columnInfo[2]}") "${columnInfo[2]}"` 
	  default:
	    return `"${columnInfo[2]}"`
    }
  }

  async getInputStream(queryInfo) {        
  
    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,queryInfo.TABLE_NAME],'')
    
	if (this.failedPrematureClose) {
	  await this.reconnect(new Error('Previous Pipeline Aborted. Switching database connection'),'INPUT STREAM')
	}
 		
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
	
	/*
	**
	** pg-query-stream loops repeatidly returning all the rows in the table with Vertica.
	**
	** pg-cursor also loops repeatidly returning all the rows in the table with Vertica.
	**
	** Use a custom Reader class to stream rows	
	**
	*/
	
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
		this.streamingStackTrace = new Error().stack
		const inputStream = new VerticaInputStream(this.connection,queryInfo.SQL_STATEMENT,this.yadamuLogger)
		return inputStream
      } catch (e) {
		const cause = this.trackExceptions(new VerticaError(this.DRIVER_ID,e,this.streamingStackTrace,queryInfo.SQL_STATEMENT))
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
		// this.SQL_TRACE.traceSQL(ddlStatement))
        return this.executeSQL(ddlStatement)
      }))
    } catch (e) {
	 this.yadamuLogger.handleException([this.DATABASE_VENDOR,this.ROLE,'DDL'],e)
	 results = e;
    }
	return results;
  }
   
  async generateStatementCache(schema) {
    return await super.generateStatementCache(VerticaStatementGenerator, schema)
  }

  getOutputStream(tableName,metrics) {
	 return super.getOutputStream(VerticaWriter,tableName,metrics)
  }
  
  getOutputManager(tableName,metrics) {
	 return super.getOutputStream(VerticaOutputManager,tableName,metrics)
  }
 
  classFactory(yadamu) {
	return new VerticaDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select pg_backend_pid()`)
	const pid = results.rows[0][0];
    return pid
  }

  async reportCopyErrors(tableName,metrics) {

	 const causes = []
	 let sizeIssue = 0;
	 metrics.rejected.forEach((r) => {
	   const err = new Error()
	   err.stack =  `${metrics.stack.slice(0,5)}: ${r[1]}${metrics.stack.slice(5)}`
	   err.recordNumber = r[0]
	   const columnNameOffset = r[1].indexOf('column: [') + 9
	   /*
	   err.columnName = r[1].substring(columnNameOffset,r[1].indexOf(']',columnNameOffset+1))
	   err.columnIdx = this.metadata.columnNames.indexOf(err.columnName)
	   err.columnLength = this.maxLengths[err.columnIdx]
	   err.dataLength = parseInt(r[2])
	   */
	   err.tags = []
	   if (err.dataLength > err.columnLength) {
		 err.tags.push("CONTENT_TOO_LARGE")
		 sizeIssue++
	   }
  	   causes.push(err)
	 })
	 const err = new Error(`Errors detected durng COPY operation: ${metrics.rejected.length} records rejected.`)
	 err.tags = []
	 if (causes.length === sizeIssue) {
	    err.tags.push("CONTENT_TOO_LARGE")
	 } 
     err.cause = causes;	 
	 err.sql = metrics.sql;
	 this.yadamuLogger.handleException([...err.tags,this.DATABASE_VENDOR,tableName],err)
  }

  async copyOperation(tableName,copyOperation,metrics) {
	
	try {
  	  const rejectedRecordsTableName = `YRT-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
	  const sqlStatement = `${copyOperation.dml} REJECTED DATA AS TABLE "${rejectedRecordsTableName}"  NO COMMIT`;
	  metrics.writerStartTime = performance.now()
	  let results = await this.beginTransaction()
	  metrics.stack = new Error().stack
	  results = await this.insertBatch(sqlStatement,rejectedRecordsTableName)
	  metrics.writerEndTime = performance.now()
	  metrics.written = results.inserted
	  metrics.skipped = results.rejected 
	  metrics.rejected = results.errors
	  metrics.read = metrics.written + metrics.skipped
	  results = await this.commitTransaction()
	  metrics.committed = metrics.written 
	  metrics.written = 0
  	} catch(e) {
	  metrics.writerError = e
	  try {
  	    this.yadamuLogger.handleException([this.DATABASE_VENDOR,this.ROLE,'COPY',tableName],e)
	    let results = await this.rollbackTransaction()
	  } catch (e) {
		e.cause = metrics.writerError
		metrics.writerError = e
	  }
	}
	return metrics
  }    

}

export { VerticaDBI as default }
