"use strict" 
const fs = require('fs');
const fsp = require('fs').promises
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');
const crypto = require('crypto');

const util = require('util')
const stream = require('stream')
const pipeline = util.promisify(stream.pipeline);

/* 
**
** Require Database Vendors API 
**
*/
const {Query, Client,Pool} = require('pg')
const Cursor  = require('pg-cursor')
const CopyFrom = require('pg-copy-streams').from;
const QueryStream = require('pg-query-stream')
const types = require('pg').types;

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')

const VerticaConstants = require('./verticaConstants.js')
const VerticaInputStream = require('./verticaReader.js')
const { VerticaError, VertiaCopyOperationFailure } = require('./verticaException.js')
const VerticaParser = require('./verticaParser.js');
const VerticaWriter = require('./verticaWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const VerticaStatementLibrary = require('./verticaStatementLibrary.js');

const {YadamuError} = require('../../common/yadamuException.js');
const {FileError, FileNotFound, DirectoryNotFound} = require('../../file/node/fileException.js');

class VerticaDBI extends YadamuDBI {
    
  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,VerticaConstants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return VerticaDBI.YADAMU_DBI_PARAMETERS
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
  
  get SPATIAL_FORMAT()         { return this.parameters.SPATIAL_FORMAT || VerticaConstants.SPATIAL_FORMAT }
  get CIRCLE_FORMAT()          { return this.parameters.CIRCLE_FORMAT || VerticaConstants.CIRCLE_FORMAT }
  
  get INBOUND_CIRCLE_FORMAT()  { return this.systemInformation?.typeMappings?.circleFormat || this.CIRCLE_FORMAT};
  get COPY_TRIM_WHITEPSPACE()  { return this.parameters.COPY_TRIM_WHITEPSPACE || VerticaConstants.COPY_TRIM_WHITEPSPACE }
  get MERGEOUT_INSERT_COUNT()  { return this.parameters.MERGEOUT_INSERT_COUNT || VerticaConstants.MERGEOUT_INSERT_COUNT }
  
  constructor(yadamu,settings,parameters) {
    super(yadamu,settings,parameters);
       
    this.pgClient = undefined;
    
    /*
    FETCH_AS_STRING.forEach((PGOID) => {
      types.setTypeParser(PGOID, (v) => {return v})
    })
    */
	
    this.StatementLibrary = VerticaStatementLibrary
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
	  const pgErr = this.trackExceptions(new VerticaError(err,this.verticaStack,this.verticasOperation))
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
	  throw this.trackExceptions(new VerticaError(e,stack,'pg.Pool.connect()'))
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
      throw this.trackExceptions(new VerticaException(e,stack,operation))
	}
    await configureConnection();
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
	  const pgErr = this.trackExceptions(new VerticaError(err,this.verticaStack,this.verticasOperation))
      this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,`CONNECTION_ON_ERROR`],pgErr);
      // throw pgErr
    })
   
    await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION);				
	
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION)
	this._DB_VERSION = results.rows[0][3];
	
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
		const err = this.trackExceptions(new VerticaError(e,stack,'Client.release()'))
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
	    throw this.trackExceptions(new VerticaError(e,stack,'pg.Pool.close()'))
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
		const cause = this.trackExceptions(new VerticaError(e,stack,sqlStatement))
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
    result = await this.executeSQL(copyResults);
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
    await super.initialize(true);
  }
  
  async initializeImport() {
	 super.initializeImport()
	 await fsp.mkdir(this.LOCAL_STAGING_AREA,{recursive: true});
  }
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

     // ### ANSI-92 Transaction model - Transaction is always in progress 
     // await this.executeSQL(this.StatementLibrary.SQL_BEGIN_TRANSACTION);
	 
	 super.beginTransaction();

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],new Error().stack)

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
  	sqlStatement = `create temporary table if not exists "YADAMU_STAGING" (data, "JSON") on commit preserve rows`;					   
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
    let elapsedTime;
    try {
      await this.createStagingTable();    
      elapsedTime = await this.loadStagingTable(importFilePath)
    }
    catch (e) {
      throw e
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
  	const sqlStatement = `select YADAMU_IMPORT_JSON(data,$1) from "YADAMU_STAGING"`;
  	var results = await this.executeSQL(sqlStatement,[schema]);
    if (results.rows.length > 0) {
      return this.processLog(results.rows[0][0],'JSON_EACH');  
    }
    else {
      this.yadamuLogger.error([`${this.constructor.name}.processStagingTable()`],`Unexpected Error. No response from CALL_YADAMU_IMPORT_JSON(). Please ensure file is valid JSON and NOT pretty printed.`);
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
    const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION(this.parameters[keyName]));
	const schemaInfo = this.buildSchemaInfo(results.rows)
	return schemaInfo
  }

  createParser(tableInfo) {
    return new VerticaParser(tableInfo,this.yadamuLogger);
  }  
  
  inputStreamError(e,sqlStatement) {
    return this.trackExceptions(new VerticaError(e,this.streamingStackTrace,sqlStatement))
  }

  generateSelectListEntry(columnInfo) {
	const dataType = YadamuLibrary.decomposeDataType(columnInfo[3])
	switch (dataType.type) {
	  case 'interval':
	    return `CAST("${columnInfo[2]}" AS VARCHAR) "${columnInfo[2]}"` 
	  /*
	  case 'numeric':
	    return `CAST("${columnInfo[2]}" AS VARCHAR) "${columnInfo[2]}"` 
	  */
	  case 'date':
		return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS"Z"') "${columnInfo[2]}"`
	  case 'time':
	  case 'timestamp':
	    return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"') "${columnInfo[2]}"`
	  case 'timetz':
	  case 'timestamptz':
	    return `TO_CHAR("${columnInfo[2]}",'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM') "${columnInfo[2]}"`
	  case 'geometry':
	  case 'geography':
	    // TODO : Support Text / GeoJSON
	    return `TO_HEX(ST_AsBinary("${columnInfo[2]}")) "${columnInfo[2]}"` 
	  default:
	    return `"${columnInfo[2]}"`
    }
  }

  async getInputStream(tableInfo) {        
  
    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,tableInfo.TABLE_NAME],'')
    
	if (this.failedPrematureClose) {
	  await this.reconnect(new Error('Previous Pipeline Aborted. Switching database connection'),'INPUT STREAM')
	}
 		
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))
	
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
        const sqlStartTime = performance.now();
		this.streamingStackTrace = new Error().stack
		const inputStream = new VerticaInputStream(this.connection,tableInfo.SQL_STATEMENT,this.yadamuLogger)
		return inputStream
      } catch (e) {
		const cause = this.trackExceptions(new VerticaError(e,this.streamingStackTrace,tableInfo.SQL_STATEMENT))
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
	 return super.getOutputStream(VerticaWriter,tableName,ddlComplete)
  }
 
  classFactory(yadamu) {
	return new VerticaDBI(yadamu)
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

    if (!VerticaConstants.STAGED_DATA_SOURCES.includes(vendor)) {
       return false;
	}
	
	return this.reportCopyOperationMode(controlFile.settings.contentType === 'CSV',controlFilePath,controlFile.settings.contentType)
  }
  
  async reportCopyErrors(tableName,results,stack,statement) {
	  
	 const causes = []
	 let sizeIssue = 0;
	 results.forEach((r) => {
	   const err = new Error()
	   err.stack =  `${stack.slice(0,5)}: ${r[1]}${stack.slice(5)}`
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
	 const err = new Error(`Errors detected durng COPY operation: ${results.length} records rejected.`);
	 err.tags = []
	 if (causes.length === sizeIssue) {
	    err.tags.push("CONTENT_TOO_LARGE")
	 } 
     err.cause = causes;	 
	 err.sql = statement;
	 this.yadamuLogger.handleException([...err.tags,this.DATABASE_VENDOR,tableName],err)
  }

  async copyOperation(tableName,statement) {
	let startTime 
    try {
      const stack = new Error().stack
  	  const rejectedRecordsTableName = `YRT-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
      const sqlStatement = `${statement} REJECTED DATA AS TABLE "${rejectedRecordsTableName}"  NO COMMIT`; 	
	  let startTime = performance.now()
      const results = await this.insertBatch(sqlStatement,rejectedRecordsTableName);
	  const elapsedTime = performance.now() - startTime;
	  if (results.rejected > 0) {
	    await this.reportCopyErrors(tableName,results.errors,stack,sqlStatement)
      }
      await this.commitTransaction();
	  const writerThroughput = isNaN(elapsedTime) ? 'N/A' : Math.round((results.inserted/elapsedTime) * 1000)
      const writerTimings = `Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s. Throughput: ${Math.round(writerThroughput)} rows/s.`
  	  const rowSummary = results.rejected === 0 ? `Rows ${results.inserted}.` : `Rows ${results.inserted}. Skipped ${results.rejected}.`
      if (results.rejected > 0) {
		this.yadamuLogger.error([tableName,'Copy'],`${rowSummary} ${writerTimings}`)  
	  }
	  else {
		this.yadamuLogger.info([tableName,'Copy'],`${rowSummary} ${writerTimings}`)  
	  }	  
	} catch (cause) { 
	  const elapsedTime = performance.now() - startTime;
  	  await this.rollbackTransaction(cause);
	  // await this.reportCopyErrors(batch.copy,'Copy',cause)
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'Copy',tableName],cause)
    } 
  }
}

module.exports = VerticaDBI
