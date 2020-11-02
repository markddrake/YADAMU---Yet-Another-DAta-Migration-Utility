"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

const util = require('util')
const stream = require('stream')
const pipeline = util.promisify(stream.pipeline);

/* 
**
** Require Database Vendors API 
**
** ### pg_query-stream 3.x is not supported as it does not emit error events when the connection is lost: https://github.com/brianc/node-postgres/issues/2187
**
*/
const {Client,Pool} = require('pg')
const CopyFrom = require('pg-copy-streams').from;
const QueryStream = require('pg-query-stream')
const types = require('pg').types;

const Yadamu = require('../../common/yadamu.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const PostgresConstants = require('./postgresConstants.js')
const PostgresError = require('./postgresError.js')
const PostgresParser = require('./postgresParser.js');
const PostgresWriter = require('./postgresWriter.js');
const StatementGenerator = require('./statementGenerator.js');

class PostgresDBI extends YadamuDBI {
    
  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_SCHEMA_INFORMATION()         { return _SQL_SCHEMA_INFORMATION } 
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return _SQL_RELEASE_SAVE_POINT }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  get DATABASE_VENDOR()        { return PostgresConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return PostgresConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return PostgresConstants.STATEMENT_TERMINATOR };
   
  // Enable configuration via command line parameters
  
  get SPATIAL_FORMAT()         { return this.parameters.SPATIAL_FORMAT || PostgresConstants.SPATIAL_FORMAT }

  constructor(yadamu) {
    super(yadamu,PostgresConstants.DEFAULT_PARAMETERS);
       
    this.pgClient = undefined;
    this.useBinaryJSON = false
    
    /*
    FETCH_AS_STRING.forEach((PGOID) => {
      types.setTypeParser(PGOID, (v) => {return v})
    })
    */
  }

  /*
  **
  ** Local methods 
  **
  */
  
  async testConnection(connectionProperties) {   
    super.setConnectionProperties(connectionProperties);
	try {
      const pgClient = new Client(this.connectionProperties);
      await pgClient.connect();
      await pgClient.end();     
								  
	} catch (e) {
      throw e;
	}
	
  }
  
  async createConnectionPool() {
	
    this.logConnectionProperties();
	let sqlStartTime = performance.now();
	this.pool = new Pool(this.connectionProperties);
    this.traceTiming(sqlStartTime,performance.now())
	
	this.pool.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  const pgErr = this.captureException(new PostgresError(err,this.postgresStack,this.postgressOperation))
      this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,`POOL_ON_ERROR`],pgErr);
      // throw pgErr
    })

  }
  
  async getConnectionFromPool() {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)

	
	let stack
    this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));

	try {
      const sqlStartTime = performance.now();
	  stack = new Error().stack;
	  const connection = await this.pool.connect();
      this.traceTiming(sqlStartTime,performance.now())
      return connection
	} catch (e) {
	  throw this.captureException(new PostgresError(e,stack,'pg.Pool.connect()'))
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
      const pgClient = new Client(this.connectionProperties);
					
	  operation = 'Client.connect()'
	  stack = new Error().stack;
      this.connection = await pgClient.connect();
    
	  this.traceTiming(sqlStartTime,performance.now())
	} catch (e) {
      throw this.captureException(new PostgresException(e,stack,operation))
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
	  const pgErr = this.captureException(new PostgresError(err,this.postgresStack,this.postgressOperation))
      this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,`CONNECTION_ON_ERROR`],pgErr);
      // throw pgErr
    })
   
    await this.executeSQL(PostgresDBI.SQL_CONFIGURE_CONNECTION);				
	
    const results = await this.executeSQL(PostgresDBI.SQL_SYSTEM_INFORMATION)
	this._DB_VERSION = results.rows[0][3];

  }
  
  

  async closeConnection() {

  	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined && this.connection.release)})`)
	  
    if (this.connection !== undefined && this.connection.release) {
	  let stack
      try {
     	stack = new Error().stack;
        await this.connection.release();
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
		const err = this.captureException(new PostgresError(e,stack,'Client.release()'))
		throw err
      }
	}
  };
  
  async closePool() {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool !== undefined && this.pool.end)})`)

    if (this.pool !== undefined && this.pool.end) {
      let stack
	  try {
	    stack = new Error().stack
	    await this.pool.end();
        this.pool = undefined
  	  } catch (e) {
        this.pool = undefined
	    throw this.captureException(new PostgresError(e,stack,'pg.Pool.close()'))
	  }
	}
  }
  
  async _reconnect() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
    await this.executeSQL('select 1')
  }
  
  getConnectionProperties() {
    return {
      user      : this.parameters.USERNAME
     ,host      : this.parameters.HOSTNAME
     ,database  : this.parameters.DATABASE
     ,password  : this.parameters.PASSWORD
     ,port      : this.parameters.PORT
    }
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
		const cause = this.captureException(new PostgresError(e,stack,sqlStatement))
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
  
  async initialize() {
    await super.initialize(true);   
  }
    
  /*
  **
  **  Gracefully close down the database connection and pool.
  **
  */
  
  async finalize() {
	await super.finalize()
  } 

  /*
  **
  **  Abort the database connection and pool.
  **
  */

  async abort() {
									   
    await super.abort();
	  
  }


  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

     const sqlStatement =  `begin transaction`
     await this.executeSQL(sqlStatement);
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
    const sqlStatement =  `commit transaction`
    await this.executeSQL(sqlStatement);
	
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
	
    const sqlStatement =  `rollback transaction`
	 
	try {
      super.rollbackTransaction()
      await this.executeSQL(sqlStatement);
	} catch (newIssue) {
	  this.checkCause('ROLBACK TRANSACTION',cause,newIssue);								   
	}
  }

  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
																
	 

    await this.executeSQL(PostgresDBI.SQL_CREATE_SAVE_POINT);
    super.createSavePoint();
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)
																 
	 

    this.checkConnectionState(cause)
	 
	
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
		
    let stack
    try {
      await this.executeSQL(PostgresDBI.SQL_RESTORE_SAVE_POINT);
      super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVEPOINT',cause,newIssue);
	}
  }  

  async releaseSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(PostgresDBI.SQL_RELEASE_SAVE_POINT);    
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
      inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})
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
  	const sqlStatement = `select ${this.useBinaryJSON ? 'import_jsonb' : 'import_json'}(data,$1) from "YADAMU_STAGING"`;
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
      this.yadamuLogger.error([`${this.constructor.name}.processStagingTable()`],`Unexpected Error. No response from ${ this.useBinaryJSON === true ? 'CALL IMPORT_JSONB()' : 'CALL_IMPORT_JSON()'}. Please ensure file is valid JSON and NOT pretty printed.`);
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
  
  async getPostgisInfo() {

    let postgis = undefined    
    const sqlStatement  =  `SELECT PostGIS_full_version() "POSTGIS"`;

    try {
      const results = await this.executeSQL(sqlStatement)
      return results.rows[0].POSTGIS;
	} catch (e) {
      if (e.code && (e.code === '42883')) {
        // ### What to do about SystemInfo.SPATIAL_FORMAT There can be no Geography or Geometry columns without POSTGIS
        return "Not Installed"
      }
      else {
        throw e;
      }
    }
  }

  async getSystemInformation() {     
  
    const postgisInfo = await this.getPostgisInfo();
   
    const results = await this.executeSQL(PostgresDBI.SQL_SYSTEM_INFORMATION)
    const sysInfo = results.rows[0];
	
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo[4]
     ,vendor             : this.DATABASE_VENDOR
     ,postgisInfo        : postgisInfo
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : Yadamu.YADAMU_VERSION
	 ,currentUser        : sysInfo[1]
     ,sessionUser        : sysInfo[2]
	 ,dbName             : sysInfo[0]
     ,databaseVersion    : sysInfo[3]
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,nodeClient         : {
        version          : process.version
       ,architecture     : process.arch
       ,platform         : process.platform
      }      
    }
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
    
    const results = await this.executeSQL(PostgresDBI.SQL_SCHEMA_INFORMATION,[this.parameters[keyName],this.SPATIAL_FORMAT]);
	if ((results.rowCount === 1) && Array.isArray(results.rows[0][6])) { // EXPORT_JSON returned Errors
       this.processLog(results.rows[0][6],`EXPORT_JSON('${this.parameters[keyName]}','${this.SPATIAL_FORMAT}')`)
	}
    return this.generateSchemaInfo(results.rows)
  }

  createParser(tableInfo) {
    return new PostgresParser(tableInfo,this.yadamuLogger);
  }  
  
  streamingError(e,sqlStatement) {
    return this.captureException(new PostgresError(e,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(tableInfo) {        

    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		this.streamingStackTrace = new Error().stack
        const queryStream = new QueryStream(tableInfo.SQL_STATEMENT)
        this.traceTiming(sqlStartTime,performance.now())
        return await this.connection.query(queryStream)   
      } catch (e) {
		const cause = this.captureException(new PostgresError(e,this.streamingStackTrace,tableInfo.SQL_STATEMENT))
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'READER')
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
    await this.createSchema(this.parameters.TO_USER);
    await Promise.all(ddl.map(async (ddlStatement) => {
      try {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
        return await this.executeSQL(ddlStatement);
      } catch (e) {
        this.yadamuLogger.info([this.DATABASE_VENDOR,'DDL'],`${ddlStatement}\n`)
        this.yadamuLogger.handleException([this.DATABASE_VENDOR,'DDL'],e)
      }
    }))
  }
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator, schema, executeDDL)
  }

  getOutputStream(tableName,ddlComplete) {
	 return super.getOutputStream(PostgresWriter,tableName,ddlComplete)
  }
 
  async insertBatch(sqlStatement,batch) {
	 
    const result = await this.executeSQL(sqlStatement,batch)
    return result;
  }

  classFactory(yadamu) {
	return new PostgresDBI(yadamu)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select pg_backend_pid()`)
	const pid = results.rows[0][0];
    return pid
  }
	  
}

module.exports = PostgresDBI

const _SQL_CONFIGURE_CONNECTION = `set timezone to 'UTC'; SET extra_float_digits to 3; SET Intervalstyle = 'iso_8601'`

const _SQL_SCHEMA_INFORMATION   = `select * from EXPORT_JSON($1,$2)`;
 
const _SQL_SYSTEM_INFORMATION   = `select current_database() database_name,current_user, session_user, current_setting('server_version_num') database_version, right(to_char(current_timestamp,'YYYY-MM-DD"T"Hh24:MI:SSTZH:TZM'),6) timezone`;

const _SQL_CREATE_SAVE_POINT    = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT   = `ROLLBACK TO SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT   = `RELEASE SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _PGOID_DATE         = 1082; 
const _PGOID_TIMESTAMP    = 1114;
const _PGOID_TIMESTAMP_TZ = 1118;

