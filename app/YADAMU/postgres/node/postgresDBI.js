"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/
const {Client,Pool} = require('pg')
const CopyFrom = require('pg-copy-streams').from;
const QueryStream = require('pg-query-stream')

const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const {ConnectionError, PostgresError} = require('../../common/yadamuError.js')
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');

const sqlGenerateQueries = `select EXPORT_JSON($1,$2)`;

const sqlSystemInformation = `select current_database() database_name,current_user,session_user,current_setting('server_version_num') database_version`;					   

const sqlCreateSavePoint = `SAVEPOINT YadamuInsert`;

const sqlRestoreSavePoint = `ROLLBACK TO SAVEPOINT YadamuInsert`;

const sqlReleaseSavePoint = `RELEASE SAVEPOINT YadamuInsert`;


class PostgresDBI extends YadamuDBI {
    
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
	
	const yadamuLogger = this.yadamuLogger
    const databaseVendor = this.DATABASE_VENDOR

    this.logConnectionProperties();
	let sqlStartTime = performance.now();
	this.pool = new Pool(this.connectionProperties);
    this.traceTiming(sqlStartTime,performance.now())
	
	const self = this
    this.pool.on('error',(err, p) => {
	  // Do not throw errors here.. Node will terminate immediately
	  // const pgErr = new PostgresError(err,self.postgresStack,self.postgressOperation)
      // yadamuLogger.logException([`${databaseVendor}`,`Client.onError()`],pgErr);
      // throw pgErr
    })

  }
  
  async getConnectionFromPool() {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
    }
	
	let stack
	try {
      const sqlStartTime = performance.now();
	  stack = new Error().stack;
	  const connection = await this.pool.connect();
      this.traceTiming(sqlStartTime,performance.now())
      return connection
	} catch (e) {
	  throw new PostgresError(e,stack,'pg.Pool.connect()')
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
      throw new PostgresException(e,stack,operation);
	}
    await configureConnection();
  }
  
  async releaseConnection() {
    if (this.connection !== undefined && this.connection.release) {
	  let stack
      try {
     	stack = new Error().stack;
        await this.connection.release();
      } catch (e) {
		const err = new PostgresError(e,stack,'Client.release()');
        this.yadamuLogger.logException([`${this.constructor.name}.releaseConnection()`],err);
      }
	}
  };
  
  async reconnectImpl() {
      
    // this.yadamuLogger.trace([`${this.constructor.name}.reconnectImpl()`],`Attemping reconnection.`);
       
    /*
    **
    ** Attempt to close the connection. Log but do not throw any errors...
    **
    */	
	
    if (this.connection) {
	  try { 
		await this.connection.release();
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.reconnectImpl()`,`{this.constructor.name}.release()`],e);
      }
    }

    let retryCount = 0;
	let connectionFailure 
    this.connectionOpen = false;
    
    while (retryCount < 10) {
	  let stack
      try {
        this.connection = await this.getConnectionFromPool()
        // this.yadamuLogger.trace([`${this.constructor.name}.reconnectImpl()`],`New connection availabe.`);
        return;
      } catch (e) {
        if (true) {
		  connectionFailure = e;
          this.yadamuLogger.warning([`${this.constructor.name}.reconnectImpl()`],`Waiting for ${this.DATABASE_VENDOR} restart.`)
          await this.waitForRestart(500);
          retryCount++;
        }
        else {
          this.yadamuLogger.logException([`${this.constructor.name}.reconnectImpl()`,`${this.constructor.name}.getConnectionFromPool()`],e);
          throw e;
        }
      }
    }
    // this.yadamuLogger.trace([`${this.constructor.name}.reconnectImpl()`],`Unable to re-establish connection.`)
	throw connectionFailure
  }

  async configureConnection() {
    
	const yadamuLogger = this.yadamuLogger
    const databaseVendor = this.DATABASE_VENDOR
   
    const self = this
    this.connection.on('error',
	  function(err, p) {
        // yadamuLogger.info([`${databaseVendor}`,`Connection.onError()`],err.message);
   	    // Do not throw errors here.. Node will terminate immediately
   	    // const pgErr = new PostgresError(err,self.postgresStack,self.postgressOperation)  
        // throw pgErr
      }
	)

    this.connection.on('notice',
	  function(n){ 
	    const notice = JSON.parse(JSON.stringify(n));
        switch (notice.code) {
          case '42P07': // Table exists on Create Table if not exists
            break;
          case '00000': // Table not found on Drop Table if exists
		    break;
          default:
            yadamuLogger.info([`${this.constructor.name}.onNotice()`],`${n}`);
        }
      }
	)  
  
    const setTimezone = `set timezone to 'UTC'`
	this.executeSQL(setTimezone);
  
    const setFloatPrecision = `set extra_float_digits to 3`
	this.executeSQL(setFloatPrecision);

    const setIntervalFormat =  `SET intervalstyle = 'iso_8601';`;
	this.executeSQL(setIntervalFormat);
  }
  
  /*
  **
  ** Overridden Methods
  **
  */
  
  get DATABASE_VENDOR()    { return 'Postgres' };
  get SOFTWARE_VENDOR()    { return 'The PostgreSQL Global Development Group' };
  get SPATIAL_FORMAT()      { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().postgres }

  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().postgres);
       
    this.pgClient = undefined;
    this.useBinaryJSON = false
    this.activeTransaction = false;
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
	
    let attemptReconnect = !this.reconnectInProgress;

	if ((this.status.sqlTrace) && (typeof sqlStatemeent === 'string')) {
      this.status.sqlTrace.write(this.traceSQL(sqlStatement));
    }

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
        const results = await this.connection.query(sqlStatement,args)
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new PostgresError(e,stack,sqlStatement);
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause)
          continue;
        }
        throw cause
      }      
    } 
	
  }
  
  async initialize() {
    await super.initialize(true);   
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */
 
  async closePool() {
    let stack
	try {
	  stack = new Error().stack
	  if (this.pool !== undefined && this.pool.end) {
	    await this.pool.end();
	  }
	} catch (e) {
	  throw new PostgresError(e,stack,'pg.Pool.close()')
	}
  }
  
  async finalize() {
    await this.releaseConnection();
	await this.closePool();
  } 

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    try {
      await this.releaseConnection();
      await this.closePool();
	} catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}.abort()`],e);
	}
  }


  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     const sqlStatement =  `begin transaction`

     if (this.activeTransaction === false) {
       this.activeTransaction = true;
       await this.executeSQL(sqlStatement);
     }
  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
     const sqlStatement =  `commit transaction`
	 
     if (this.activeTransaction === true) {
       this.activeTransaction = false;
       await this.executeSQL(sqlStatement);
     }
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

    const sqlStatement =  `rollback transaction`
	 
	try {
      if (this.activeTransaction === true) {
        this.activeTransaction = false;
        await this.executeSQL(sqlStatement);
      }
	} catch (e) {
	  if (cause instanceof Error) {
		e = e instanceof PostgresError ? e : new PostgresError(e,stack,sqlStatement)
        this.yadamuLogger.logException([`${this.constructor.name}.rollbackTransaction()`],e)
	    e = cause
	  }
	  throw e;
	}
  }

  async createSavePoint() {

    await this.executeSQL(sqlCreateSavePoint);
  }
  
  async restoreSavePoint(cause) {

    let stack
    try {
      await this.executeSQL(sqlRestoreSavePoint);
	} catch (e) {
      e = e instanceof PostgresError ? e : new PostgresError(e,stack,sqlStatement)
	  if (cause instanceof Error) {
        this.yadamuLogger.logException([`${this.constructor.name}.rollbackTransaction()`],e)
	    e = cause
	  }
	  throw e;
	}
  }  

  async releaseSavePoint(cause) {

    await this.executeSQL(sqlReleaseSavePoint);    
	
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
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(copyStatement))
    }    

    let inputStream = fs.createReadStream(importFilePath);
    const stream = await this.executeSQL(CopyFrom(copyStatement));
    const importProcess = new Promise(async function(resolve,reject) {  
      stream.on('end',function() {resolve()})
  	  stream.on('error',function(err){reject(err)});  	  
      inputStream.pipe(stream);
    })  
    
    const startTime = performance.now();
    await importProcess;
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

  processLog(log) {
    super.processLog(log, this.status, this.yadamuLogger)
    return log
  }

  async processStagingTable(schema) {  	
  	const sqlStatement = `select ${this.useBinaryJSON ? 'import_jsonb' : 'import_json'}(data,$1) from "YADAMU_STAGING"`;
  	var results = await this.executeSQL(sqlStatement,[schema]);
    if (results.rows.length > 0) {
      if (this.useBinaryJSON  === true) {
	    return this.processLog(results.rows[0].import_jsonb);  
      }
      else {
	    return this.processLog(results.rows[0].import_json);  
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

    const sqlStatement  =  `SELECT PostGIS_full_version() "POSTGIS"`;
    
    let postgis = undefined
    
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

  async getSystemInformation(EXPORT_VERSION) {     
  
    const postgisInfo = await this.getPostgisInfo();
   
    const results = await this.executeSQL(sqlSystemInformation)
    const sysInfo = results.rows[0];
	
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,postgisInfo        : postgisInfo
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : EXPORT_VERSION
	 ,sessionUser        : sysInfo.session_user
     ,dbName             : sysInfo.database_name
     ,databaseVersion    : sysInfo.database_version
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
  
  async fetchMetadata(schema) {
    const results = await this.executeSQL(sqlGenerateQueries,[schema,this.spatialFormat]);
    this.metadata = results.rows[0].export_json;
  }
  
  generateTableInfo() {
      
    const tableInfo = Object.keys(this.metadata).map(function(value) {
      return {TABLE_NAME : value, SQL_STATEMENT : this.metadata[value].sqlStatemeent}
    },this)
    return tableInfo;    
    
  }
  
  async getSchemaInfo(schema) {
    await this.fetchMetadata(this.parameters[schema]);
    return this.generateTableInfo();
  }

  generateMetadata(tableInfo,server) {     
    return this.metadata;
  }
   
  generateSelectStatement(tableMetadata) {
     return tableMetadata;
  }   

  createParser(tableInfo,objectMode) {
    return new DBParser(tableInfo,objectMode,this.yadamuLogger);
  }  
  
  forceEndOnInputStreamError(error) {
	return true;
  }
  
  streamingError(e,stack,tableInfo) {
    return new PostgresError(e,stack,tableInfo.SQL_STATEMENT)
  }
  async getInputStream(tableInfo,parser) {        
    const queryStream = new QueryStream(tableInfo.SQL_STATEMENT)
    return await this.executeSQL(queryStream)   
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
  
  async executeDDLImpl(ddl) {
    await this.createSchema(this.parameters.TO_USER);
    await Promise.all(ddl.map(async function(ddlStatement) {
      try {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
        return await this.executeSQL(ddlStatement);
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.executeDDL()`],e)
        this.yadamuLogger.writeDirect(`${ddlStatement}\n`)
      }
    },this))
  }
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator, schema, executeDDL)
  }

  getTableWriter(table) {
    const tableName = this.metadata[table].tableName
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger);      
  }
  
  async insertBatch(sqlStatement,batch) {
    const result = await this.executeSQL(sqlStatement,batch)
    return result;
  }

  async slaveDBI(slaveNumber) {
	const dbi = new PostgresDBI(this.yadamu)
	dbi.setParameters(this.parameters);
	const connection = await this.getConnectionFromPool()
	return await super.slaveDBI(slaveNumber,dbi,connection)
  }
  
  tableWriterFactory(tableName) {
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger)
  }
  
}

module.exports = PostgresDBI
