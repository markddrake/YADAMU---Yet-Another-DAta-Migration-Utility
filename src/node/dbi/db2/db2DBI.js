
// ### TODO Support BSON and JSON Storage
// ### TODO Detect if spatial extended is installed
// ### TODO Support spatial data formats when spatial extender is installed
// ### TODO Enhance wrapping of JSON where the top level node is not an Object when storing BSON.
// ### TODO Unwrap wrapped JSON with BOSN

import fs                             from 'fs';

import { 
  performance 
}                                     from 'perf_hooks';
				
import {
  setTimeout
}                                     from "timers/promises"
				
/* Database Vendors API */                                    

import ibmdb                          from 'ibm_db'
const {Pool} = ibmdb;

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

import {
  CopyOperationAborted,
  UnimplementedMethod
}                                     from '../../core/yadamuException.js'

/* Yadamu DBI */                                    

import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                   

import DB2Constants                   from './db2Constants.js'
import DB2DataTypes                   from './db2DataTypes.js'
import DB2Error                       from './db2Exception.js'
import DB2Parser                      from './db2Parser.js'
import DB2Writer                      from './db2Writer.js'
import DB2OutputManager               from './db2OutputManager.js'
import DB2StatementLibrary            from './db2StatementLibrary.js'
import DB2StatementGenerator     from './db2StatementGenerator.js'

class DB2DBI extends YadamuDBI {
    
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.DBI_PARAMETERS,DB2Constants.DBI_PARAMETERS))
	return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return DB2DBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_KEY()           { return DB2Constants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return DB2Constants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return DB2Constants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return DB2Constants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.LOADER_STAGING }
  
  get DB2GSE_INSTALLED()              { return false }

  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
	this.DATA_TYPES = DB2DataTypes
  }

  /*
  **
  ** Local methods 
  **
  */
  
  async testConnection(connectionProperties) {   
    // Validate the supplied connection properties
    throw new UnimplementedMethod('testConnection()',`YadamuDBI`,this.constructor.name)
  }
  
  async createConnectionPool() {	
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],'Creating Pool')
	this.connectionPool = new Pool()
  }
  
  createConnectionString() {
    return `DATABASE=${this.vendorProperties.database};HOSTNAME=${this.vendorProperties.hostname};PORT=${this.vendorProperties.port || 50000};PROTOCOL=TCPIP;UID=${this.vendorProperties.username};PWD=${this.vendorProperties.password}`
  }
  
  async badConnection() {
     
	 const stack = new Error().stack

	 try {
	   const results = await this.connection.query(`select 1 from sysibm.sysdummy1`)
	   return false
	 } catch(e) {
	   const cause = new DB2Error(this.DRIVER_ID,e,stack,'DB2.Pool.badConnection()')
	   if (cause.lostConnection()) {
		 return true
	   }
	   throw cause
	 }
  }
  
  async _reconnect() {

    this.connection = await (this.isManager() ? this.getConnectionFromPool() : this.manager.getConnectionFromPool())
	
	
  }
  
  async getConnectionFromPool() {
    
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`getConnectionFromPool()`)
    
	// Get a connection from the connection pool
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    while (true) {

      this.SQL_TRACE.comment(`Gettting Connection From Pool.`)
      const sqlStartTime = performance.now()
	  try  {
        const stack = new Error().stack
	    const connection = await new Promise((resolve,reject) => {
          this.connectionPool.open(this.createConnectionString(),(err,conn) => {
	        if (err) reject(new DB2Error(this.DRIVER_ID,err,stack,'DB2.Pool.getConnection()'))
	        resolve(conn)
	      })
	    })
	    return connection
 	  } catch(cause) {
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
		  continue;
        }
        throw this.trackExceptions(cause)
	  }
    }
  }

  async getConnection() {
    // Get a direct connection to the database bypassing the connection pool.
    throw new UnimplementedMethod('getConnection()',`YadamuDBI`,this.constructor.name)
  }
  
  async configureConnection() {    

    let result = await this.executeSQL(DB2StatementLibrary.SQL_CONFIGURE_CONNECTION)

    this._DATABASE_VERSION = result[0].DATABASE_VERSION.substring(5)
    // Perform connection specific configuration such as setting sesssion time zone to UTC...
  }

  async closeConnection(options) {

    // Close a connection and return it to the connection pool

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined && (typeof this.connection.close === 'function'))})`)
	if (await this.badConnection()) {
	  // Do not return failed connections to the pool.
      await this.connection.realClose()
    }
	
    await this.connection.close()
  }
	
  async closePool(options) {

    // Close the connection pool

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],`closePool()`)
    await this.connectionPool.close()

 }

  async executeSQL(sqlStatement,args) {
	  
	// Execute the supplied SQL statement binding the specified arguments
	
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

	if (typeof sqlStatement === 'string') {
      this.SQL_TRACE.traceSQL(sqlStatement)
    }

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
		stack = new Error().stack
        const results = await this.connection.query(sqlStatement,args)
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.trackExceptions(new DB2Error(this.DRIVER_ID,e,stack,sqlStatement))
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if itYAD cannot reconnect...
          await this.reconnect(cause,'SQL')
          continue;
        }
        throw cause
      }      
    } 
	
  }
  
  async batchInsert(batch) {
	  
	// Execute the supplied SQL statement binding the specified arguments
	
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    this.SQL_TRACE.traceSQL(batch.sql)

    let stack
    const timerAbort = new AbortController()

    while (true) {
      // Exit with result or exception.  
      try {

        /*
        setTimeout(10000,null,{ref: false, signal: timerAbort.signal}).then(() => {
          this.yadamuLogger.warning([this.DATABASE_VENDOR,this.ROLE,,'BATCH INSERT'],`Batch Insert operation timed out`)
		  return
	    }).catch((e) => { console.log(e) })
	 	*/
		
        const sqlStartTime = performance.now()
		stack = new Error().stack
        const results = await this.connection.query(batch)
		timerAbort.abort()
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		timerAbort.abort()
		const cause = this.trackExceptions(new DB2Error(this.DRIVER_ID,e,stack,batch.sql))
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if itYAD cannot reconnect...
          await this.reconnect(cause,'SQL')
          continue;
        }
        throw cause
      }      
    } 
	
  }
  
  /*
  **
  **  Open the Database connection 
  **
  
  async initialize() {
    await super.initialize(true)   
  }
   
  */   
  
  /*
  **
  **  Gracefully close down the database connection and pool.
  **
  
  async final() {
	await super.final()
  } 

  */

  /*
  **
  **  Abort the database connection and pool.
  **
   
  async destroy(e) {
    await super.destroy(e)
  }

  */

  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
	 
	/*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.beginTransaction() is called after the transaction has been started
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
    await this.connection.beginTransaction()
	super.beginTransaction()

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.commitTransaction() is called after the transaction has been committed
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
	super.commitTransaction()
    await this.connection.commitTransaction()
	
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

   // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.rollackTransaction() is called after the transaction has been aborted
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/

	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.
	
     
	try {
      super.rollbackTransaction()
      await this.connection.rollbackTransaction()
	} catch (newIssue) {
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)								   
	}
  }

  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
																
    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.createSavePoint() is called after the save point has been created
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	 
    await this.executeSQL(DB2StatementLibrary.SQL_CREATE_SAVE_POINT)
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)
																 
    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.restoreSavePoint() is called after reverting to the save point
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	 

    this.checkConnectionState(cause)
	 
	
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
		
    let stack
    try {
      await this.executeSQL(DB2StatementLibrary.SQL_RESTORE_SAVE_POINT)
      super.restoreSavePoint()
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVPOINT',cause,newIssue)
	}
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
    await super.createStagingTable()
  }
  
  async loadStagingTable(importFilePath) {
	// Process a JSON file that has been uploaded to the server using the database's native JSON capabilities. In most use cases 
	// using client side implementaions are faster, more efficient and can handle much larger files. 
	// The default implementation throws an unsupport feature exception
	await super.loadStagingTable()
  }
  
  async uploadFile(importFilePath) {
	// Upload a JSON file to the server so it can be parsed and processed using the database's native JSON capabilities. In most use cases 
	// using client side implementaions are faster, more efficient and can handle much larger files. 
	// The default implementation throws an unsupport feature exception

    await super.uploadFile()
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
  
    // Get Information about the target server
	
	const sysInfo = await this.executeSQL(DB2StatementLibrary.SQL_SYSTEM_INFORMATION)
    return Object.assign(
	  super.getSystemInformation()
	, sysInfo[0]
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
  
  generateTableInfo() {
    
    // Psuedo Code shown below..
	
    const queryInfo = Object.keys(this.metadata).map((value) => {
      return {TABLE_NAME : value, SQL_STATEMENT : this.metadata[value].sqlStatemeent}
    })
    return queryInfo;    
    
  }
  
  generateMetadata(schemaInformation) {   

    schemaInformation.forEach((table,idx) => {
      table.EXTENDED_TYPE_ARRAY  = typeof table.EXTENDED_TYPE_ARRAY  === 'string' ? JSON.parse(table.EXTENDED_TYPE_ARRAY)  : table.EXTENDED_TYPE_ARRAY
    })

    const metadata = super.generateMetadata(schemaInformation) 

    return metadata	

  }  
    
  async getSchemaMetadata() {
    
    /*
    ** Returns an array of information about each table in the schema being exported.
    **
    ** The following item are mandatory, since they are required to build the "metadata" object that forms part of the YADAMU export file 
    ** and which is used as the starting point when for database to database copy operations.
    ** 
    ** TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME_ARRAY, DATA_TYPE_ARRAY, SIZE_CONSTRAINT_ARRAY
    **
    ** The Arrays are expected to be valid JSON arrays.
    **
    ** The query may also return additional information about the SQL that should be used to retieve the data from the schema
    **
    ** Implimentations should provde a custom impliemtnation of generateMetadata() if they need more than the minimum set of information about the schema.
    **
    */
          
    return await this.executeSQL(DB2StatementLibrary.SQL_SCHEMA_METADATA,[this.CURRENT_SCHEMA])
  }
   
  generateSQLQuery(tableMetadata) {
     const queryInfo = super.generateSQLQuery(tableMetadata)
     return queryInfo;
  }   

  createParser(queryInfo,parseDelay) {
    return new DB2Parser(this,queryInfo,this.yadamuLogger,parseDelay)
  }  
  
  inputStreamError(cause,sqlStatement) {
    return this.trackExceptions(((cause instanceof DB2Error) || (cause instanceof CopyOperationAborted)) ? cause : new DB2Error(this.DRIVER_ID,cause,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(queryInfo) {

    // Either return the databases native readable stream or use the DB2Reader to create a class that wraps a cursor or event stream in a Readable

    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],queryInfo.TABLE_NAME)

    this.streamingStackTrace = new Error().stack;
    this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
	return this.connection.queryStream(queryInfo.SQL_STATEMENT)
	
  }  
    
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
   
  async createSchema(schema) {
	await this.executeSQL(`create schema "${schema}"`)
  }
  
  async generateStatementCache(schema) {
    return await super.generateStatementCache(DB2StatementGenerator, schema)
  }

  getOutputStream(tableName,metrics) {
	 // Get an instance of the YadamuWriter implementation associated for this database
	 return super.getOutputStream(DB2Writer,tableName,metrics)
  }

  getOutputManager(tableName,metrics) {
	 // Get an instance of the YadamuWriter implementation associated for this database
	 return super.getOutputStream(DB2OutputManager,tableName,metrics)
  }

  classFactory(yadamu) {
	// Create a worker DBI that has it's own connection to the database (eg can begin and end transactions of it's own. 
	// Ideally the connection should come from the same connection pool that provided the connection to this DBI.
	return new DB2DBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
	// Get a uniqueID for the current connection
    throw new Error('Unimplemented Method')
  }
	  
}

export { DB2DBI as default }
