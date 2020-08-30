"use strict" 
const fs = require('fs');
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const Yadamu = require('../../common/yadamu.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const ExampleConstants = require('./ExampleConstants.js');
const ExampleError = require('./exampleError.js')
const ExampleParser = require('./exampleParser.js');
const ExampleWriter = require('./exampleWriter.js');
const ExampleReader = require('./exampleReader.js');
const StatementGenerator = require('./statementGenerator.js');

class ExampleDBI extends YadamuDBI {
   
  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()                         { return _SQL_SCHEMA_INFORMATION } 
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_BEGIN_TRANSACTION()                          { return _SQL_BEGIN_TRANSACTION }  
  static get SQL_COMMIT_TRANSACTION()                         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()                       { return _SQL_SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()                          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()                         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()                         { return _SQL_RELEASE_SAVE_POINT }
    
  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_VENDOR()        { return ExampleConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return ExampleConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return ExampleConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  get SPATIAL_FORMAT()        { return this.parameters.SPATIAL_FORMAT || ExampleConstants.SPATIAL_FORMAT };

  constructor(yadamu) {
    super(yadamu,ExampleConstants.DEFAULT_PARAMETERS);
  }

  /*
  **
  ** Local methods 
  **
  */
  
  async testConnection(connectionProperties) {   
    // Validate the supplied connection properties
	throw new Error('Unimplemented Method')
  }
  
  async createConnectionPool() {	
    // Create a connection pool.
	throw new Error('Unimplemented Method')
  }
  
  async getConnectionFromPool() {
    // Get a connection from the connection pool
    throw new Error('Unimplemented Method')
  }

  async getConnection() {
    // Get a direct connection to the database bypassing the connection pool.
    throw new Error('Unimplemented Method')
  }
  
  async configureConnection() {    
    // Perform connection specific configuration such as setting sesssion time zone to UTC...
	throw new Error('Unimplemented Method')
  }

  async closeConnection() {
    // Close a connection and return it to the connection pool
	throw new Error('Unimplemented Method')
  }
	
  async closePool() {
    // Close the connection pool
	throw new Error('Unimplemented Method')
  }

  getConnectionProperties() {
	// Convert supplied parameters to format expected by connection mechansim
    return {		
	}
     
  }

  async executeSQL(sqlStatement,args) {
	  
	// Execute the supplied SQL statement binding the specified arguments
	
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

	if ((this.status.sqlTrace) && (typeof sqlStatemeent === 'string')) {
      this.status.sqlTrace.write(this.traceSQL(sqlStatement));
    }

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
        const results = await /* EXECUTE_SQL_STATEMENT */
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.captureException(this.captureException(new ExampleError(e,stack,sqlStatement))
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          continue;
        }
        throw cause
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
	 
	/*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.beginTransaction() is called after the transaction has been started
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
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

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.commitTransaction() is called after the transaction has been committed
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
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

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.rollackTransaction() is called after the transaction has been aborted
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/

	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.
	
    const sqlStatement =  `rollback transaction`
	 
	try {
      super.rollbackTransaction()
      await this.executeSQL(sqlStatement);
	} catch (newIssue) {
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue);								   
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
	 
    await this.executeSQL('create savepoint YADAMU_INSERT);
    super.createSavePoint();
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
      await this.executeSQL(SQL_RESTORE_SAVE_POINT);
      super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVPOINT',cause,newIssue);
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

	super.createStagingTable()
  }
  
  async loadStagingTable(importFilePath) {
	// Process a JSON file that has been uploaded to the server using the database's native JSON capabilities. In most use cases 
	// using client side implementaions are faster, more efficient and can handle much larger files. 
	// The default implementation throws an unsupport feature exception
	super.loadStagingTable()
  }
  
  async uploadFile(importFilePath) {
	// Upload a JSON file to the server so it can be parsed and processed using the database's native JSON capabilities. In most use cases 
	// using client side implementaions are faster, more efficient and can handle much larger files. 
	// The default implementation throws an unsupport feature exception
	super.uploadFile()
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
	    return this.processLog(results.rows[0].import_jsonb,'JSONB_EACH');  
      }
      else {
	    return this.processLog(results.rows[0].import_json,'JSON_EACH');  
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
  
  async getSystemInformation() {     
  
    // Get Information about the target server
	
	const sysInfo = await this.executeSQL('SELECT SYSTEM INFORMATION')
    
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : Yadamu.EXPORT_VERSION
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
	 ,sessionUser        : sysInfo.SESSION_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_VERSION
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
    return await this.executeSQL('GENERATE DDL STATEMENTS')
  }
  
  generateTableInfo() {
    
    // Psuedo Code shown below..
	
    const tableInfo = Object.keys(this.metadata).map((value) => {
      return {TABLE_NAME : value, SQL_STATEMENT : this.metadata[value].sqlStatemeent}
    })
    return tableInfo;    
    
  }
  
  async getSchemaInfo(keyName) {
    
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
          
    return await this.executeSQL('GENERATE METADATA FROM SCHEMA',this.parameters[keyName])
  }
   
  generateSelectStatement(tableMetadata) {
     return tableMetadata;
  }   

  createParser(tableInfo) {
    return new ExampleParser(tableInfo,this.yadamuLogger);
  }  
  
  streamingError(e,sqlStatement) {
    return this.captureException(new ExampleError(e,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(tableInfo) {

    // Either return the databases native readable stream or use the ExampleReader to create a class that wraps a cursor or event stream in a Readable

    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],tableInfo.TABLE_NAME)
    this.streamingStackTrace = new Error().stack;
    return new ExampleReader(this.connection,tableInfo.SQL_STATEMENT);
	
  }  
  
  
    throw new Error('Unimplemented Method')
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
   
  async createSchema(schema) {
	// Create a schema 
    throw new Error('Unimplemented Method')
  }
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator, schema, executeDDL)
  }

  getOutputStream(tableName,ddlComplete) {
	 // Get an instance of the YadamuWriter implementation associated for this database
	 return super.getOutputStream(ExampleWriter,tableName,ddlComplete)
  }

  classFactory(yadamu) {
	// Create a worker DBI that has it's own connection to the database (eg can begin and end transactions of it's own. 
	// Ideally the connection should come from the same connection pool that provided the connection to this DBI.
	return new ExampleDBI(yadamu)
  }
  
  async getConnectionID() {
	// Get a uniqueID for the current connection
    throw new Error('Unimplemented Method')
  }
	  
}

module.exports = ExampleDBI
