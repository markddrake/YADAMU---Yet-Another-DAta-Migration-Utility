
import fs                             from 'fs';

import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    
		
/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    

import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                   

import Comparitor                     from './exampleCompare.js'
import DatabaseError                  from './exampleException.js'
import DataTypes                      from './exampleDataTypes.js'
import Parser                         from './exampleParser.js'
import StatementGenerator             from './exampleStatementGenerator.js'
import StatementLibrary               from './exampleStatementLibrary.js'
import OutputManager                  from './exampleOutputManager.js'
import Writer                         from './exampleWriter.js'

import ExampleConstants               from './exampleConstants.js'
import ExampleReader                  from './exampleReader.js'

class ExampleDBI extends YadamuDBI {
       
  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...ExampleConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return ExampleDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_KEY()           { return ExampleConstants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return ExampleConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return ExampleConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return ExampleConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.LOADER_STAGING }

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

  }

  /*
  **
  ** Local methods 
  **
  */
  
  async testConnection() {   
    // Validate the supplied connection properties
    throw new UnimplementedMethod('testConnection()',`YadamuDBI`,this.constructor.name)
  }
  
  async createConnectionPool() {	
    // Create a connection pool.
    throw new UnimplementedMethod('createConnectionPool()',`YadamuDBI`,this.constructor.name)
  }
  
  async getConnectionFromPool() {
    // Get a connection from the connection pool
    throw new UnimplementedMethod('getConnectionFromPool()',`YadamuDBI`,this.constructor.name)
  }

  async getConnection() {
    // Get a direct connection to the database bypassing the connection pool.
    throw new UnimplementedMethod('getConnection()',`YadamuDBI`,this.constructor.name)
  }
  
  async configureConnection() {    
    // Perform connection specific configuration such as setting sesssion time zone to UTC...
    throw new UnimplementedMethod('configureConnection()',`YadamuDBI`,this.constructor.name)
  }

  async closeConnection(options) {
    // Close a connection and return it to the connection pool
    throw new UnimplementedMethod('closeConnection()',`YadamuDBI`,this.constructor.name)
  }
	
  async closePool(options) {
    // Close the connection pool
    throw new UnimplementedMethod('closePool()',`YadamuDBI`,this.constructor.name)
 }

  async executeSQL(sqlStatement,args) {
	  
	// Execute the supplied SQL statement binding the specified arguments
	
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

	if (typeof sqlStatement === 'string') {
      this.SQL_TRACE.traceSQL(sqlStatement)
    }

    while (true) {
      // Exit with result or exception.  
      let stack
      try {
		stack = new Error().stack
        const sqlStartTime = performance.now()
        const results = await this.connection.executeSQL(sqlStatemeent) /* EXECUTE_SQL_STATEMENT */
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,sqlStatement)
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

    // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
	 
	/*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.beginTransaction() is called after the transaction has been started
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
    await this.executeSQL(ExampleStatementLibrary.SQL_BEGIN_TRANSACTION)
	super.beginTransaction()

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.LOGGER.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.commitTransaction() is called after the transaction has been committed
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	
	super.commitTransaction()
    await this.executeSQL(ExampleStatementLibrary.SQL_COMMIT_TRANSACTION)
	
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

   // this.LOGGER.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber()],``)

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
      await this.executeSQL(ExampleStatementLibrary.SQL_ROLLBACK_TRANSACTION)
	} catch (newIssue) {
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)								   
	}
  }

  async createSavePoint() {

    // this.LOGGER.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
																
    /*
	**
	** Sample implementaion show below assuming transaction is managed using SQL. Note that super.createSavePoint() is called after the save point has been created
	** to perform housing keeping operations related to driver state and correctly tracking number of rows that have been processed.
	**
	*/
	 
    await this.executeSQL(ExampleStatementLibrary.SQL_CREATE_SAVE_POINT)
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)
																 
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
      await this.executeSQL(ExampleStatementLibrary.SQL_RESTORE_SAVE_POINT)
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
    super.processLog(log, operation, this.status, this.LOGGER)
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
	
	const sysInfo = await this.executeSQL(ExampleStatementLibrary.SQL_SELECT_SYSTEM_INFORMATION)
    
	return Object.assign(
	  super.getSystemInformation()
	, {}
    )
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    return await this.executeSQL(ExampleStatementLibrary.SQL_GENERATE_STATEMENTS,this.CURRENT_SCHEMA)
  }
  
  generateTableInfo() {
    
    // Psuedo Code shown below..
	
    const queryInfo = Object.keys(this.metadata).map((value) => {
      return {TABLE_NAME : value, SQL_STATEMENT : this.metadata[value].sqlStatemeent}
    })
    return queryInfo;    
    
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
          
    return await this.executeSQL(DB2StatementLibrary.SQL_SCHEMA_METADATA,this.CURRENT_SCHEMA)
  }
   
  generateSQLQuery(tableMetadata) {
     const queryInfo = super.generateSQLQuery(tableMetadata)
     return queryInfo;
  }   

  async _getInputStream(queryInfo) {

    // Either return the databases native readable stream or use the ExampleReader to create a class that wraps a cursor or event stream in a Readable

    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],queryInfo.TABLE_NAME)

	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
		stack = new Error().stack
        const sqlStartTime = performance.now()
		const is = new ExampleReader(this.connection,queryInfo.SQL_STATEMENT)
	    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return is;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,sqlStatement)
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
    
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
   
  async createSchema(schema) {
	// Create a schema 
    throw new UnimplementedMethod('createSchema()',`YadamuDBI`,this.constructor.name)
  }
  
  classFactory(yadamu) {
	// Create a worker DBI that has it's own connection to the database (eg can begin and end transactions of it's own. 
	// Ideally the connection should come from the same connection pool that provided the connection to this DBI.
	return new ExampleDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
	// Get a uniqueID for the current connection
    throw new Error('Unimplemented Method')
  }
	  
}

export { ExampleDBI as default }
