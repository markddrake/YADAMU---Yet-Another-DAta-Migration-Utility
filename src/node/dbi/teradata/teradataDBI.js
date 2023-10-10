
import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    
							          
// import TeradataConnection from "teradata-nodejs-driver/teradata-connection";
// import TeradataExceptions from "teradata-nodejs-driver/teradata-exceptions";

/*
**
** Teradata Implementation Notes
**
**    Spatial Data Types : Use JSON
**    Interval Data Types: Use VARCHAR
**    LOB Support: VARCHAR and BINARY are supported to 16Mb
**
*/

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

import {
  UnimplementedMethod
}                                    from '../../core/yadamuException.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                   
						          
import TeradataConstants              from './teradataConstants.js'
import TeradataDataTypes              from './teradataDataTypes.js'
import TeradataReader                 from './teradataReader.js'
import TeradataParser                 from './teradataParser.js'
import TeradataOutputManager          from './teradataOutputManager.js'
import TeradataWriter                 from './teradataWriter.js'
import TeradataStatementGenerator     from './teradataStatementGenerator.js'
import TeradataStatementLibrary       from './teradataStatementLibrary.js'
import TeradataCompare                from './teradataCompare.js'

import { 
  TeradataError 
}                                     from './teradataException.js'

const MAX_CHARATER_SIZE = 64000


class TeradataDBI extends YadamuDBI {
     
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.DBI_PARAMETERS,TeradataConstants.DBI_PARAMETERS))
	return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return TeradataDBI.DBI_PARAMETERS
  }

  get DATABASE_KEY()              { return TeradataConstants.DATABASE_KEY};
  get DATABASE_VENDOR()           { return TeradataConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()           { return TeradataConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()      { return TeradataConstants.STATEMENT_TERMINATOR };

  get DATATYPE_IDENTITY_MAPPING() { return false }
  get FETCH_SIZE()                { return this.parameters.TERDATA_FETCH_SIZE || TeradataConstants.FETCH_SIZE}

  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
	this.DATA_TYPES = TeradataDataTypes

	this.DATA_TYPES.storageOptions.BOOLEAN_TYPE = this.parameters.TERADATA_BOOLEAN_STORAGE_OPTION || this.DBI_PARAMETERS.BOOLEAN_STORAGE_OPTION || this.DATA_TYPES.storageOptions.BOOLEAN_TYPE

    this.connection = undefined;
  }
  
  /*
  **
  ** Local methods 
  **
  */
  
  establishConnection(connection) {
      
    return new Promise((resolve,reject) => {
      connection.connect((err,connection) => {
        if (err) {
          reject(this.trackExceptions(new TeradataError(this.DRIVER_ID,err,`Teradata-sdk.Connection.connect()`)))
        }
        resolve(connection)
      })
    })
  } 

  async testConnection() {   
    this.setDatabase()
    try {
      let connection = Teradata.createConnection(this.vendorProperties)
      connection = await this.establishConnection(connection)
      connection.destroy()
    } catch (e) {
      throw this.trackExceptions(new TeradataError(this.DRIVER_ID,e,'Teradata-SDK.connection.connect()'))
    }
  }
  
  async createConnectionPool() { 
    // Teradata does not support connection pooling
  }

  async getConnectionFromPool() {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)
    
    //  Do not Configure Connection here. 
    
	let operation
    this.SQL_TRACE.comment(`Gettting Connection.`)
    try {
      const sqlStartTime = performance.now()
      this.logConnectionProperties()
	  operation = 'TeradataConnection.TeradataConnection()';
      const connection = new TeradataConnection.TeradataConnection()
	  operation = 'TeradataConnection.TeradataConnection().connect()';      
      connection.connect(this.vendorProperties)
	  operation = 'TeradataConnection.TeradataConnection().cursor()';      
      this.cursor = connection.cursor()
	  this.readCursor = this.cursor
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return connection;
    } catch (e) {
      const err = this.trackExceptions(new TeradataError(this.DRIVER_ID,e,'TeradataConnection.connect()'))
      throw err;
    }
    
  }
  
  async getConnection() {
    // Get a direct connection to the database bypassing the connection pool.
    throw new UnimplementedMethod('getConnection()',`YadamuDBI`,this.constructor.name)
  }
  
  async configureConnection() {    
    
	const results = await this.executeSQL(TeradataStatementLibrary.SQL_SYSTEM_INFORMATION,[])
	this._DATABASE_VERSION = results[0][0].slice('Database Version'.length+1)
  }

  async closeConnection(options) {
      
    // this.LOGGER.trace([this.DATABASE_VENDOR,this.getSlaveNumber()],`closeConnection(${(this.connection !== undefined && this.connection.destroy)})`)
      
    if (this.connection !== undefined && this.connection.destroy) {
      await this.connection.destroy()
    }
    
  }
    
  async closePool(options) {
    // Teradata-SDK does not support connection pooling
  }
  
  updateVendorProperties(vendorProperties) {

    vendorProperties.host              = this.parameters.HOSTNAME || vendorProperties.host      || vendorProperties.host
    vendorProperties.user              = this.parameters.USERNAME || vendorProperties.user      || vendorProperties.user
    vendorProperties.password          = this.parameters.PASSWORD || vendorProperties.password  || vendorProperties.password       
    vendorProperties.log               = "0"
	vendorProperties.teradata_values   = "false"

  }
  	  
  async _executeDDL(ddl) {
	
	// Override to use for of rather than map
	
	let results =[]
	const startTime = performance.now()
	try {
      for (let ddlStatement of ddl) {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA)
		results.push(await this.executeSQL(ddlStatement,{}))
      }
    } catch (e) {
      console.log(e)
	 const exceptionFile = this.LOGGER.handleException([this.DATABASE_VENDOR,'DDL'],e)
	 await this.LOGGER.writeMetadata(exceptionFile,this.yadamu,this.systemInformation,this.metadata)
	 results = e;
    }
    return results;
  }

  async executeSQL(sqlStatement, args) {
          
    this.SQL_TRACE.comment(sqlStatement + JSON.stringify(args))
    try {
      const sqlStartTime = performance.now()
	  this.cursor.execute(sqlStatement,args)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return this.cursor;
    } catch (e) {
      throw err;
    }
    
  }   
   
  async callProcedure(procedureName, args) {
          
    this.SQL_TRACE.comment(procedureName + JSON.stringify(args))
    try {
      const sqlStartTime = performance.now()
      const cursor = this.connection.cursor()
      cursor.callproc(procedureName,args)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return cursor;
    } catch (e) {
      const err = this.trackExceptions(new TeradataError(this.DRIVER_ID,e,`terdata.cursor.callProc(${procedureName})`))
      throw err;
    }
    
  }   
  
  setDatabase() {  
    if ((this.parameters.Teradata_SCHEMA_DB) && (this.parameters.Teradata_SCHEMA_DB !== this.vendorProperties.database)) {
      this.vendorProperties.database = this.parameters.Teradata_SCHEMA_DB
    }
  }  
  
  async initialize() {
    await super.initialize(true)   
	this.spatialFormat = "WKTB"
  }
    
  /*
  **
  ** Begin a transaction
  **
  */

  setTransactionCursor() {

  	 this.cursor = this.connection.cursor()

  }

  resetTransactionCursor() {
	  
	this.cursor.close()
	this.cursor = this.readCursor;
	
  }
  
  async beginTransaction() {

    // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
     
    this.setTransactionCursor()
    await this.executeSQL(TeradataStatementLibrary.SQL_BEGIN_TRANSACTION,[])
    await super.beginTransaction()

  }

  /*
  **
  ** Commit the current transaction
  **
  */
   
  async commitTransaction() {
	  
    // this.LOGGER.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    super.commitTransaction()
    await this.executeSQL(TeradataStatementLibrary.SQL_COMMIT_TRANSACTION,[])
    this.resetTransactionCursor()
	
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
      await this.executeSQL(TeradataStatementLibrary.SQL_ROLLBACK_TRANSACTION,[])
      this.resetTransactionCursor()
    } catch (newIssue) {
      this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)                                  
    }
  }

  async createSavePoint() {
    throw new UnimplementedMethod('createSavePoint()',`YadamuDBI`,this.constructor.name)
  }
  
  async restoreSavePoint(cause) {
    throw new UnimplementedMethod('restoreSavePoint()',`YadamuDBI`,this.constructor.name)
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
   
    const results = await this.executeSQL(TeradataStatementLibrary.SQL_SYSTEM_INFORMATION,[])
    const sysInfo = results[0]

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
    return undefined
  }
  
  async getSchemaMetadata() {

    let schemaInfo = await this.executeSQL(TeradataStatementLibrary.SQL_SCHEMA_INFORMATION,[this.CURRENT_SCHEMA])
	schemaInfo = schemaInfo.map((table) => {
	  return {
	    TABLE_SCHEMA          : table[0]
	  , TABLE_NAME            : table[1]
	  , COLUMN_NAME_ARRAY     : table[2]
	  , DATA_TYPE_ARRAY       : table[3]
	  , SIZE_CONSTRAINT_ARRAY : table[4]
	  , CLIENT_SELECT_LIST    : table[5]
	  }
    })
    return schemaInfo
  }

  createParser(queryInfo,parseDelay) {
    return new TeradataParser(this,queryInfo,this.LOGGER,parseDelay)
  }  
  
  inputStreamError(cause,sqlStatement) {
    return this.trackExceptions(((cause instanceof TeradataError) || (cause instanceof CopyOperationAborted)) ? cause : new TeradataError(this.DRIVER_ID,cause,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(queryInfo) {
    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],queryInfo.TABLE_NAME)
    this.streamingStackTrace = new Error().stack;
	try {
      return new TeradataReader(this.connection.cursor(),queryInfo.SQL_STATEMENT)
	} catch (e) {
	  throw this.trackExceptions(new TeradataError(this.DRIVER_ID,e,this.streamingStackTrace,queryInfo.SQL_STATEMENT))
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
   
  async generateStatementCache(schema) {
    return await super.generateStatementCache(TeradataStatementGenerator,schema) 
  }
 
  getOutputStream(tableName,metrics) {
     // Get an instance of the YadamuWriter implementation associated for this database
     return super.getOutputStream(TeradataWriter,tableName,metrics)
  }

  getOutputManager(tableName,metrics) {
     // Get an instance of the YadamuWriter implementation associated for this database
     return super.getOutputStream(TeradataOutputManager,tableName,metrics)
  }
  
  classFactory(yadamu) {
	return new TeradataDBI(yadamu,this,this.connectionParameters,this.parameters)
  }

  async getComparator(configuration) {
	 await this.initialize()	 
	 return new TeradataCompare(this,configuration)
  }
  
}
 
export { TeradataDBI as default }
