
"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const Yadamu = require('../../common/yadamu.js');
const YadamuDBI =  require('../../common/yadamuDBI.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class DBInterface extends YadamuDBI {
    
  async testConnection(connectionProperties) {   
    super.setConnectionProperties(connectionProperties);
	// ### Test Database connection
  }
  
  
  async createConnectionPool() {
	// ### Create a connection pool
  }
	  
  async getConnectionFromPool() {
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getSlaveNumber()],`getConnectionFromPool()`)
	// ### Get a Database connection from the connection pool
  }
  
  async getConnection() {
	// ###  Get a Database connection directly from the database (E.G. not using the pool)
  }
  
  async closeConnection() {
	/// ### Release connection and return to pool
  }
  
  async closePool() {
	// ### Close database connection pool
  }
  
  async reconnectImpl() {
	 // Override default which returns unsupported
      this.connection = this.isMaster() ? await this.getConnectionFromPool() : await this.connectionProvider.getConnectionFromPool()
  }
  
  async configureConnection() {
	### Configure the new connection here. Performed for every connection 
  }
  
  
  isValidDDL() {
	// ### Override super.isValudDDL() which returns false
    return (this.systemInformation.vendor === this.DATABASE_VENDOR)
  }
  
  objectMode() {
	// ### Override super.isValudDDL() which returns false  
    return true;
  }

  get DATABASE_VENDOR()    { return ### };
  get SOFTWARE_VENDOR()    { return ### };
  get SPATIAL_FORMAT()     { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().databaseId }
  
  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().databaseId)
    this.connection = undefined;
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize() {
    await super.initialize(true);     
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
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
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getSlaveNumber()],``)
     // ### BEGIN TRANSACTION 
	 super.beginTransaction();
							
  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getSlaveNumber()],``)
	// ### COMMIT TRANSACTION 
	super.commitTransaction()
															 
  }
	
  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

   // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getSlaveNumber()],``)

    this.checkConnectionState(cause)

	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.
		 
	try {
	  // ### ROLLBACK TRANSACTION 
      super.rollbackTransaction()
	} catch (newIssue) {
	  // ### Create Database specific Exception ???
	  this.checkCause(cause,newIssue);										   
	}
  }

  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getSlaveNumber()],``)
    // ### CREATE SAVEPOINT
    super.createSavePoint();
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getSlaveNumber()],``)
																 
    this.checkConnectionState(cause)
	 
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
		
    let stack
    try {
    // ### ROLLBACK TO SAVEPOINT
      super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause(cause,newIssue);
	}
  }  

  async releaseSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.releaseSavePoint()`,this.getSlaveNumber()],``)
    // ### RELEASE SAVE POINT   
    super.releaseSavePoint();

  } 
  
  async uploadFile(importFilePath) {
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  async processFile(hndl) {
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

    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : ### Database Session Timezone
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : this.EXPORT_VERSION
	 ,sessionUser        : ### Database Session User
     ,dbName             : ### Database Name
     ,databaseVersion    : ### Database Version
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,nodeClient         : {
        version          : process.version
       ,architecture     : process.arch
       ,platform         : process.platform
      }      
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
  
  async getSchemaInfo(schema) {
    return []
  }

  generateMetadata(tableInfo,server) {    
    return {}
  }
   
  
  async getInputStream(tableInfo,parser) {
  }      
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL) 
  }
  
  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)
  }

  async createSchema(schema) {  
    // ### Create a Database Schema or equivilant
  }
  
  async executeDDLImpl(ddl) {
    // ### Execute a DDL operation
  }
  
  tableWriterFactory(tableName) {
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger)
  }


  async slaveDBI(slaveNumber) {
	const dbi = new ###DatabaseInterface(this.yadamu)
	return await super.slaveDBI(slaveNumber,dbi)
  }
  

  async getConnectionID() {
	// ### Get a Unique ID for the connection
	return pid
  }
}

module.exports = DBInterface
