"use strict" 

const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const mariadb = require('mariadb');

const Yadamu = require('../../common/yadamu.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const MariadbConstants = require('./mariadbConstants.js')
const MariadbError = require('./mariadbError.js')
const MariadbParser = require('./mariadbParser.js');
const MariadbWriter = require('./mariadbWriter.js');
const StatementGenerator = require('../../dbShared/mysql/statementGenerator57.js');

class MariadbDBI extends YadamuDBI {
    
  // Until we have static constants

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_CREATE_SAVE_POINT()                          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()                         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()                         { return _SQL_RELEASE_SAVE_POINT }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_VENDOR()            { return MariadbConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()            { return MariadbConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()       { return MariadbConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  get SPATIAL_FORMAT()             { return this.parameters.SPATIAL_FORMAT            || MariadbConstants.SPATIAL_FORMAT }
  get TABLE_MATCHING()             { return this.parameters.TABLE_MATCHING            || MySQLDBI.TABLE_MATCHING}
  get TREAT_TINYINT1_AS_BOOLEAN()  { return this.parameters.TREAT_TINYINT1_AS_BOOLEAN || MariadbConstants.TREAT_TINYINT1_AS_BOOLEAN }
  
  constructor(yadamu) {

    super(yadamu,MariadbConstants.DEFAULT_PARAMETERS);
    this.pool = undefined;
  }
  
  async testConnection(connectionProperties,parameters) {   
    try {
	  this.setConnectionProperties(connectionProperties);
      this.connection = await mariadb.createConnection(this.connectionProperties);
	  await this.connection.end();
	  super.setParameters(parameters)
	} catch (e) {
	  throw (e)
	} 
  }	
	     
  async configureConnection() {  

    await this.executeSQL(MariadbDBI.SQL_CONFIGURE_CONNECTION);
 
  }


  async checkMaxAllowedPacketSize() {
	  
	this.connection = await this.getConnectionFromPool()

    const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
    const sqlQueryPacketSize = `SELECT @@max_allowed_packet`;
    const sqlSetPacketSize = `SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`
    
    let results = await this.executeSQL(sqlQueryPacketSize);
    
    if (parseInt(results[0]['@@max_allowed_packet']) <  maxAllowedPacketSize) {
      this.yadamuLogger.info([`${this.constructor.name}.setMaxAllowedPacketSize()`],`Increasing MAX_ALLOWED_PACKET to 1G.`);
      results = await this.executeSQL(sqlSetPacketSize);
    }    
	
	await this.closeConnection();
	
  }
  
  async createConnectionPool() {
    this.logConnectionProperties();
	let sqlStartTime = performance.now();
	this.pool = mariadb.createPool(this.connectionProperties);
	this.traceTiming(sqlStartTime,performance.now())
    await this.checkMaxAllowedPacketSize();
  }
  
  async getConnectionFromPool() {

    let stack
    this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
	try {
      const sqlStartTime = performance.now();
	  stack = new Error().stack
	  const connection = await this.pool.getConnection();
	  this.traceTiming(sqlStartTime,performance.now())
	  connection.ping()
      return connection
	} catch (e) {
	  throw this.captureException(new MariadbError(e,stack,'mariadb.Pool.getConnection()'))
	}
  }

  async closeConnection() {
	  
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined && this.connection.end)})`)
	  
    if (this.connection !== undefined && this.connection.end) {
      let stack;
      try {
        stack = new Error().stack
        await this.connection.end();
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
  	    throw this.captureException(new MariadbError(e,stack,'Mariadb.Connection.end()'))
	  }
	}
  };
   
  async closePool() {
	  
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool !== undefined && this.pool.end)})`)
	  
    if (this.pool !== undefined && this.pool.end) {
      let stack;
      try {
        stack = new Error().stack
        await this.pool.end();
        this.pool = undefined;
      } catch (e) {
        this.pool = undefined;
  	    throw this.captureException(new MariadbError(e,stack,'Mariadb.Pool.end()'))
	  }
	}
	
  };
   
  async _reconnect() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
  }

  async createSchema(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema);
	return results;
    
  }
  
  async executeSQL(sqlStatement,args) {
     
	
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(sqlStatement));
    
	while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
        const results = await this.connection.query(sqlStatement,args)
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = this.captureException(new MariadbError(e,stack,sqlStatement))
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

  async _executeDDL(ddl) {
    await this.createSchema(this.parameters.TO_USER);
    const ddlResults = await Promise.all(ddl.map((ddlStatement) => {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      return this.executeSQL(ddlStatement) 
    }))
	return ddlResults;
  }

  setConnectionProperties(connectionProperties) {
  	 super.setConnectionProperties(Object.assign( Object.keys(connectionProperties).length > 0 ? connectionProperties : this.connectionProperties, MariadbConstants.CONNECTION_PROPERTY_DEFAULTS));
  }

  getConnectionProperties() {
    return Object.assign({
      host              : this.parameters.HOSTNAME
    , user              : this.parameters.USERNAME
    , password          : this.parameters.PASSWORD
    , database          : this.parameters.DATABASE
    , port              : this.parameters.PORT
    },MariadbConstants.CONNECTION_PROPERTY_DEFAULTS);
  }
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  setSpatialSerializer(spatialFormat) {      
    switch (spatialFormat) {
      case "WKB":
        this.spatialSerializer = "ST_AsBinary(";
        break;
      case "EWKB":
        this.spatialSerializer = "ST_AsBinary(";
        break;
      case "WKT":
        this.spatialSerializer = "ST_AsText(";
        break;
      case "EWKT":
        this.spatialSerializer = "ST_AsText(";
        break;
       case "GeoJSON":
	     this.spatialSerializer = "ST_AsGeoJSON("
		 break;
     default:
        this.spatialSerializer = "ST_AsBinary(";
    }  
  }  
    
  async initialize() {
    await super.initialize(true);
    this.setSpatialSerializer(this.SPATIAL_FORMAT);
  }

  async finalizeRead(tableInfo) {
    this.checkConnectionState(this.fatalError) 
    await this.executeSQL(`FLUSH TABLE "${tableInfo.TABLE_SCHEMA}"."${tableInfo.TABLE_NAME}"`)
  }

  /*
  **
  **  Gracefully close down the database connection and pool
  **
  */
  
  async finalize() {
    await super.finalize()
  }

  /*
  **
  **  Abort the database connection and pool
  **
  */

  async abort() {
	await super.abort()
  }

  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(`begin transaction`));
    
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
        await this.connection.beginTransaction();
        this.traceTiming(sqlStartTime,performance.now())
		super.beginTransaction()
		break
      } catch (e) {
		const cause = MariadbError(e,stack,'mariadb.Connection.beginTransaction()');
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'BEGIN TRANSACTION')
          continue;
        }
        throw cause
      }      
    } 
	
  }  
 
  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	    
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    let stack
    this.status.sqlTrace.write(this.traceSQL(`commit transaction`));
   
    try {
	  super.commitTransaction()
      const sqlStartTime = performance.now();
      stack = new Error().stack
      await this.connection.commit();
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  const cause = this.captureException(new MariadbError(e,stack,'mariadb.Connection.commit()'))
	  if (cause.lostConnection()) {
        await this.reconnect(cause,'COMMIT TRANSACTION')
	  }
	  else {
        throw cause
      }      
    } 
	
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

    let stack
    this.status.sqlTrace.write(this.traceSQL(`rollback transaction`));
    try {
	  super.rollbackTransaction()
      const sqlStartTime = performance.now();
      stack = new Error().stack
      await this.connection.rollback();
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  const newIssue = this.captureException(new MariadbError(e,stack,'mariadb.Connection.rollback()'))
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
    } 
	
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)
    await this.executeSQL(MariadbDBI.SQL_CREATE_SAVE_POINT);
	super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState();

	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
      await this.executeSQL(MariadbDBI.SQL_RESTORE_SAVE_POINT);
	  super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(MariadbDBI.SQL_RELEASE_SAVE_POINT);    
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
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

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
  
    const results = await this.executeSQL(MariadbDBI.SQL_SYSTEM_INFORMATION); 
    const sysInfo = results[0];
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : Yadamu.EXPORT_VERSION
     ,sessionUser        : sysInfo.SESSION_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,serverHostName     : sysInfo.SERVER_HOST
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,serverVendor       : sysInfo.SERVER_VENDOR_ID
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,nls_parameters     : {
        serverCharacterSet   : sysInfo.SERVER_CHARACTER_SET,
        databaseCharacterSet : sysInfo.DATABASE_CHARACTER_SET
      }
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

    // Cannot use JSON_ARRAYAGG for DATA_TYPES and SIZE_CONSTRAINTS beacuse MYSQL implementation of JSON_ARRAYAGG does not support ordering

	const SQL_SCHEMA_INFORMATION = 
     `select c.table_schema "TABLE_SCHEMA"   
             ,c.table_name "TABLE_NAME"
             ,concat('[',group_concat(concat('"',column_name,'"') order by ordinal_position separator ','),']')  "COLUMN_NAME_ARRAY"
             ,concat(
               '[',
                group_concat(
                  json_quote(case 
                               when cc.check_clause is not null then 
                                 'json'
                               when c.column_type = 'tinyint(1)' then 
                                 '${this.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)'}'
                               else 
                                 data_type
                             end
                            ) 
                   order by ordinal_position separator ','
                ),
                ']'
              ) "DATA_TYPE_ARRAY"
             ,concat(
               '[',
               group_concat(
                 json_quote(case 
                              when (numeric_precision is not null) and (numeric_scale is not null) then
                                concat(numeric_precision,',',numeric_scale) 
                              when (numeric_precision is not null) then 
                                case
                                  when column_type like '%unsigned' then
                                    numeric_precision
                                  else
                                    numeric_precision + 1
                                end
                              when (datetime_precision is not null) then 
                                datetime_precision
                              when (character_maximum_length is not null) then
                                character_maximum_length
                              else   
                                ''   
                            end
                           ) 
                 order by ordinal_position separator ','
               ),
               ']'
              ) "SIZE_CONSTRAINT_ARRAY"
             ,group_concat(
                   case 
                     when data_type in ('date','time','datetime','timestamp') then
                       -- Force ISO 8601 rendering of value 
                       concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')',' "',column_name,'"')
                     when data_type = 'year' then
                       -- Prevent rendering of value as base64:type13: 
                       concat('CAST("', column_name, '"as DECIMAL) "',column_name,'"')
                     when data_type = 'geometry' then
                       -- Force ${this.spatialFormat} rendering of value
                       concat('${this.spatialSerializer}"', column_name, '") "',column_name,'"')
                     when data_type in ('float') then
                       -- Render Floats with greatest possible precision 
                       -- Risk of Overflow ????
                       -- concat('(floor(1e15*"',column_name,'")/1e15) "',column_name,'"')                                      
                       -- Render Floats and Double as String ???
                       concat('cast((floor(1e15*"',column_name,'")/1e15) as varchar(64)) "',column_name,'"')
                     when data_type = 'double' then
                       concat('CAST("', column_name, '"as VARCHAR(128)) "',column_name,'"')
                     else
                       concat('"',column_name,'"')
                   end
                   order by ordinal_position separator ','
              ) "CLIENT_SELECT_LIST"
               from information_schema.columns c
                    left join information_schema.tables t
                       on t.table_name = c.table_name 
                      and t.table_schema = c.table_schema
                    left outer join information_schema.check_constraints cc
                       on cc.table_name = c.table_name 
                      and cc.constraint_schema = c.table_schema
                      and check_clause = concat('json_valid("',column_name,'")')  
              where c.extra <> 'VIRTUAL GENERATED'
                and t.table_type = 'BASE TABLE'
                and t.table_schema = ?
            group by t.table_schema, t.table_name`;

    const results = await this.executeSQL(SQL_SCHEMA_INFORMATION,[this.parameters[keyName]]);
    const schemaInfo = this.generateSchemaInfo(results) 
    return schemaInfo

  }

  streamingError(err,sqlStatement) {
	 return this.captureException(new MariadbError(err,this.streamingStackTrace,sqlStatement))
  }
    
  async getInputStream(tableInfo) {
    
	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT));
    
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		this.streamingStackTrace = new Error().stack
		const is = this.connection.queryStream(tableInfo.SQL_STATEMENT);
	    this.traceTiming(sqlStartTime,performance.now())
		return is;
      } catch (e) {
		const cause = this.captureException(new MariadbError(e,this.streamingStackTrace,sqlStatement))
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
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL) 
  }

  createParser(tableInfo) {
    return new MariadbParser(tableInfo,this.yadamuLogger);
  }  

  getOutputStream(tableName,ddlComplete) {
	 return super.getOutputStream(MariadbWriter,tableName,ddlComplete)
  }

  classFactory(yadamu) {
	return new MariadbDBI(yadamu)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select connection_id() "pid"`)
	const pid = results[0][0];
    return pid
  }
}

module.exports = MariadbDBI

const _SQL_CONFIGURE_CONNECTION = `SET AUTOCOMMIT = 0, TIME_ZONE = '+00:00',SESSION INTERACTIVE_TIMEOUT = 600000, WAIT_TIMEOUT = 600000, SQL_MODE='ANSI_QUOTES,PAD_CHAR_TO_FULL_LENGTH', GROUP_CONCAT_MAX_LEN = 1024000, GLOBAL LOCAL_INFILE = 'ON'`

const _SQL_SYSTEM_INFORMATION   = `select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET"`;                     
 
const _SQL_CREATE_SAVE_POINT    = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT   = `ROLLBACK TO SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT   = `RELEASE SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;