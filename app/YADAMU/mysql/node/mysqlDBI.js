"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/
const mysql = require('mysql');

const Yadamu = require('../../common/yadamu.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const MySQLConstants = require('./mysqlConstants.js')
const MySQLError = require('./mysqlError.js')
const MySQLParser = require('./mysqlParser.js');
const MySQLWriter = require('./mysqlWriter.js');
const StatementGenerator80 = require('./statementGenerator.js');
const StatementGenerator57 = require('../../dbShared/mysql/statementGenerator57.js');

class MySQLDBI extends YadamuDBI {
    
  // Until we have static constants

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_CHECK_INFORAMATION_SCHEMA_STATE()            { return _SQL_CHECK_INFORAMATION_SCHEMA_STATE } 
  static get SQL_INFORMATION_SCHEMA_FROM_CLAUSE()             { return _SQL_INFORMATION_SCHEMA_FROM_CLAUSE }
  static get SQL_INFORMATION_SCHEMA_FROM_CLAUSE_DUPLICATES()  { return _SQL_INFORMATION_SCHEMA_FROM_CLAUSE_DUPLICATES }
  static get SQL_CREATE_SAVE_POINT()                          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()                         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()                         { return _SQL_RELEASE_SAVE_POINT }


  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  // Define Getters based on configuration settings here
 
  // Override YadamuDBI

  get DATABASE_VENDOR()            { return MySQLConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()            { return MySQLConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()       { return MySQLConstants.STATEMENT_TERMINATOR };
  
  // Enable configuration via command line parameters
  get SPATIAL_FORMAT()             { return this.parameters.SPATIAL_FORMAT            || MySQLConstants.SPATIAL_FORMAT }
  get TABLE_MATCHING()             { return this.parameters.TABLE_MATCHING            || MySQLConstants.TABLE_MATCHING}
  get READ_KEEP_ALIVE()            { return this.parameters.READ_KEEP_ALIVE           || MySQLConstants.READ_KEEP_ALIVE}
  get TREAT_TINYINT1_AS_BOOLEAN()  { return this.parameters.TREAT_TINYINT1_AS_BOOLEAN || MySQLConstants.TREAT_TINYINT1_AS_BOOLEAN }
  
  constructor(yadamu) {
    super(yadamu,MySQLConstants.DEFAULT_PARAMETERS)
    this.keepAliveInterval = this.parameters.READ_KEEP_ALIVE ? this.parameters.READ_KEEP_ALIVE : 0
	this.keepAliveHdl = undefined
  }

  /*
  **
  ** Local methods 
  **
  */
   
  async testConnection(connectionProperties,parameters) {   
    try {
      this.setConnectionProperties(connectionProperties);
      this.connection = this.getConnectionFromPool();
      await this.openDatabaseConnection();
	  await this.connection.end();
	  super.setParameters(parameters)
	} catch (e) {
	  throw (e)
	} 
  }
  
  async configureConnection() {

    await this.executeSQL(MySQLDBI.SQL_CONFIGURE_CONNECTION);

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
	 
    // MySQL.createPool() is synchronous	 
	  
    this.logConnectionProperties();
	
	let stack, operation
	try {
	  stack = new Error().stack;
	  operation = 'mysql.createPool()'  
	  const sqlStartTime = performance.now();
	  this.pool = new mysql.createPool(this.connectionProperties);
	  this.traceTiming(sqlStartTime,performance.now())
      await this.checkMaxAllowedPacketSize()
	} catch (e) {
      throw this.captureException(new MySQLError(e,stack,operation))
    }
	
	
  }
  
  async getConnectionFromPool() {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)
	
	if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
    }
	
	const stack = new Error().stack;
    const connection = await new Promise((resolve,reject) => {
        const sqlStartTime = performance.now();
        this.pool.getConnection((err,connection) => {
            this.traceTiming(sqlStartTime,performance.now())
            if (err) {
		      reject(this.captureException(new MySQLError(err,stack,'mysql.Pool.getConnection()')))
            }
            resolve(connection);
          }
		)
      }
	)
	
    return connection
  }
  
  async closeConnection() {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${this.connection !== undefined && this.connection.release})`)

 	if (this.keepAliveHdl) {
	  clearInterval(this.keepAliveHdl)
	}

    if (this.connection !== undefined && this.connection.release) {
	  let stack;
      try {
        stack = new Error().stack
        await this.connection.release();
		this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
  	    throw this.captureException(new MySQLError(e,stack,'MySQL.Connection.release()'))
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
  	    throw new MariadbError(e,stack,'Mariadb.Pool.end()')
	  }
	}
	
  };	  

  async reconnectImpl() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.connectionProvider.getConnectionFromPool()
    await this.connection.ping()
  }

  executeSQL(sqlStatement,args) {
    
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    return new Promise((resolve,reject) => {
                   if (this.status.sqlTrace) {
                     this.status.sqlTrace.write(this.traceSQL(sqlStatement));
                   }
				   const stack = new Error().stack;
                   const sqlStartTime = performance.now(); 
				   this.connection.query(
                     sqlStatement,
                     args,
                     async (err,results,fields) => {
                       const sqlEndTime = performance.now()
                       if (err) {
         		         const cause = this.captureException(new MySQLError(err,stack,sqlStatement))
		                 if (attemptReconnect && cause.lostConnection()) {
						   attemptReconnect = false
						   try {
                             await this.reconnect(cause,'SQL')
                             results = await this.executeSQL(sqlStatement,args);
                             resolve(results);
						   } catch (e) {
                             reject(e);
                           }							 
              1          }
                         else {
                           reject(cause);
                         }
                       }
					   this.traceTiming(sqlStartTime,sqlEndTime)
                       resolve(results);
                   })
               })
  }  
     
  async createSchema(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema);
	return results;
    
  }
  
  async createStagingTable() {    	
	const sqlStatement = `CREATE TEMPORARY TABLE IF NOT EXISTS "YADAMU_STAGING"("DATA" JSON)`;					   
	const results = await this.executeSQL(sqlStatement);
	return results;
  }

  async loadStagingTable(importFilePath) { 
    importFilePath = importFilePath.replace(/\\/g, "\\\\");
	const sqlStatement = `LOAD DATA LOCAL INFILE '${importFilePath}' INTO TABLE "YADAMU_STAGING" FIELDS ESCAPED BY ''`;					   
	const results = await this.executeSQL(sqlStatement);
	return results;
  }

  async verifyDataLoad() {    	
	const sqlStatement = `SELECT COUNT(*) FROM "YADAMU_STAGING"`;				
	const results = await  this.executeSQL(sqlStatement);
	return results;
  }
  
  async executeDDLImpl(ddl) {
    await this.createSchema(this.parameters.TO_USER);
    const ddlResults = await Promise.all(ddl.map((ddlStatement) => {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      return this.executeSQL(ddlStatement) 
    }))	
	return ddlResults;
  }

  setConnectionProperties(connectionProperties) {
	 super.setConnectionProperties(Object.assign( Object.keys(connectionProperties).length > 0 ? connectionProperties : this.connectionProperties, MySQLConstants.CONNECTION_PROPERTY_DEFAULTS));
  }

  getConnectionProperties() {
    return Object.assign({
      host              : this.parameters.HOSTNAME
    , user              : this.parameters.USERNAME
    , password          : this.parameters.PASSWORD
    , database          : this.parameters.DATABASE
    , port              : this.parameters.PORT
    },MySQLConstants.CONNECTION_PROPERTY_DEFAULTS);
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

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`begin transaction`));
    }    

    let stack
	try {
	  stack = new Error().stack
      await this.connection.beginTransaction();
	  super.beginTransaction();
	} catch (e) {
      throw this.captureException(new MySQLError(e,stack,'mysql.Connection.beginTransaction()'))
	} 

  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
	
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`commit transaction`));
    }    

	let stack
	try {
	  super.commitTransaction();
	  stack = new Error().stack
      await this.connection.commit();
	} catch (e) {
      throw this.captureException(new MySQLError(e,stack,'mysql.Connection.commit()'))
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
			
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`rollback transaction`));
    }    
	
	let stack
	try {
	  super.rollbackTransaction();
	  stack = new Error().stack
      await this.connection.rollback();
	} catch (e) {
      const newIssue = this.captureException(new MySQLError(e,stack,'mysql.Connection.rollback()'))
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
	}
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(MySQLDBI.SQL_CREATE_SAVE_POINT);
	super.createSavePoint();
   }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

	this.checkConnectionState(cause)

	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
	
	try {
      await this.executeSQL(MySQLDBI.SQL_RESTORE_SAVE_POINT);
	  super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(MySQLDBI.SQL_RELEASE_SAVE_POINT);   
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
 
  async uploadFile(importFilePath) {
	let results = await this.createStagingTable();
	results = await this.loadStagingTable(importFilePath);
    return results;
  }

  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */
  
  processLog(results,operation) {
    if (results[0].logRecords !== null) {
      const log = JSON.parse(results[0].logRecords);
      super.processLog(log, operation, this.status, this.yadamuLogger)
      return log
    }
    else {
      return null
    }
  }

  async processFile(hndl) {
    const sqlStatement = `SET @RESULTS = ''; CALL IMPORT_JSON(?,@RESULTS); SELECT @RESULTS "logRecords";`;					   
	let results = await  this.executeSQL(sqlStatement,this.parameters.TO_USER);
    results = results.pop();
	return this.processLog(results,'JSON_TABLE');
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
  
    const results = await this.executeSQL(MySQLDBI.SQL_SYSTEM_INFORMATION); 
    const sysInfo = results[0];
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : this.EXPORT_VERSION
     ,sessionUser        : sysInfo.SESSION_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,serverHostName     : sysInfo.SERVER_HOST
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,serverVendor       : sysInfo.SERVER_VENDOR_ID
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
      
    /*
    **
    ** During testing on 5.7 it appeared tha that is is possible for the Information Schema to get corrupted
    ** In the corrupt state some table contains duplicate entires for each column in the table.
    ** 
    ** This routine checks for this state and creates a query that will workaround the problem if the 
    ** Information schema is corrupt.
    ** 
    */   
    
    const SQL_SCHEMA_INFORMATION_SELECT_CLAUSE = 
      `select c.table_schema "TABLE_SCHEMA"
             ,c.table_name "TABLE_NAME"
             ,concat('[',group_concat(concat('"',column_name,'"') order by ordinal_position separator ','),']')  "COLUMN_NAME_ARRAY"
             ,concat('[',group_concat(case 
                                        when column_type = 'tinyint(1)' then 
                                          json_quote('${this.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)'}')
                                        else 
                                          json_quote(data_type)
                                      end 
                                      order by ordinal_position separator ','),']')  "DATA_TYPE_ARRAY"
             ,concat('[',group_concat(json_quote(
                                  case when (numeric_precision is not null) and (numeric_scale is not null)
                                         then concat(numeric_precision,',',numeric_scale) 
                                       when (numeric_precision is not null)
                                         then case
                                                when column_type like '%unsigned' then 
                                                  numeric_precision
                                                else
                                                  numeric_precision + 1
                                              end
                                       when (datetime_precision is not null)
                                         then datetime_precision
                                       when (character_maximum_length is not null)
                                         then character_maximum_length
                                       else   
                                         ''   
                                  end
                                 ) 
                                 order by ordinal_position separator ','
                          ),']') "SIZE_CONSTRAINT_ARRAY"
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
                     when data_type = 'float' then
                       -- Render Floats with greatest possible precision 
                       -- Risk of Overflow ????
                       concat('(floor(1e15*"',column_name,'")/1e15) "',column_name,'"')                                      
                     else
                       concat('"',column_name,'"')
                   end
                   order by ordinal_position separator ','
              ) "CLIENT_SELECT_LIST"`;
         
    const duplicates = await this.executeSQL(MySQLDBI.SQL_CHECK_INFORAMATION_SCHEMA_STATE,[this.parameters[keyName]]);
    const SQL_SCHEMA_INFORMATION = `${SQL_SCHEMA_INFORMATION_SELECT_CLAUSE}\n${duplicates.length === 0 ? MySQLDBI.SQL_INFORMATION_SCHEMA_FROM_CLAUSE : MySQLDBI.SQL_INFORMATION_SCHEMA_FROM_CLAUSE_DUPLICATES}`;
	return await this.executeSQL(SQL_SCHEMA_INFORMATION,[this.parameters[keyName]]);

  }
  
  streamingError(err,sqlStatement) {
	 return this.captureException(new MySQLError(err,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(tableInfo) {

    /*
	**
	** Intermittant Timeout problem with MySQL causes premature abort on Input Stream
	** If this occures set READ_KEEP_ALIVE to a value >  0 Use a KeepAlive query to prevent Timeouts on the MySQL Connection.
	** Use a local keepAliveHdl to allow for parallel operaitons
    **
	** Use setInterval.. 
	** It appears that the keepAlive Promises do not resolve until input stream has been emptied.
	**
    **
	*/

    let keepAliveHdl = undefined
   
    if (this.keepAliveInterval > 0) {
      this.yadamuLogger.info([`${this.constructor.name}.getInputStream()`],`Stating Keep Alive. Interval ${this.keepAliveInterval}ms.`)
      keepAliveHdl = setInterval(this.keepAlive,this.keepAliveInterval,this);
	}

	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))
    }

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		this.streamingStackTrace = new Error().stack
		const is = this.connection.query(tableInfo.SQL_STATEMENT).stream();
        is.on('end', async () => {
		  // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,`${is.constructor.name}.onEnd()`,`${tableInfo.TABLE_NAME}`],``); 
          if (keepAliveHdl !== undefined) {
		    clearInterval(keepAliveHdl);
		    keepAliveHdl = undefined
		  }
	    })
		return is;
      } catch (e) {
		const cause = this.captureException(new MySQLError(e,this.streamingStackTrace,tableInfo.SQL_STATEMENT))
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
    let statementGenerator = undefined;
    const sqlVersion = `SELECT @@version`
    const results = await this.executeSQL(sqlVersion);
    if (results[0]['@@version'] > '6.0') {
      await super.generateStatementCache(StatementGenerator80, schema, executeDDL)
    }
    else {
      await super.generateStatementCache(StatementGenerator57, schema, executeDDL)
    }
  }
  
  getOutputStream(tableName) {
	 return super.getOutputStream(MySQLWriter,tableName)
  }

  createParser(tableInfo) {
    this.parser = new MySQLParser(tableInfo,this.yadamuLogger,this);
	return this.parser;
  }  
    
  async keepAlive(dbi) {
	// Prevent Connections with Long Running streaming operations from timing out..
    dbi.yadamuLogger.info([`${this.constructor.name}.keepAlive()`],`Row [${dbi.parser.getCounter()}]`)
	try {
	  this.results = await dbi.executeSQL('select 1');
	} catch (e) {
      // Don't care of timeout query fails
	}
  }

  async workerDBI(workerNumber) {
	const dbi = new MySQLDBI(this.yadamu)
	return await super.workerDBI(workerNumber,dbi)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select connection_id() "pid"`)
	const pid = results[0].pid;
    return pid
  }
}

module.exports = MySQLDBI

const _SQL_CONFIGURE_CONNECTION = `SET AUTOCOMMIT = 0, TIME_ZONE = '+00:00',SESSION INTERACTIVE_TIMEOUT = 600000, WAIT_TIMEOUT = 600000, SQL_MODE='ANSI_QUOTES,PAD_CHAR_TO_FULL_LENGTH', GROUP_CONCAT_MAX_LEN = 1024000, GLOBAL LOCAL_INFILE = 'ON'`

const _SQL_SYSTEM_INFORMATION   = `select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET"`;                     

// Check for duplicate entries INFORMATION_SCHEMA.columns

const _SQL_CHECK_INFORAMATION_SCHEMA_STATE =
`select distinct c.table_schema, c.table_name
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
  group by TABLE_SCHEMA,TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
  having count(*) > 1`

const _SQL_INFORMATION_SCHEMA_FROM_CLAUSE =
`   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
      group by t.table_schema, t.table_name`;
      
   
// Hack for Duplicate Entries in INFORMATION_SCHEMA.columns seen MySQL 5.7

const _SQL_INFORMATION_SCHEMA_FROM_CLAUSE_DUPLICATES  = 
`   from (
     select distinct c.table_catalog, c.table_schema, c.table_name,column_name,ordinal_position,data_type,column_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision
       from information_schema.columns c, information_schema.tables t
       where t.table_name = c.table_name 
         and c.extra <> 'VIRTUAL GENERATED'
         and t.table_schema = c.table_schema
         and t.table_type = 'BASE TABLE'
         and t.table_schema = ?
   ) c
  group by c.table_schema, c.table_name`;

const _SQL_CREATE_SAVE_POINT  = `SAVEPOINT ${YadamuDBI.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT = `ROLLBACK TO SAVEPOINT ${YadamuDBI.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT = `RELEASE SAVEPOINT ${YadamuDBI.SAVE_POINT_NAME}`;
