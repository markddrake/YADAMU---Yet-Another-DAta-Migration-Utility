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

const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const {ConnectionError, MariadbError} = require('../../common/yadamuError.js')
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('../../dbShared/mysql/statementGenerator57.js');

const sqlSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET"`;                     

const sqlCreateSavePoint = `SAVEPOINT YadamuInsert`;

const sqlRestoreSavePoint = `ROLLBACK TO SAVEPOINT YadamuInsert`;

const sqlReleaseSavePoint = `RELEASE SAVEPOINT YadamuInsert`;

const CONNECTION_PROPERTY_DEFAULTS = {
  multipleStatements: true
, typeCast          : true
, supportBigNumbers : true
, bigNumberStrings  : true          
, dateStrings       : true
, trace             : true
}

class MariadbDBI extends YadamuDBI {
    
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
	
  // Cannot use JSON_ARRAYAGG for DATA_TYPES and SIZE_CONSTRAINTS beacuse MYSQL implementation of JSON_ARRAYAGG does not support ordering
  sqlSchemaInfo() {
     return `select c.table_schema "TABLE_SCHEMA"
                   ,c.table_name "TABLE_NAME"
                   ,group_concat(concat('"',column_name,'"') order by ordinal_position separator ',')  "COLUMN_LIST"
                   ,concat('[',group_concat(json_quote(data_type) order by ordinal_position separator ','),']')  "DATA_TYPES"
                   ,concat('[',group_concat(json_quote(
                                        case when (numeric_precision is not null) and (numeric_scale is not null)
                                               then concat(numeric_precision,',',numeric_scale) 
                                             when (numeric_precision is not null)
                                               then case
                                                      when column_type like '%unsigned' 
                                                        then numeric_precision
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
                                ),']') "SIZE_CONSTRAINTS"
                   ,concat(
                      'select json_array('
                      ,group_concat(
                        case 
                          when data_type in ('date','time','datetime','timestamp') then
                            -- Force ISO 8601 rendering of value 
                            concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')')
                          when data_type = 'year' then
                            -- Prevent rendering of value as base64:type13: 
                            concat('CAST("', column_name, '"as DECIMAL)')
                          when data_type like '%blob' then
                            -- Force HEXBINARY rendering of value
                            concat('HEX("', column_name, '")')
                          when data_type = 'varbinary' then
                            -- Force HEXBINARY rendering of value
                            concat('HEX("', column_name, '")')
                          when data_type = 'binary' then
                            -- Force HEXBINARY rendering of value
                            concat('HEX("', column_name, '")')
                            when data_type = 'geometry' then
                              -- Force ${this.spatialFormat} rendering of value
                              concat('${this.spatialSerializer}"', column_name, '"))')
                            when data_type = 'float' then
                            -- Render Floats with greatest possible precision 
                            -- Risk of Overflow ????
                            concat('(floor(1e15*"',column_name,'")/1e15)')
                          else
                            concat('"',column_name,'"')
                        end
                        order by ordinal_position separator ','
                      )
                      ,') "json" from "'
                      ,c.table_schema
                      ,'"."'
                      ,c.table_name
                      ,'"'
                    ) "SQL_STATEMENT"
               from information_schema.columns c, information_schema.tables t
              where t.table_name = c.table_name 
                 and c.extra <> 'VIRTUAL GENERATED'
                and t.table_schema = c.table_schema
                and t.table_type = 'BASE TABLE'
                and t.table_schema = ?
          	  group by t.table_schema, t.table_name`;
  }
     
  async configureConnection() {  

    const sqlSetITimeout  = `SET SESSION interactive_timeout = 600000`;
    await this.executeSQL(sqlSetITimeout);

    const sqlSetWTimeout  = `SET SESSION wait_timeout = 600000`;
    await this.executeSQL(sqlSetWTimeout);

    const sqlSetSqlMode = `SET SESSION SQL_MODE='ANSI_QUOTES,PAD_CHAR_TO_FULL_LENGTH'`;
    await this.executeSQL(sqlSetSqlMode);
    
    const sqlTimeZone = `SET TIME_ZONE = '+00:00'`;
    await this.executeSQL(sqlTimeZone);
   
    const setGroupConcatLength = `SET SESSION group_concat_max_len = 1024000`
    await this.executeSQL(setGroupConcatLength);

    const enableFileUpload = `SET GLOBAL local_infile = 'ON'`
    await this.executeSQL(enableFileUpload);
    
    const disableAutoCommit = 'set autocommit = 0';
    await this.executeSQL(disableAutoCommit);   
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
	
	await this.releaseConnection();
	
  }
  
  async createConnectionPool() {
    this.logConnectionProperties();
	let sqlStartTime = performance.now();
	this.pool = mariadb.createPool(this.connectionProperties);
	this.traceTiming(sqlStartTime,performance.now())
    await this.checkMaxAllowedPacketSize();
  }
  
  async getConnectionFromPool() {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
    }
	
    let stack
	try {
      const sqlStartTime = performance.now();
	  stack = new Error().stack
	  const connection = await this.pool.getConnection();
	  this.traceTiming(sqlStartTime,performance.now())
	  connection.ping()
      return connection
	} catch (e) {
	  throw new MariadbError(e,stack,'mariadb.Pool.getConnection()')
	}
  }

  async releaseConnection() {
    if (this.connection !== undefined && this.connection.end) {
      try {
        await this.connection.end();
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.releaseConnection()`],e); 
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
	
	await this.releaseConnection()
    this.connection = await this.getConnectionFromPool()
	
    // this.yadamuLogger.trace([`${this.constructor.name}.reconnectImpl()`],`Unable to re-establish connection.`)
	
  }

  async createSchema(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema);
	return results;
    
  }
  
  async executeSQL(sqlStatement,args) {
     
    let attemptReconnect = this.attemptReconnection;
    
    if (this.status.sqlTrace) {
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
		const cause = new MariadbError(e,stack,sqlStatement)
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

  async executeDDLImpl(ddl) {
    await this.createSchema(this.parameters.TO_USER);
    const ddlResults = await Promise.all(ddl.map(function(ddlStatement) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      return this.executeSQL(ddlStatement) 
    },this))
	return ddlResults;
  }

  /*
  **
  ** Overridden Methods
  **
  */
  
  get DATABASE_VENDOR() { return 'MariaDB' };
  get SOFTWARE_VENDOR() { return ' MariaDB Corporation AB[' };
  get SPATIAL_FORMAT()  { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().mariadb }

  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().mariadb);
    this.pool = undefined;
  }
  
  setConnectionProperties(connectionProperties) {
	 super.setConnectionProperties(Object.assign(connectionProperties,CONNECTION_PROPERTY_DEFAULTS));
  }

  getConnectionProperties() {
    return Object.assign({
      host              : this.parameters.HOSTNAME
    , user              : this.parameters.USERNAME
    , password          : this.parameters.PASSWORD
    , database          : this.parameters.DATABASE
    , port              : this.parameters.PORT
    },CONNECTION_PROPERTY_DEFAULTS);
  }
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  setSpatialSerializer(spatialFormat) {      
    switch (spatialFormat) {
      case "WKB":
        this.spatialSerializer = "HEX(ST_AsBinary(";
        break;
      case "EWKB":
        this.spatialSerializer = "HEX(ST_AsBinary(";
        break;
      case "WKT":
        this.spatialSerializer = "(ST_AsText(";
        break;
      case "EWKT":
        this.spatialSerializer = "(ST_AsText(";
        break;
     default:
        this.spatialSerializer = "HEX(ST_AsBinary(";
    }  
  }  
    
  async initialize() {
    await super.initialize(true);
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
    this.setSpatialSerializer(this.spatialFormat);
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */
  
  async finalize() {
    await this.connection.end();
    await this.pool.end();
  }

  async finalizeRead(tableInfo) {
    await this.executeSQL(`FLUSH TABLE "${tableInfo.TABLE_SCHEMA}"."${tableInfo.TABLE_NAME}"`)
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    try {
      await this.releaseConnection();
	  if (this.pool !== undefined) {
	    await this.pool.end();
      }
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

    let attemptReconnect = this.attemptReconnection;
    
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`begin transaction`));
    }    
   
    let stack
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
          await this.reconnect(cause)
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
	    
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`commit transaction`));
    }    
   
    let stack
    try {
      const sqlStartTime = performance.now();
      stack = new Error().stack
      await this.connection.commit();
      this.traceTiming(sqlStartTime,performance.now())
	  super.commitTransaction()
    } catch (e) {
	  const cause = new MariadbError(e,stack,'mariadb.Connection.commit()');
	  if (cause.lostConnection()) {
        await this.reconnect(cause)
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

	// If rollbackTransaction was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`rollback transaction`));
    }    
   
    let stack
    try {
      const sqlStartTime = performance.now();
      stack = new Error().stack
      await this.connection.rollback();
      this.traceTiming(sqlStartTime,performance.now())
	  super.rollbackTransaction()
    } catch (e) {
	  const err = new MariadbError(e,stack,'mariadb.Connection.rollback()')
	  if (err.lostConnection()) {
        await this.reconnect(err)
	  }
	  else {
  	    if (cause instanceof Error) {
          this.yadamuLogger.logException([`${this.constructor.name}.rollbackTransaction()`],err)
	      err = cause
		}
        throw cause
      }      
    } 
	
  }
  
  async createSavePoint() {
    await this.executeSQL(sqlCreateSavePoint);
	super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
      await this.executeSQL(sqlRestoreSavePoint);
	  super.restoreSavePoint();
	} catch (e) {
	  if (cause instanceof Error) {
        this.yadamuLogger.logException([`${this.constructor.name}.restoreSavePoint()`],e)
	    e = cause
	  }
	  throw e;
	}
  }  

  async releaseSavePoint() {
    await this.executeSQL(sqlReleaseSavePoint);    
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
  
  async getSystemInformation(EXPORT_VERSION) {     
  
    const results = await this.executeSQL(sqlSystemInformation); 
    const sysInfo = results[0];
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : EXPORT_VERSION
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
    
  async getSchemaInfo(schema) {
      
    return await this.executeSQL(this.sqlSchemaInfo(),[this.parameters[schema]]);

  }

  generateMetadata(tableInfo,server) {    

    const metadata = {}
  
    for (let table of tableInfo) {
       metadata[table.TABLE_NAME] = {
         owner                    : table.TABLE_SCHEMA
       , tableName                : table.TABLE_NAME
       , columns                  : table.COLUMN_LIST
       , dataTypes                : JSON.parse(table.DATA_TYPES)
       , sizeConstraints          : JSON.parse(table.SIZE_CONSTRAINTS)
      }
    }
  
    return metadata;    

  }
  
  streamingError(err,stack,tableInfo) {
	 return new MariadbError(err,stack,tableInfo.SQL_STATEMENT)
  }
    
  async getInputStream(tableInfo,parser) {
       
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT));
    }
	
	const is = this.connection.queryStream(tableInfo.SQL_STATEMENT);
	return is;
  }
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL) 
  }

  createParser(query,objectMode) {
    return new DBParser(query,objectMode,this.yadamuLogger);
  }  

  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)
  }

  async finalizeDataLoad() {
  }  

  async slaveDBI(slaveNumber) {
	const dbi = new MariadbDBI(this.yadamu)
	dbi.pool = this.pool
	dbi.setParameters(this.parameters);
	const connection = await this.getConnectionFromPool()
	return await super.slaveDBI(slaveNumber,dbi,connection)
  }
  
  tableWriterFactory(tableName) {
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger)
  }

}

module.exports = MariadbDBI
