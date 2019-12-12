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
}

class MariadbDBI extends YadamuDBI {
    
  async testConnection(connectionProperties,parameters) {   
    try {
	  this.setConnectionProperties(connectionProperties);
      this.conn = await mariadb.createConnection(this.connectionProperties);
	  await this.conn.end();
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
     
  async configureSession() {  

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

  async setMaxAllowedPacketSize() {

    const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
    const sqlQueryPacketSize = `SELECT @@max_allowed_packet`;
    const sqlSetPacketSize = `SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`
    
    
    let results = await this.executeSQL(sqlQueryPacketSize);
    
    if (parseInt(results[0]['@@max_allowed_packet']) <  maxAllowedPacketSize) {
      this.yadamuLogger.info([`${this.constructor.name}.setMaxAllowedPacketSize()`],`Increasing MAX_ALLOWED_PACKET to 1G.`);
      results = await this.executeSQL(sqlSetPacketSize);
      await this.conn.end();
      await this.pool.end();
      return true;
    }    
    return false;
  }
  
  async getConnectionPool() {

    this.logConnectionProperties();

    this.pool = mariadb.createPool(this.connectionProperties);
    this.conn = await this.pool.getConnection();

    if (await this.setMaxAllowedPacketSize()) {
      this.pool = mariadb.createPool(this.connectionProperties);
      this.conn = await this.pool.getConnection();
    }
    
    await this.configureSession(); 	

  }    

  waitForRestart(delayms) {
    return new Promise(function (resolve, reject) {
        setTimeout(resolve, delayms);
    });
  }
   
  async reconnect() {
      
    let retryCount = 0;
    
    if (this.conn) {
      try { 
        this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Closing Connection.`);
        await this.conn.end();
        this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Connection Closed.`);
      } catch (e) {
        this.yadamuLogger.warning([`${this.constructor.name}.reconnect()`],`this.conn.end() raised\n${e}`);
      }
    }

    if (this.pool) {    
      try { 
        this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Closing Pool.`);
        await this.pool.end();
        this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Pool Closed.`);
      } catch (e) {
        this.yadamuLogger.warning([`${this.constructor.name}.reconnect()`],`this.pool.end() raised\n${e}`);
      }
    }
    
    while (retryCount < 10) {
      try {
        await this.getConnectionPool()
        break;
      } catch (e) {
        if (e.fatal && (e.code && (e.code === 'ECONNREFUSED'))) {
          this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Waiting for MariaDB server restart.`)
          this.waitForRestart(100);
          retryCount++;
        }
        else {
          throw e;
        }
      }
    }
  }
         
  async createSchema(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema);
	return results;
    
  }
  
  async executeSQL(sqlStatement,args) {
     
    let attemptReconnect = true;
    
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
    }
   
    while (true) {
      // Will exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        const results = await this.conn.query(sqlStatement,args)
        const sqlCumlativeTime = performance.now() - sqlStartTime;
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`--\n-- Elapsed Time: ${YadamuLibrary.stringifyDuration(sqlCumlativeTime)}s.\n--\n`);
        }
        this.sqlCumlativeTime = this.sqlCumlativeTime + sqlCumlativeTime        
		return results;
      } catch (e) {
        if (attemptReconnect && ((e.fatal) && (e.code && (e.code === 'ER_CMD_CONNECTION_CLOSED') || (e.code === 'ECONNABORTED') || (e.code === 'ER_SOCKET_UNEXPECTED_CLOSE') || (err.code === 'ECONNRESET')))){
          attemptReconnect = false;
          this.yadamuLogger.warning([`${this.constructor.name}.executeSQL()`],`SQL Operation raised\n${e}`);
          this.yadamuLogger.info([`${this.constructor.name}.executeSQL()`],`Attemping reconnection.`);
          await this.reconnect()
          this.yadamuLogger.info([`${this.constructor.name}.executeSQL()`],`New connection availabe.`);
          continue;
        }
        throw (e)
      }      
    } 
  }  

  async executeDDLImpl(ddl) {
    await this.createSchema(this.parameters.TO_USER);
    await Promise.all(ddl.map(function(ddlStatement) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      return this.executeSQL(ddlStatement) 
    },this))
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
  
  async getDatabaseConnectionImpl() {
    await this.getConnectionPool();
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
    await this.conn.end();
    await this.pool.end();
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
	try {
      await this.conn.end();
    } catch (e) {}
	try {
      await this.pool.end();
    } catch (e) {}
  }

  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {
    await this.conn.beginTransaction();
  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
    await this.conn.commit();
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
    await this.conn.rollback();
  }
  
  async createSavePoint() {
    await this.executeSQL(sqlCreateSavePoint);
  }
  
  async restoreSavePoint() {
    await this.executeSQL(sqlRestoreSavePoint);
  }  

  async releaseSavePoint() {
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
   
  async getInputStream(query,parser) {
       
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${query.SQL_STATEMENT};\n--\n`);
    }

	const self = this
    const is = new Readable({objectMode: true });
    is._read = function() {};  
  
    this.conn.queryStream(query.SQL_STATEMENT).on('data',
    function(row) {
      is.push(row)
    }).on('end',
    function() {
      is.push(null)
    }).on('error',
    async function(err) {
      self.yadamuLogger.info([`${self.constructor.name}.getInputStream()`,`${err.code}`],`Connection Idle Time: ${YadamuLibrary.stringifyDuration(performance.now() - self.lastUsedTime)}.`); 
	  if ((err.fatal) && (err.code && (err.code === 'PROTOCOL_CONNECTION_LOST') || (err.code === 'ECONNRESET'))){
	    err.yadamuHandled = true;
        self.yadamuLogger.warning([`${self.constructor.name}.getInputStream()`],`SQL Operation raised\n${err}`);
        self.yadamuLogger.info([`${self.constructor.name}.getInputStream()`],`Attemping reconnection.`);
        await self.reconnect()
        this.lastUsedTime = performance.now();
        self.yadamuLogger.info([`${self.constructor.name}.getInputStream()`],`New connection availabe.`);
	  }      
    })
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

}

module.exports = MariadbDBI
