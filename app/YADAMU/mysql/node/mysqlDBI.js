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
const DBParser = require('./dbParser.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator80 = require('./statementGenerator.js');
const StatementGenerator57 = require('../../dbShared/mysql/statementGenerator57.js');

const sqlSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET"`;                     

// Check for duplicate entries INFORMATION_SCHEMA.columns

const sqlCheckInformationSchemaState =
`select distinct c.table_schema, c.table_name
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
  group by TABLE_SCHEMA,TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
  having count(*) > 1`

const sqlInformationSchemaClean =
`   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
      group by t.table_schema, t.table_name`;
      
   
// Hack for Duplicate Entries in INFORMATION_SCHEMA.columns seen MSSQL 5.7

const sqlInformationSchemaDirty  = 
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

class MySQLDBI extends YadamuDBI {
    
  /*
  **
  ** Local methods 
  **
  */
   
  async testConnection(connectionProperties,parameters) {   
    try {
      this.setConnectionProperties(connectionProperties);
      this.conn = this.newConnection();
      await this.establishConnection();
	  await this.conn.end();
	  super.setParameters(parameters)
	} catch (e) {
	  throw (e)
	} 
  }

  async sqlTableInfo(schema) {

    /*
    **
    ** During testing on 5.7 it appeared tha that is is possible for the Information Schema to get corrupted
    ** In the corrupt state some table contains duplicate entires for each column in the table.
    ** 
    ** This routine checks for this state and creates a query that will workaround the problem if the 
    ** Information schema is corrupt.
    ** 
    */   
    
    const selectSchemaInfo = 
      `select c.table_schema "TABLE_SCHEMA"
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
               ) "SQL_STATEMENT"`;
       
  
    const results = await this.executeSQL(sqlCheckInformationSchemaState,[schema]);
    if (results.length ===  0) {
      return `${selectSchemaInfo} ${sqlInformationSchemaClean}`
    }
    else {
      for (const i in results) {
        this.yadamuLogger.warning([`${this.constructor.name}`,`"${results[i].TABLE_SCHEMA}"."${results[i].TABLE_NAME}"`],`Duplicate entires detected in INFORMATION_SCHEMA.COLUMNS.`)
      }
      return `${selectSchemaInfo} ${sqlInformationSchemaDirty}`
    }
  }
  
  establishConnection() {
   
    const self = this
    const conn = this.conn;
   
    return new Promise(function(resolve,reject) {
                         const sqlStartTime = performance.now();
                         conn.connect(function(err) {
						   const sqlCumlativeTime = performance.now() - sqlStartTime;
                           if (self.status.sqlTrace) {
                             self.status.sqlTrace.write(`--\n-- Elapsed Time: ${YadamuLibrary.stringifyDuration(sqlCumlativeTime)}s.\n--\n`);
                           }
                           self.sqlCumlativeTime = self.sqlCumlativeTime + sqlCumlativeTime
                           if (err) {
                             reject(err);
                           }
                           resolve();
                         })
                      })
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
        // this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Closing Connection`);
        await this.conn.end();
        // this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Connection Closed`);
      } catch (e) {
        this.yadamuLogger.warning([`${this.constructor.name}.reconnect()`],`this.conn.end() raised\n${e}`);
      }
    }
    
    this.connectionOpen = false;
    
    while (retryCount < 10) {
      try {
        // this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Opening Connection`);
        await this.getConnection()
        // this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Connection Opened`);
        break;
      } catch (e) {
        if (e.fatal && (e.code && (e.code === 'ECONNREFUSED'))) {
          this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`Waiting for MySQL server restart.`)
          this.waitForRestart(500);
          retryCount++;
        }
        else {
          this.yadamuLogger.info([`${this.constructor.name}.reconnect()`],`this.getConnection() raised\n${e}`);
          throw e;
        }
      }
    }
  }

  executeSQL(sqlStatement,args,attemptReconnect) {
    
    attemptReconnect = attemptReconnect === undefined ? true : attemptReconnect
    const self = this
  
    return new Promise(
                 function(resolve,reject) {
                   if (self.status.sqlTrace) {
                     self.status.sqlTrace.write(`${sqlStatement};\n--\n`);
                   }
                   const sqlStartTime = performance.now();
				   self.conn.query(
                     sqlStatement,
                     args,
                     async function(err,results,fields) {
                       const sqlCumlativeTime = performance.now() - sqlStartTime;
                       if (err) {
                         if (attemptReconnect && ((err.fatal) && (err.code && (err.code === 'PROTOCOL_CONNECTION_LOST') || (err.code === 'ECONNRESET')))){
                           self.yadamuLogger.warning([`${self.constructor.name}.executeSQL()`],`SQL Operation raised\n${err}`);
                           self.yadamuLogger.info([`${self.constructor.name}.executeSQL()`],`Attemping reconnection.`);
                           await self.reconnect()
                           self.yadamuLogger.info([`${self.constructor.name}.executeSQL()`],`New connection availabe.`);
                           results = await self.executeSQL(sqlStatement,args,false);
                           resolve(results);
              1          }
                         else {
                           reject(err);
                         }
                       }
                       if (self.status.sqlTrace) {
                         self.status.sqlTrace.write(`--\n-- Elapsed Time: ${YadamuLibrary.stringifyDuration(sqlCumlativeTime)}s.\n--\n`);
                       }
                       self.sqlCumlativeTime = self.sqlCumlativeTime + sqlCumlativeTime
                       resolve(results);
                   })
               })
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
      this.connectionOpen = false;
      return true;
    }    
    return false;
  }
  
  setConnectionProperties(connectionProperties) {
	connectionProperties = Object.assign(connectionProperties,CONNECTION_PROPERTY_DEFAULTS);
	super.setConnectionProperties(connectionProperties); 
  }
	  
  
  newConnection() {
     
     const self = this;
     
     const conn = mysql.createConnection(this.connectionProperties); 
     conn.on('error',function(err) {
       self.yadamuLogger.logException([`${this.constructor.name}.newConnection()`,`connection.onError()`],err);
     });
     return conn;
  }
  
  
  async getConnection() {

    this.logConnectionProperties();

    this.conn = this.newConnection();
    await this.establishConnection();
	// console.log('Setting KeepAlive to 5 Mins')
    // this.conn._socket.setKeepAlive(true, 3600000);

    this.connectionOpen = true;
    
    if (await this.setMaxAllowedPacketSize()) {
      this.conn = this.newConnection();
      await this.establishConnection();
      this.connectionOpen = true;
    }

    await this.configureSession(); 	

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
  
  /*
  **
  ** Overridden Methods
  **
  */
    
  async executeDDLImpl(ddl) {
    await this.createSchema(this.parameters.TO_USER);
    await Promise.all(ddl.map(function(ddlStatement) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${ddlStatement};\n--\n`);
      }
      return this.conn.query(ddlStatement) 
    },this))
  }

  get DATABASE_VENDOR() { return 'MySQL' };
  get SOFTWARE_VENDOR() { return 'Oracle Corporation (MySQL)' };
  get SPATIAL_FORMAT()  { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().mysql }
  
  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().mysql)
    this.connectionOpen = false
    this.keepAliveInterval = this.parameters.READ_KEEP_ALIVE ? this.parameters.READ_KEEP_ALIVE : 60000

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
    await this.getConnection();
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    await this.conn.end();
    this.connectionOpen = false;
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
    this.connectionOpen = false;
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
  
  processLog(results) {
    if (results[0].logRecords !== null) {
      const log = JSON.parse(results[0].logRecords);
      super.processLog(log, this.status, this.yadamuLogger)
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
	return this.processLog(results);
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
      
    const tableInfo = await this.sqlTableInfo();
    return await this.executeSQL(tableInfo,[this.parameters[schema]]);

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

    /*
	**
	** Intermittant Timeout problem with MySQL causes premature abort on Input Stream
	** Use a KeepAlive query to prevent Timeouts on the MySQL Connection.
    **
	** Use setInterval.. 
	** It appears that the keepAlive Promises do not resolve until input stream has been emptied.
	**
    **
	*/
   
    const keepAliveHdl = setInterval(this.keepAlive,this.keepAliveInterval,this);

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${query.SQL_STATEMENT};\n--\n`)
    }
	
	const self = this
    const is = this.conn.query(query.SQL_STATEMENT).stream();
    const streamCreatedTime = performance.now();
    is.on('end',
	  async function() {
		clearInterval(keepAliveHdl);
	    await self.executeSQL(`FLUSH TABLES "${query.TABLE_SCHEMA}"."${query.TABLE_NAME}"`)
	})
	is.on('error',
	  async function(err) {
        self.yadamuLogger.info([`${self.constructor.name}.getInputStream()`,`${err.code}`],`Stream Processing Time: ${YadamuLibrary.stringifyDuration(performance.now() - streamCreatedTime)}.`); 
	    if ((err.fatal) && (err.code && (err.code === 'PROTOCOL_CONNECTION_LOST') || (err.code === 'ECONNRESET'))){
		  err.yadamuHandled = true;
          self.yadamuLogger.warning([`${self.constructor.name}.getInputStream()`],`SQL Operation raised\n${err}`);
          self.yadamuLogger.info([`${self.constructor.name}.getInputStream()`],`Attemping reconnection.`);
          await self.reconnect()
          self.yadamuLogger.info([`${self.constructor.name}.getInputStream()`],`New connection availabe.`);
	    }      
	})
    return is
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
  
  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)

  }
  
  createParser(query,objectMode) {
    this.parser = new DBParser(query,objectMode,this.yadamuLogger,this);
	return this.parser;
  }  
    
  async keepAlive(dbi) {
    dbi.yadamuLogger.info([`${this.constructor.name}.keepAlive()`],`Row [${dbi.parser.getCounter()}]`)
	this.results = await dbi.executeSQL('select 1');
  }
}

module.exports = MySQLDBI

