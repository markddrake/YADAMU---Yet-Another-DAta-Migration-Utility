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
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const MySQLError = require('./mysqlError.js')
const MySQLParser = require('./mysqlParser.js');
const MySQLWriter = require('./mysqlWriter.js');
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
      
   
// Hack for Duplicate Entries in INFORMATION_SCHEMA.columns seen MySQL 5.7

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

const sqlCreateSavePoint  = `SAVEPOINT ${YadamuDBI.SAVE_POINT_NAME}`;

const sqlRestoreSavePoint = `ROLLBACK TO SAVEPOINT ${YadamuDBI.SAVE_POINT_NAME}`;

const sqlReleaseSavePoint = `RELEASE SAVEPOINT ${YadamuDBI.SAVE_POINT_NAME}`;

const CONNECTION_PROPERTY_DEFAULTS = {
  multipleStatements: true
, typeCast          : true
, supportBigNumbers : true
, bigNumberStrings  : true          
, dateStrings       : true
, trace             : true
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
      this.connection = this.getConnectionFromPool();
      await this.openDatabaseConnection();
	  await this.connection.end();
	  super.setParameters(parameters)
	} catch (e) {
	  throw (e)
	} 
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
   
  executeSQL(sqlStatement,args) {
    
    let attemptReconnect = this.attemptReconnection;

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

  setConnectionProperties(connectionProperties) {
	connectionProperties = Object.assign(connectionProperties,CONNECTION_PROPERTY_DEFAULTS);
	super.setConnectionProperties(connectionProperties); 
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
    const ddlResults = await Promise.all(ddl.map((ddlStatement) => {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      return this.executeSQL(ddlStatement) 
    }))	
	return ddlResults;
  }

  get DATABASE_VENDOR() { return 'MySQL' };
  get SOFTWARE_VENDOR() { return 'Oracle Corporation (MySQL)' };
  get SPATIAL_FORMAT()  { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().mysql }
  
  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().mysql)
    this.keepAliveInterval = this.parameters.READ_KEEP_ALIVE ? this.parameters.READ_KEEP_ALIVE : 0
	this.keepAliveHdl = undefined
  }
  
  setConnectionProperties(connectionProperties) {
	 super.setConnectionProperties(Object.assign( Object.keys(connectionProperties).length > 0 ? connectionProperties : this.connectionProperties,CONNECTION_PROPERTY_DEFAULTS));
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
       case "GeoJSON":
	     this.spatialSerializer = "(ST_AsGeoJSON("
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
	  stack = new Error().stack
      await this.connection.commit();
	  super.commitTransaction();
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
	  stack = new Error().stack
      await this.connection.rollback();
	  super.rollbackTransaction();
	} catch (e) {
      const newIssue = this.captureException(new MySQLError(e,stack,'mysql.Connection.rollback()'))
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
	}
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(sqlCreateSavePoint);
	super.createSavePoint();
   }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

	this.checkConnectionState(cause)

	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
	
	try {
      await this.executeSQL(sqlRestoreSavePoint);
	  super.restoreSavePoint();
	} catch (newIssue) {
	  this.checkCause('RESTORE SAVPOINT',cause,newIssue)
	}
  }  

  async releaseSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.releaseSavePoint()`,this.getWorkerNumber()],``)

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
  
    const results = await this.executeSQL(sqlSystemInformation); 
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
  
  streamingError(err,sqlStatement) {
	 return this.captureException(new MySQLError(err,this.streamingStackTrace,sqlStatement))
  }
  
  async freeInputStream(tableInfo,inputStream) {
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

	let attemptReconnect = this.attemptReconnection;
    
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
		const cause = this.captureException(new MySQLError(e,this.streamingStackTrace,sqlStatement))
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
   
  createParser(tableInfo,objectMode) {
    this.parser = new MySQLParser(tableInfo,objectMode,this.yadamuLogger,this);
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

