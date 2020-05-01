
"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

var snowflake = require('snowflake-sdk');

/*
**
** Snowflake Implementation Notes
**
**    Spatial Data Types : Use JSON
**    Interval Data Types: Use VARCHAR
**    LOB Support: VARCHAR and BINARY are supported to 16Mb
**
*/

const Yadamu = require('../../common/yadamu.js');
const YadamuDBI =  require('../../common/yadamuDBI.js');
const {ConnectionError, SnowFlakeError} = require('../../common/yadamuError.js')
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

const sqlTableInfo = 
`select t.table_schema "TABLE_SCHEMA"
         ,t.table_name   "TABLE_NAME"
         ,listagg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position) "COLUMN_LIST"
         ,listagg(concat('"',data_type,'"'),',') within group (order by ordinal_position) "DATA_TYPES"
         ,listagg(case
                       when (numeric_precision is not null) and (numeric_scale is not null) 
                         then concat('"',numeric_precision,',',numeric_scale,'"')
                       when (numeric_precision is not null) 
                         then concat('"',numeric_precision,'"')
                       when (datetime_precision is not null)
                         then concat('"',datetime_precision,'"')
                       when (character_maximum_length is not null)
                         then concat('"',character_maximum_length,'"')
                       else
                         '""'
                     end
                    ,','
                   )
                   within group (order by ordinal_position) "SIZE_CONSTRAINTS"
         ,concat('select ',listagg(case
                                     when c.data_type = 'VARIANT' then
                                       concat('TO_VARCHAR("',column_name,'") "',column_name,'"')
                                     else
                                       concat('"',column_name,'"')
                                   end
                                  ,',') within group (order by ordinal_position)
                          ,' from "',t.table_schema,'"."',t.table_name,'"') "SQL_STATEMENT"
     from information_schema.columns c, information_schema.tables t
    where t.table_name = c.table_name
      and t.table_schema = c.table_schema
      and t.table_type = 'BASE TABLE'
      and t.table_schema = ?
    group by t.table_schema, t.table_name`;

const sqlBeginTransaction = `begin`;

const sqlCommitTransaction = `commit`;

const sqlRollbackTransaction = `rollback`;

class SnowFlakeDBI extends YadamuDBI {
    
  // Promisfy ...
    
  establishConnection(connection) {
      
    return new Promise(function(resolve,reject) {
	  const stack = new stack().error
      connection.connect(function(err,connection) {
        if (err) {
          reject(new SnowFlakeError(err,stack,'snowflake-sdk.Connection.connect());
        }
        resolve(connection);
      })
    })
  } 
  
  executeSQL(sqlStatement, args) {
      	  
    const self = this

    // Promisfy ...

    return new Promise(function(resolve,reject) {
      if (self.status.sqlTrace) {
        self.status.sqlTrace.write(this.traceSQL(sqlStatement));
      }
	  const stack = new stack().error
      self.connection.execute({
        sqlText: sqlStatement
       ,binds  : args
	   ,fetchAsString: ['Number','Date','JSON']
       ,complete: function(err,statement,rows) {
                    if (err) {
                      self.yadamuLogger.logException([`${self.constructor.name}.executeSQL()`],err);
                      reject(new SnowFlakeError(err,stack,statement.sqlText);
                    }
                    else {
                      resolve(rows);
                    }
                  }
      })
    })
  }   
   
  async testConnection(connectionProperties,parameters) {   
    super.setConnectionProperties(connectionProperties);
	this.setTargetDatabase();
	try {
      let connection = snowflake.createConnection(this.connectionProperties);
      connection = await this.establishConnection(connection);
      connection.destroy()
	  super.setParameters(parameters)
	} catch (e) {
      throw e;
	}
	
  }
  
  async createConnectionPool() {
	  
	// Snowflake-SDK does not support connection pooling

  }
  
  async getConnectionFromPool() {

    this.setTargetDatabase();
    this.logConnectionProperties();
    let connection = snowflake.createConnection(this.connectionProperties);
    connection = await this.establishConnection(connection);
    const sqlStartTime = performance.now();
    this.traceTiming(sqlStartTime,performance.now())
    return connection

  }
  
  async releaseConnection() {
    if (this.connection !== undefined && this.connection.destroy) {
      try {
        await this.connection.destroy();
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.releaseConnection()`],e);
      }
	}
  };
  
  async configureConnection() {
    
    let results = await this.executeSQL(`alter session set autocommit = false timezone = 'UTC' TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM'`);

  }
  
  setTargetDatabase() {  
    if ((this.parameters.SNOWFLAKE_SCHEMA_DB) && (this.parameters.SNOWFLAKE_SCHEMA_DB !== this.connectionProperties.database)) {
      this.connectionProperties.database = this.parameters.SNOWFLAKE_SCHEMA_DB
    }
  }
  
  async executeDDLImpl(ddl) {
    const results = await Promise.all(ddl.map(async function(ddlStatement) {
      try {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
        const result = this.executeSQL(ddlStatement,[]);
        return result;
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.executeDDL()`],e)
        this.yadamuLogger.writeDirect(`${ddlStatement}\n`)
      } 
    },this))
  }
  
  get DATABASE_VENDOR() { return 'SNOWFLAKE' };
  get SOFTWARE_VENDOR() { return 'Snokwflake Inc' };
  get SPATIAL_FORMAT()  { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().snowflake }
          
  getConnectionProperties() {
    return {
      account           : this.parameters.HOSTNAME
    , username          : this.parameters.USERNAME
    , password          : this.parameters.PASSWORD
    , database          : this.parameters.DATABASE
    }
  }
  
  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().snowflake)
    this.connection = undefined;
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */

  async initialize() {
     await super.initialize(true);   
     this.spatialFormat = "GeoJSON"
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    await this.releaseConnection();
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
	 
    try {
      await this.releaseConnection();
    } catch (e) {
	  this.yadamuLogger.logException([`${this.constructor.name}.abort()`],e)
    }
    this.connection = undefined;
  }
  
    /*
  **
  ** Begin the current transaction
  **
  */
  
  async beginTransaction() {
     await this.executeSQL(sqlBeginTransaction,[]);
  }

  /*
  **
  ** Commit the current transaction
  **
  */
    
  async commitTransaction() {
     await this.executeSQL(sqlCommitTransaction,[]);
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
     await this.executeSQL(sqlRollbackTransaction,[]);
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
  
  async getSystemInformation(EXPORT_VERSION) {   
  
    const sqlStatement = 'SELECT CURRENT_WAREHOUSE() WAREHOUSE, CURRENT_DATABASE() DATABASE_NAME, CURRENT_SCHEMA() SCHEMA, CURRENT_ACCOUNT() ACCOUNT, CURRENT_VERSION() DATABASE_VERSION, CURRENT_CLIENT() CLIENT';
    
    const results = await this.executeSQL(sqlStatement,[]);
    
    const sysInfo = results[0];

    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     //,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.OWNER
     ,exportVersion      : EXPORT_VERSION
	 //,sessionUser      : sysInfo.SESSION_USER
	 //,currentUser      : sysInfo.CURRENT_USER
     ,warehouse          : sysInfo.WAREHOUSE
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,client             : sysInfo.CLIENT
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,account            : sysInfo.ACCOUNT
     //,nodeClient         : {}} 
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
  
  async getSchemaInfo(schemaKey) {
    const results = await this.executeSQL(sqlTableInfo,[this.parameters[schemaKey]])
    return results
  }

  generateMetadata(tableInfo,server) {    
    const metadata = {}
    for (let table of tableInfo) {
      metadata[table.TABLE_NAME] = {
        owner                    : table.TABLE_SCHEMA
       ,tableName                : table.TABLE_NAME
       ,columns                  : table.COLUMN_LIST
       ,dataTypes                : JSON.parse('[' + table.DATA_TYPES + ']')
       ,sizeConstraints          : JSON.parse('[' + table.SIZE_CONSTRAINTS + ']')
      }
    }
    return metadata;  
  }
   
  createParser(tableInfo,objectMode) {
    return new DBParser(tableInfo,objectMode,this.yadamuLogger);
  }  
    
  async getInputStream(tableInfo,parser) {
      
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))
    }
        
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
    
    const statement = this.connection.execute({sqlText: tableInfo.SQL_STATEMENT,  fetchAsString: ['Number','Date'], streamResult: true})
    const snowflakeStream = statement.streamRows();
    snowflakeStream.on('data', function(row) {readStream.push(row)})
    snowflakeStream.on('end',function(result) {readStream.push(null)});
    return readStream;      
  }      
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL) 
  }
  
  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)
  }  

 async newSlaveInterface(slaveNumber) {
	const dbi = new SnowFlakeDBI(this.yadamu)
	dbi.setParameters(this.parameters);
	dbi.setConnectionProperties(this.connectionProperties)
    await super.newSlaveInterface(slaveNumber,dbi,await dbi.getConnectionFromPool())
	dbi.configureConnection();
    return dbi;
  }

  tableWriterFactory(tableName) {
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger)
  }

}

module.exports = SnowFlakeDBI
