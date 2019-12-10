
"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

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

class snowflakeDBI extends YadamuDBI {
    
  // Promisfy ...
    
  establishConnection() {
      
    const connection = this.connection;
    return new Promise(function(resolve,reject) {
       connection.connect(function(err,conn) {
         if (err) {
           reject(err);
         }
         resolve(conn);
       })
    })
  } 
   
  executeSQL(sqlStatement, args) {
      
    const self = this
    
    return new Promise(function(resolve,reject) {
      if (self.status.sqlTrace) {
        self.status.sqlTrace.write(`${sqlStatement};\n--\n`);
      }
      self.connection.execute({
        sqlText: sqlStatement
       ,binds  : args
       ,complete: function(err,statement,rows) {
                    if (err) {
                      console.log(err);
                      reject(err);
                    }
                    else {
                      resolve(rows);
                    }
                  }
      })
    })
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
  isValidDDL() {
    return (this.systemInformation.vendor === this.DATABASE_VENDOR)
  }
  
  objectMode() {
    return true;
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
  
  async getConnection() {
    this.logConnectionProperties();
    this.connection = snowflake.createConnection(this.connectionProperties);
    this.connection = await this.establishConnection();
    let results = await this.executeSQL(`alter session set autocommit = false`);
    results = await this.executeSQL(`alter session set timezone = 'UTC'`);
    results = await this.executeSQL(`alter session set TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM'`);
  } 
  
  async initialize() {
     await super.initialize(true);   
     await this.getConnection()
     this.spatialFormat = "GeoJSON"
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    if (this.connection) {
      await this.connection.destroy();
      this.connection = undefined;
    }
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    try {
      await this.connection.destroy();
    } catch (e) {
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
   
  createParser(query,objectMode) {
    return new DBParser(query,objectMode,this.yadamuLogger);
  }  
    
  async getInputStream(query,parser) {
      
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${query.SQL_STATEMENT};\n--\n`)
    }
        
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
    
    const statement = this.connection.execute({sqlText: query.SQL_STATEMENT,  fetchAsString: ['Number','Date'], streamResult: true})
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

}

module.exports = snowflakeDBI
