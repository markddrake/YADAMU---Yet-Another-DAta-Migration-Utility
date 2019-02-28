"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/
const mysql = require('mysql');

const Yadamu = require('../../common/yadamu.js');
const DBParser = require('../../common/dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator80 = require('./statementGenerator.js');
const StatementGenerator57 = require('../../dbShared/mysql/statementGenerator57.js');

const defaultParameters = {
  BATCHSIZE         : 10000
, COMMITSIZE        : 10000
, IDENTIFIER_CASE   : null
}

const sqlGetSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE"`;                     

const sqlGetTableInfo = 
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
              when data_type = 'date'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%TZ'')')
              when data_type = 'timestamp'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')')
              when data_type = 'datetime'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT("', column_name, '", ''%Y-%m-%dT%T.%f'')')
              when data_type = 'year'
                -- Prevent rendering of value as base64:type13: 
                then concat('CAST("', column_name, '"as DECIMAL)')
              when data_type like '%blob'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              when data_type = 'varbinary'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              when data_type = 'binary'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              when data_type = 'geometry'
                -- Force WKT rendering of value
                then concat('ST_asText("', column_name, '")')
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

const sqlInformationSchemaFix  = 
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

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class DBInterface {
    
  get DATABASE_VENDOR() { return 'MySQL' };
  get SOFTWARE_VENDOR() { return 'Oracle Corporation (MySQL)' };
  get SPATIAL_FORMAT()  { return 'WKT' };

  establishConnection() {
   
    const conn = this.conn;
    return new Promise(function(resolve,reject) {
                         conn.connect(function(err) {
                                        if (err) {
                                          reject(err);
                                        }
                                        resolve();
                                      })
                      })
  } 

  executeSQL(sqlStatement,args) {
    
    const status = this.status;
    const conn = this.conn;
    
    return new Promise(function(resolve,reject) {
                         if (status.sqlTrace) {
                           status.sqlTrace.write(`${sqlStatement};\n--\n`);
                         }
                         conn.query(sqlStatement,args,function(err,rows,fields) {
                                                    if (err) {
                                                      reject(err);
                                                    }
                                                    resolve(rows);
                                                 })
                       })
  }  

  async configureSession() {

    const sqlAnsiQuotes = `SET SESSION SQL_MODE=ANSI_QUOTES`;
    
    await this.executeSQL(sqlAnsiQuotes);
    
    const sqlTimeZone = `SET TIME_ZONE = '+00:00'`;
    await this.executeSQL(sqlTimeZone);
   
    const setGroupConcatLength = `SET SESSION group_concat_max_len = 1024000`
    await this.executeSQL(setGroupConcatLength);

    const enableFileUpload = `SET GLOBAL local_infile = 'ON'`
    await this.executeSQL(enableFileUpload);
  }

  async setMaxAllowedPacketSize() {

    const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
    const sqlQueryPacketSize = `SELECT @@max_allowed_packet`;
    const sqlSetPacketSize = `SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`
    
    
    let results = await this.executeSQL(sqlQueryPacketSize);
    
    if (parseInt(results[0]['@@max_allowed_packet']) <  maxAllowedPacketSize) {
      this.logWriter.write(`${new Date().toISOString()}: Increasing MAX_ALLOWED_PACKET to 1G.\n`);
      results = await this.executeSQL(sqlSetPacketSize);
      await this.conn.end();
      return true;
    }    
    return false;
  }
  
  async getConnection() {
 
    this.conn = mysql.createConnection(this.connectionProperties);
    await this.establishConnection();

    if (await this.setMaxAllowedPacketSize()) {
      this.conn = mysql.createConnection(this.connectionProperties);
      await this.establishConnection();
    }

    await this.configureSession(); 	

  }    
  
  async createTargetDatabase(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema);
	return results;
    
  }
  
  setConnectionProperties(connectionProperties) {
    this.connectionProperties = connectionProperties
  }
  
  getConnectionProperties() {
    return {
      host              : this.parameters.HOSTNAME
    , user              : this.parameters.USERNAME
    , password          : this.parameters.PASSWORD
    , database          : this.parameters.DATABASE
    , port              : this.parameters.PORT
    , multipleStatements: true
    , typeCast          : false
    , supportBigNumbers : true
    , bigNumberStrings  : true          
    , dateStrings       : true
    }
  }
  
  constructor(yadamu) {
    this.yadamu = yadamu;
    this.parameters = yadamu.mergeDefaultParameters(defaultParameters);
    this.status = yadamu.getStatus()
    this.logWriter = yadamu.getLogWriter();
     
    this.conn = undefined;;
    this.connectionProperties = this.getConnectionProperties()   

    this.statementCache = undefined;
    
    this.tableName  = undefined;
    this.tableInfo  = undefined;
    this.insertMode = 'Empty';
    this.skipTable = true;


  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize() {
    await this.getConnection();
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    await this.conn.end();
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    await this.conn.end();
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
  
  	 
  async createStagingTable() {    	
	const sqlStatement = `CREATE TEMPORARY TABLE IF NOT EXISTS "JSON_STAGING"("DATA" JSON)`;					   
	const results = await this.executeSQL(sqlStatement);
	return results;
  }

  async loadStagingTable(importFilePath) { 
    importFilePath = importFilePath.replace(/\\/g, "\\\\");
	const sqlStatement = `LOAD DATA LOCAL INFILE '${importFilePath}' INTO TABLE "JSON_STAGING" FIELDS ESCAPED BY ''`;					   
	const results = await this.executeSQL(sqlStatement);
	return results;
  }

  async verifyDataLoad() {    	
	const sqlStatement = `SELECT COUNT(*) FROM "JSON_STAGING"`;				
	const results = await  this.executeSQL(sqlStatement);
	return results;
  }

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

  async processFile(mode,schema,hndl) {
    const sqlStatement = `SET @RESULTS = ''; CALL IMPORT_JSON(?,@RESULTS); SELECT @RESULTS "logRecords";`;					   
	let results = await  this.executeSQL(sqlStatement,schema);
    results = results.pop();
	return JSON.parse(results[0].logRecords)
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
  
  async getSystemInformation(schema,EXPORT_VERSION) {     
  
    const results = await this.executeSQL(sqlGetSystemInformation); 
    const sysInfo = results[0];
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : schema
     ,exportVersion      : EXPORT_VERSION
     ,sessionUser        : sysInfo.SESSION_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,serverHostName     : sysInfo.SERVER_HOST
     ,databaseVersion    : sysInfo.DATABASE_VERSION
     ,serverVendor       : sysInfo.SERVER_VENDOR_ID
    }
    
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async generateTableInfoQuery(schema) { 

  /*
  **
  ** During testing on 5.7 it appeared tha that is is possible for the Information Schema to get corrupted
  ** In this state it contains duplicate entires for each column in the table.
  ** 
  ** This routine checks for this state and creates a query that will workaround the problem if the 
  ** Information schema is corrupt.
  ** 
  */   
   
    const results = await this.executeSQL(sqlCheckInformationSchemaState,[schema]);
    if (results.length ===  0) {
      return sqlGetTableInfo + sqlInformationSchemaClean;
    }
    else {
      for (const i in results) {
        this.logWriter.write(`${new Date().toISOString()}[WARNING]: Table: "${results[i].TABLE_SCHEMA}"."${results[i].TABLE_NAME}". Duplicate entires detected in INFORMATION_SCHEMA.COLUMNS.\n`)
      }
      return sqlGetTableInfo + sqlInformationSchemaFix;
    }
  }

  async getDDLOperations(schema) {
    return []
  }
    
  async getTableInfo(schema,status) {
      
    const tableInfo = await this.generateTableInfoQuery(schema);
    return await this.executeSQL(tableInfo,[schema]);

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
   
  generateSelectStatement(tableMetadata) {
     return tableMetadata;
  }   

  createParser(query,objectMode) {
    return new DBParser(query,objectMode,this.logWriter);      
  }
  
  async getInputStream(query,parser) {
    return this.conn.query(query.SQL_STATEMENT).stream();
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
  
  async initializeDataLoad(databaseVendor) {
    await this.createTargetDatabase(this.schema);
  }
  
  async executeDDL(ddl) {
    await Promise.all(ddl.map(function(ddlStatement) {
      return this.conn.query(ddlStatement) 
    },this))
  }

  async generateStatementCache(schema,systemInformation,metadata,ddlRequired) {
    let statementGenerator = undefined;
    const sqlGetVersion = `SELECT @@version`
    const results = await this.executeSQL(sqlGetVersion);
    if (results[0]['@@version'] > '6.0') {
       statementGenerator = new StatementGenerator80(this,ddlRequired,this.parameters.BATCHSIZE,this.parameters.COMMITSIZE,this.status,this.logWriter);
    }
    else {
       statementGenerator = new StatementGenerator57(this,ddlRequired,this.parameters.BATCHSIZE,this.parameters.COMMITSIZE,this.status,this.logWriter);
    }
  
    // Uncomment the folloing statement Force 5.7 Code Path
    // statementGenerator = new StatementGenerator57(this,ddlRequired,this.parameters.BATCHSIZE,this.parameters.COMMITSIZE,this.status,this.logWriter);
    
    this.statementCache = await statementGenerator.generateStatementCache(schema,systemInformation,metadata)
  }

  getTableWriter(schema,tableName) {
    return new TableWriter(this,schema,tableName,this.statementCache[tableName],this.status,this.logWriter);      
  }
  
  async finalizeDataLoad() {
  }  

}

module.exports = DBInterface
