"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const mariadb = require('mariadb');

const YadamuDBI = require('../../common/yadamuDBI.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('../../dbShared/mysql/statementGenerator57.js');

const defaultParameters = {
  BATCHSIZE         : 10000
, COMMITSIZE        : 10000
}

const sqlSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE"`;                     

// Cannot use JSON_ARRAYAGG for DATA_TYPES and SIZE_CONSTRAINTS beacuse MYSQL implementation of JSON_ARRAYAGG does not support ordering
const sqlSchemaInfo = 
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
              when data_type in ('date','time','datetime','timestamp')
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')')
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
        ) "SQL_STATEMENT"
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
     and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
	  group by t.table_schema, t.table_name`;

class MariadbDBI extends YadamuDBI {
    
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
      await this.pool.end();
      return true;
    }    
    return false;
  }
  
  async getConnectionPool(parameters,status,logWriter) {

    this.pool = mariadb.createPool(this.connectionProperties);
    this.conn = await this.pool.getConnection();

    if (await this.setMaxAllowedPacketSize()) {
      this.pool = mariadb.createPool(this.connectionProperties);
      this.conn = await this.pool.getConnection();
    }
    
    await this.configureSession(); 	

  }    
  
  async createSchema(schema) {    	
  
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await this.executeSQL(sqlStatement,schema);
	return results;
    
  }
  
  async executeSQL(sqlStatement,args) {
    
   if (this.status.sqlTrace) {
     this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
   }

   return await this.conn.query(sqlStatement,args)
  }  

  async executeDDL(schema, ddl) {
    // console.log(ddl);
    await this.createSchema(schema);
    await Promise.all(ddl.map(function(ddlStatement) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,schema);
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
  get SPATIAL_FORMAT()  { return 'WKT' };

  constructor(yadamu) {
    super(yadamu,defaultParameters);
     
    this.pool = undefined;

    if (!this.parameters.TABLE_MATCHING) {
      this.parameters.TABLE_MATCHING = 'INSENSITIVE';
    }

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
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize(schema) {
    super.initialize(schema);
    await this.getConnectionPool();
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
    await this.conn.end();
    await this.pool.end();
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
  
  async getSystemInformation(schema,EXPORT_VERSION) {     
  
    const results = await this.executeSQL(sqlSystemInformation); 
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
     ,softwareVendor     : this.SOFTWARE_VENDOR
    }
    
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations(schema) {
    return undefined
  }
    
  async getSchemaInfo(schema,status) {
      
    return await this.executeSQL(sqlSchemaInfo,[schema]);

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

    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};  
  
    this.conn.queryStream(query.SQL_STATEMENT).on('data',
    function(row) {
      readStream.push(row)
    }).on('end',
    function() {
      readStream.push(null)
    }) 
  
    return readStream;      
  }      
  
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
  
  async initializeDataLoad(schema) {
  }
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL)
  }

  getTableWriter(schema,table) {
    return super.getTableWriter(TableWriter,schema,table)
  }

  async finalizeDataLoad() {
  }  

}

module.exports = MariadbDBI
