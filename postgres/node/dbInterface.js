"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/
const {Client} = require('pg')
const CopyFrom = require('pg-copy-streams').from;
const QueryStream = require('pg-query-stream')

const Yadamu = require('../../common/yadamu.js');
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');

const defaultParameters = {
  BATCHSIZE         : 10000
, COMMITSIZE        : 10000
, IDENTIFIER_CASE   : null
, USERNAME          : 'postgres'
, PASSWORD          : null
, HOSTNAME          : 'localhost'
, DATABASE          : 'postgres'
, PORT              : 5432

}

const sqlGenerateQueries = `select EXPORT_JSON($1)`;

const sqlGetSystemInformation = `select current_database() database_name,current_user,session_user,current_setting('server_version_num') database_version`;					   

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class DBInterface {
    
  get DATABASE_VENDOR() { return 'Postgres' };
  get SOFTWARE_VENDOR() { return 'The PostgreSQL Global Development Group' };
  get SPATIAL_FORMAT()  { return 'WKT' };

  async getClient() {

    const pgClient = new Client(this.connectionProperties);
    await pgClient.connect();
    const logWriter = this.logWriter;

    pgClient.on('notice',function(n){ 
	                       const notice = JSON.parse(JSON.stringify(n));
                           switch (notice.code) {
                             case '42P07': // Table exists on Create Table if not exists
                               break;
                             case '00000': // Table not found on Drop Table if exists
		                       break;
                             default:
                               logWriter.write(`${new Date().toISOString()}[Notice]:${n}\n`);
                           }
    })
  
    const setTimezone = `set timezone to 'UTC'`
    if (this.status.sqlTrace) {
       this.status.sqlTrace.write(`${setTimezone}\n\/\n`)
    }
    await pgClient.query(setTimezone);
  
    const setIntervalFormat =  `SET intervalstyle = 'iso_8601';`;
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${setIntervalFormat}\n\/\n`)
    }
    await pgClient.query(setIntervalFormat);

    return pgClient;
  }
  
  setConnectionProperties(connectionProperties) {
    this.connectionProperties = connectionProperties
  }
  
  getConnectionProperties() {
    return {
      user      : this.parameters.USERNAME
     ,host      : this.parameters.HOSTNAME
     ,database  : this.parameters.DATABASE
     ,password  : this.parameters.PASSWORD
     ,port      : this.parameters.PORT
    }
  }
  
  isValidDDL() {
    return (this.systemInformation.vendor === this.DATABASE_VENDOR)
  }
  
  objectMode() {
    return true;
  }
  
  setSystemInformation(systemInformation) {
    this.systemInformation = systemInformation
  }
  
  setMetadata(metadata) {
    this.metadata = metadata
  }
  
  constructor(yadamu) {
    this.yadamu = yadamu;
    this.parameters = yadamu.mergeParameters(defaultParameters);
    this.status = yadamu.getStatus()
    this.logWriter = yadamu.getLogWriter();
    
    this.systemInformation = undefined;
    this.metadata = undefined;
        
    this.pgClient = undefined;
    this.connectionProperties = this.getConnectionProperties()   
    this.useBinaryJSON = false;
    
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
  
  async initialize(schema) {
    this.pgClient = await this.getClient()
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
     await this.pgClient.end();
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
     await this.pgClient.end();
  }


  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {
     const sqlStatement =  `begin transaction`

     if (this.status.sqlTrace) {
       this.status.sqlTrace.write(`${sqlStatement};\n\n`);
     }

     await this.pgClient.query(sqlStatement);
  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
     const sqlStatement =  `commit transaction`

     if (this.status.sqlTrace) {
       this.status.sqlTrace.write(`${sqlStatement};\n\n`);
     }

     await this.pgClient.query(sqlStatement);
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
     const sqlStatement =  `rollback transaction`

     if (this.status.sqlTrace) {
       this.status.sqlTrace.write(`${sqlStatement};\n\n`);
     }

     await this.pgClient.query(sqlStatement);

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
  	let sqlStatement = `drop table if exists "JSON_STAGING"`;		
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n\/\n`)
    }    
  	await this.pgClient.query(sqlStatement);
  	sqlStatement = `create temporary table if not exists "JSON_STAGING" (data ${this.useBinaryJSON === true ? 'jsonb' : 'json'}) on commit preserve rows`;					   
    if (this.status.sqlTrace) {
      status.sqlTrace.write(`${sqlStatement}\n\/\n`)
    }    
  	await this.pgClient.query(sqlStatement);
  }
  
  async loadStagingTable(importFilePath) {

    const copyStatement = `copy "JSON_STAGING" from STDIN csv quote e'\x01' delimiter e'\x02'`;
    if (this.status.sqlTrace) {
      status.sqlTrace.write(`${copyStatement}\n\/\n`)
    }    

    let inputStream = fs.createReadStream(importFilePath);
    const stream = this.pgClient.query(CopyFrom(copyStatement));
    const importProcess = new Promise(async function(resolve,reject) {  
      stream.on('end',function() {resolve()})
  	  stream.on('error',function(err){reject(err)});  	  
      inputStream.pipe(stream);
    })  
    
    const startTime = new Date().getTime();
    await importProcess;
    const elapsedTime = new Date().getTime() - startTime
    inputStream.close()
    return elapsedTime;
  }
  
  async uploadFile(importFilePath) {
    let elapsedTime;
    try {
      await this.createStagingTable();    
      elapsedTime = await this.loadStagingTable(importFilePath)
    }
    catch (e) {
      if (e.code && (e.code === '54000')) {
        this.logWriter.write(`${new Date().toISOString()}}[uploadFile()]: Cannot process file using Binary JSON. Switching to textual JSON.\n`)
        this.useBinaryJSON = false;
        await createStagingTable();
        elapsedTime = await loadStagingTable(importFilePath);	
        console.log(elapsedTime);
      }      
      else {
        throw e
      }
    }
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

 async processStagingTable(schema) {  	
  	const sqlStatement = `select ${this.useBinaryJSON ? 'import_jsonb' : 'import_json'}(data,$1) from "JSON_STAGING"`;
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n\/\n`)
    }    
  	var results = await this.pgClient.query(sqlStatement,[schema]);
    if (results.rows.length > 0) {
      if (this.useBinaryJSON) {
	    return results.rows[0].import_jsonb;  
      }
      else {
	    return results.rows[0].import_json;  
      }
    }
    else {
      this.logWriter.write(`${new Date().toISOString()}}[processStagingTable()]: Unexpected Error. No response from ${ this.useBinaryJSON === true ? 'CALL IMPORT_JSONB()' : 'CALL_IMPORT_JSON()'}. Please ensure file is valid JSON and NOT pretty printed.\n`);
      this.status.errorRaised = true;
      // Return value will be parsed....
      return [];
    }
  }

  async processFile(mode,schema,hndl) {
     return await this.processStagingTable(schema)
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
  
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
   
	const results = await this.pgClient.query(sqlGetSystemInformation);
	const sysInfo = results.rows[0];
	
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.schema
     ,exportVersion      : EXPORT_VERSION
	 ,sessionUser        : sysInfo.session_user
     ,dbName             : sysInfo.database_name
     ,databaseVersion    : sysInfo.database_version
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
  
  async fetchMetadata(schema) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }  
    
    const results = await this.pgClient.query(sqlGenerateQueries,[schema]);
    this.metadata = results.rows[0].export_json;
  }
  
  generateTableInfo() {
      
    const tableInfo = Object.keys(this.metadata).map(function(value) {
      return {TABLE_NAME : value, SQL_STATEMENT : this.metadata[value].sqlStatemeent}
    },this)
    return tableInfo;    
    
  }
  
  async getTableInfo(schema) {
    await this.fetchMetadata(schema);    
    return this.generateTableInfo();
  }

  generateMetadata(tableInfo,server) {     
    return this.metadata;
  }
   
  generateSelectStatement(tableMetadata) {
     return tableMetadata;
  }   

  createParser(query,objectMode) {
    return new DBParser(query,objectMode,this.logWriter);      
  }
  
  async getInputStream(query,parser) {
    const queryStream = new QueryStream(query.SQL_STATEMENT)
    return await this.pgClient.query(queryStream)   
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
  
  async initializeDataLoad(schema) {
  }
  
  async executeDDL(schema, ddl) {
    await Promise.all(ddl.map(async function(ddlStatement) {
      try {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,schema);
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`${ddlStatement};\n--\n`);
        }
        return await this.pgClient.query(ddlStatement);
      } catch (e) {
        this.logWriter.write(`${e}\n${ddlStatement}\n`)
      }
    },this))
  }
  
  async generateStatementCache(schema,executeDDL) {
      
    const sqlStatement = `select GENERATE_SQL($1,$2)`
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
    }
    const results = await this.pgClient.query(sqlStatement,[{systemInformation : this.systemInformation, metadata : this.metadata},schema]);
    this.statementCache = results.rows[0].generate_sql;
    if (this.statementCache === null) {
      this.statementCache = {}
      return []
    }
    else {
      const tables = Object.keys(this.metadata); 
      const ddlStatements = tables.map(function(table,idx) {
        const tableInfo = this.statementCache[table];
       tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';
       tableInfo.batchSize = Math.trunc(45000 / tableInfo.targetDataTypes.length)
       tableInfo.commitSize = this.parameters.COMMIT_SIZE
       return tableInfo.ddl
      },this);
    
      if (executeDDL === true) {
        await this.executeDDL(schema,ddlStatements);
      }
    }
    return this.statementCache;
  }

  getTableWriter(schema,tableName) {
    return new TableWriter(this,schema,tableName,this.statementCache[tableName],this.status,this.logWriter);      
  }
  
  async insertBatch(sqlStatement,batch) {
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
    }
    const result = await this.pgClient.query(sqlStatement,batch)
    return result;
  }
  
  async finalizeDataLoad() {
  }  

}

module.exports = DBInterface
