"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const Yadamu = require('./yadamu.js');
const DBParser = require('./dbParser.js');

/*
**
** YADAMU Database Inteface class 
**
*/

class YadamuDBI {
    
  get DATABASE_VENDOR()     { return undefined };
  get SOFTWARE_VENDOR()     { return undefined };
  get SPATIAL_FORMAT()      { return undefined };
  get DEFAULT_PARAMETERS()  { return {} }
  get STATEMENT_SEPERATOR() { return '' }
  
  
  decomposeDataType(targetDataType) {
    
    const results = {};
    let components = targetDataType.split('(');
    results.type = components[0].split(' ')[0];
    if (components.length > 1 ) {
      components = components[1].split(')');
      if (components.length > 1 ) {
        results.qualifier = components[1]
      }
      components = components[0].split(',');
      if (components.length > 1 ) {
        results.length = parseInt(components[0]);
        results.scale = parseInt(components[1]);
      }
      else {
        if (components[0] === 'max') {
          results.length = -1;
        }
        else {
          results.length = parseInt(components[0])
        }
      }
    }           
    return results;      
    
  } 
  processLog(log,status,logWriter) {

    const logDML         = (status.loglevel && (status.loglevel > 0));
    const logDDL         = (status.loglevel && (status.loglevel > 1));
    const logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    const logTrace       = (status.loglevel && (status.loglevel > 3));
   
    if (status.dumpFileName) {
      fs.writeFileSync(status.dumpFileName,JSON.stringify(log));
    }
    
    log.forEach(function(result) {
                  const logEntryType = Object.keys(result)[0];
                  const logEntry = result[logEntryType];
                  switch (true) {
                    case (logEntryType === "message") : 
                      logWriter.write(`${new Date().toISOString()}: ${logEntry}.\n`)
                      break;
                    case (logEntryType === "dml") : 
                      logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elaspsed Time ${Math.round(logEntry.elapsedTime)}ms. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.\n`)
                      break;
                    case (logEntryType === "info") :
                      logWriter.write(`${new Date().toISOString()}[INFO]: "${JSON.stringify(logEntry)}".\n`);
                      break;
                    case (logDML && (logEntryType === "dml")) :
                      logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.\n`)
                      break;
                    case (logDDL && (logEntryType === "ddl")) :
                      logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.\n`) 
                      break;
                    case (logTrace && (logEntryType === "trace")) :
                      logWriter.write(`${new Date().toISOString()} [TRACE]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".\n' : '\n'}${logEntry.sqlStatement}.\n`)
                      break;
                    case (logEntryType === "error"):
   	                switch (true) {
   		              case (logEntry.severity === 'FATAL') :
                          status.errorRaised = true;
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}\n${logEntry.sqlStatement}\n`)
   				        break
   					  case (logEntry.severity === 'WARNING') :
                          status.warningRaised = true;
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}\n`)
                          break;
   					  case (logEntry.severity === 'CONTENT_TOO_LARGE') :
                          status.errorRaised = true;
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: Cannot import columns larger than 32k in 12c.\n`)
                          break;
                        case (logDDLIssues) :
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName  + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}\n`)
                      } 	
                  } 
   				if ((status.sqlTrace) && (logEntry.sqlStatement)) {
   				  status.sqlTrace.write(`${logEntry.sqlStatement}\n${this.STATEMENT_SEPERATOR}\n`)
   		        }
    },this) 
  }    
    
  updateParameters(parameters,additionalParameters) {
    Object.keys(additionalParameters).forEach(function(key) {
      parameters[key] = additionalParameters[key]
    },this)
    return parameters
  }

  mergeParameters(parameters,additionalParameters) {
    Object.keys(additionalParameters).forEach(function(key) {
      if (!parameters.hasOwnProperty(key)) {
        parameters[key] = additionalParameters[key]
      }
    },this)
    return parameters
  }

  configureTest(connectionProperties,testParameters,defaultParameters,) {
    this.connectionProperties = connectionProperties
    this.parameters = this.yadamu.getParameters()
    this.parameters = this.updateParameters(this.parameters,testParameters);
    this.parameters = this.mergeParameters(this.parameters,defaultParameters);
  }

  setConnectionProperties(connectionProperties) {
    this.connectionProperties = connectionProperties
  }
  
  getConnectionProperties() {
    this.connectionProperties = {}
  }
  
  isValidDDL() {
    return (this.systemInformation.vendor === this.DATABASE_VENDOR)
  }
  
  isDatabase() {
    return true;
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
  
  async executeDDL(ddl) {
    await Promise.all(ddl.map(async function(ddlStatement) {
      try {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TOUSER);
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`${ddlStatement};\n--\n`);
        }
        this.executeSQL(ddlStatement,{});
      } catch (e) {
        this.logWriter.write(`${e}\n${tableInfo.ddl}\n`)
      } 
    },this))
  }
    
  constructor(yadamu,defaultParameters) {
    
    // In production mode the Databae default parameters are merged with the command Line Parameters loaded by YADAMU.
        
    this.yadamu = yadamu;
    this.parameters = yadamu.getParameters()
    this.parameters = this.mergeParameters(this.parameters,defaultParameters);
    this.status = yadamu.getStatus()
    this.logWriter = yadamu.getLogWriter();
    
    this.systemInformation = undefined;
    this.metadata = undefined;

    this.connectionProperties = this.getConnectionProperties()   
    this.connection = undefined;

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
    if (this.status.sqlTrace) {
       if (this.status.sqlTrace._writableState.ended === true) {
         this.status.sqlTrace = fs.createWriteStream(this.status.sqlTrace.path,{"flags":"a"})
       }
    }
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    throw new Error('Unimplemented Method')
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    throw new Error('Unimplemented Method')
  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
    throw new Error('Unimplemented Method')
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
    throw new Error('Unimplemented Method')
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
    throw new Error('Unimplemented Method')
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  async processFile(hndl) {
    throw new Error('Unimplemented Method')
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
    throw new Error('Unimplemented Method')
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    return undefined
  }
  
  async getSchemaInfo() {
    return []
  }

  generateMetadata(tableInfo,server) {    
    return {}
  }
   
  generateSelectStatement(tableMetadata) {
     return tableMetadata;
  }   

  createParser(query,objectMode) {
    return new DBParser(query,objectMode,this.logWriter);      
  }
  
  async getInputStream(query,parser) {
    throw new Error('Unimplemented Method')
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
  
  async initializeDataLoad() {
    throw new Error('Unimplemented Method')
  }
  
  async generateStatementCache(StatementGenerator,schema,executeDDL) {
    const statementGenerator = new StatementGenerator(this,schema,this.metadata,this.parameters.BATCHSIZE,this.parameters.COMMITSIZE, this.status, this.logWriter);
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL,this.systemInformation.vendor)
  }

  getTableWriter(TableWriter,table) {
    const tableName = this.metadata[table].tableName 
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.logWriter);      
  }
 
  async finalizeDataLoad() {
    throw new Error('Unimplemented Method')
  }  

}

module.exports = YadamuDBI
