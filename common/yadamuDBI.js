"use strict" 
const fs = require('fs');
const path = require('path');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const Yadamu = require('./yadamu.js');
const YadamuRejectManager = require('./yadamuRejectManager.js');
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
  
  processLog(log,status,yadamuLogger) {

    const logDML         = (status.loglevel && (status.loglevel > 0));
    const logDDL         = (status.loglevel && (status.loglevel > 1));
    const logDDLMsgs     = (status.loglevel && (status.loglevel > 2));
    const logTrace       = (status.loglevel && (status.loglevel > 3));

    if (status.dumpFileName) {
      fs.writeFileSync(status.dumpFileName,JSON.stringify(log));
    }
    
    log.forEach(function(result) {
                  const logEntryType = Object.keys(result)[0];
                  const logEntry = result[logEntryType];
                  switch (true) {
                    case (logEntryType === "message") : 
                      yadamuLogger.log([`${this.constructor.name}`],`: ${logEntry}.`)
                      break;
                    case (logEntryType === "dml") : 
                      yadamuLogger.log([`${this.constructor.name}`],`: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elaspsed Time ${Yadamu.stringifyDuration(Math.round(logEntry.elapsedTime))}s. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.`)
                      break;
                    case (logEntryType === "info") :
                      yadamuLogger.info([`${this.constructor.name}`],`"${JSON.stringify(logEntry)}".`);
                      break;
                    case (logDML && (logEntryType === "dml")) :
                      yadamuLogger.log([`${this.constructor.name}`],`: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.`)
                      break;
                    case (logDDL && (logEntryType === "ddl")) :
                      yadamuLogger.log([`${this.constructor.name}`],`: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.`) 
                      break;
                    case (logTrace && (logEntryType === "trace")) :
                      yadamuLogger.trace([`${this.constructor.name}`],`${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".\n' : '\n'}${logEntry.sqlStatement}.`)
                      break;
                    case (logEntryType === "error"):
   	                switch (true) {
   		              case (logEntry.severity === 'FATAL') :
                        yadamuLogger.error([`${this.constructor.name}`,`${logEntry.severity}`],`${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}\n${logEntry.sqlStatement}`)
   				        break
   					  case (logEntry.severity === 'WARNING') :
                        yadamuLogger.warning([`${this.constructor.name}`,`${logEntry.severity}`],`${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
                        break;
  					  case (logEntry.severity === 'CONTENT_TOO_LARGE') :
                        yadamuLogger.error([`${this.constructor.name}`,`${logEntry.severity}`],`${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} This database does not support VARCHAR2 values longer than ${this.maxStringSize} bytes.`)
                        break;
    			      case (logEntry.severity === 'SQL_TOO_LARGE') :
                        yadamuLogger.error([`${this.constructor.name}`,`${logEntry.severity}`],`${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} This database is not configured for DLL statements longer than ${this.maxStringSize} bytes.`)
                        break;
                      case (logDDLMsgs) :
                        yadamuLogger.log([`${this.constructor.name}`,`${logEntry.severity}`],`${logEntry.tableName ? 'Table: "' + logEntry.tableName  + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
                        break;
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

  configureTest(connectionProperties,testParameters,defaultParameters,tableMappings) {
    this.connectionProperties = connectionProperties
    this.parameters = this.yadamu.getParameters()
    this.parameters = this.updateParameters(this.parameters,testParameters);
    this.parameters = this.mergeParameters(this.parameters,defaultParameters);
    if (this.parameters.MAPPINGS) {
      this.loadTableMappings(this.parameters.MAPPINGS);
    }  
    else {
      this.tableMappings = tableMappings
    }
    if (this.parameters.SQLTRACE) {
	  this.status.sqlTrace = fs.createWriteStream(this.parameters.SQLTRACE,{flags : "a"});
    }
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
  
  loadTableMappings(mappingFile) {
    this.tableMappings = require(path.resolve(mappingFile));
  }

  reverseTableMappings() {

    if (this.tableMappings !== undefined) {
      const reverseMappings = {}
      Object.keys(this.tableMappings).forEach(function(table) {
        const newKey = this.tableMappings[table].tableName
        reverseMappings[newKey] = { "tableName" : table};
        if (this.tableMappings[table].columns) {
          const columns = {};
          Object.keys(this.tableMappings[table].columns).forEach(function(column) {
            const newKey = this.tableMappings[table].columns[column]
            columns[newKey] = column;
          },this);
          reverseMappings[newKey].columns = columns
        }
      },this)
      return reverseMappings;
    }
    return this.tableMappings;
  }
    
  applyTableMappings() {
    
    const tables = Object.keys(this.metadata)
    tables.forEach(function(table) {
      const tableMappings = this.tableMappings[table]
      if (tableMappings) {
        this.metadata[table].tableName = tableMappings.tableName
        if (tableMappings.columns) {
          const columns = JSON.parse('[' + this.metadata[table].columns + ']');
          Object.keys(tableMappings.columns).forEach(function(columnName) {
            const idx = columns.indexOf(columnName);
            if (idx > -1) {
              columns[idx] = tableMappings.columns[columnName]                
            }
          },this);
          this.metadata[table].columns = '"' + columns.join('","') + '"';
        }
      }   
    },this);   
  }
  
  validateIdentifiers() {
  }
  
  setMetadata(metadata) {
    this.metadata = metadata
    if (this.tableMappings) {
      this.applyTableMappings()
    }
    else {
      this.validateIdentifiers()
    }
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
        this.yadamuLogger.logException([`${this.constructor.name}.executeDDL()`],e)
        this.yadamuLogger.writeDirect(`${ddlStatement}\n`)
      } 
    },this))
  }
  
  createRejectManager() {
    const rejectFolderPath = this.parameters.REJECT_FOLDER ? this.parameters.REJECT_FOLDER : 'work/rejected';
    this.rejectFilename = path.resolve(rejectFolderPath +  path.sep + 'yadamuRejected_' + new Date().toISOString().split(':').join('') + ".json");
    return new YadamuRejectManager(this.rejectFilename,this.yadamuLogger);
  }
    
  constructor(yadamu,defaultParameters) {
    
    // In production mode the Databae default parameters are merged with the command Line Parameters loaded by YADAMU.
        
    this.yadamu = yadamu;
    this.parameters = yadamu.getParameters()
    this.parameters = this.mergeParameters(this.parameters,defaultParameters);
    this.status = yadamu.getStatus()
    this.yadamuLogger = yadamu.getYadamuLogger();
    
    this.systemInformation = undefined;
    this.metadata = undefined;

    this.connectionProperties = this.getConnectionProperties()   
    this.connection = undefined;

    this.statementCache = undefined;
 
    this.tableName  = undefined;
    this.tableInfo  = undefined;
    this.insertMode = 'Empty';
    this.skipTable = true;
    this.skipCount = 0;

    this.tableMappings = undefined;
    if (this.parameters.MAPPINGS) {
      this.loadTableMappings(this.parameters.MAPPINGS);
    }   
 
    this.rejectManager = this.createRejectManager()

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
  ** Begin the current transaction
  **
  */
  
  async beginTransaction() {
  }

  /*
  **
  ** Commit the current transaction
  **
  */
    
  async commitTransaction() {
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
    return new DBParser(query,objectMode,this.yadamuLogger);      
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
    const statementGenerator = new StatementGenerator(this,schema,this.metadata,this.parameters.BATCHSIZE,this.parameters.COMMITSIZE, this.status, this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL,this.systemInformation.vendor)
  }

  getTableWriter(TableWriter,table) {
    this.skipCount = 0;    
    const tableName = this.metadata[table].tableName 
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger);
  }
 
  async finalizeDataLoad() {
    throw new Error('Unimplemented Method')
  }  
  
  async exportComplete() {
  }

  async importComplete() {
    this.rejectManager.close();
  }
  
  rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    this.rejectManager.rejectRow(tableName,row);
  }
  
  handleInsertError(operation,tableName,batchSize,row,record,err,info) {
     this.skipCount++;
     this.status.warningRaised = true;
     this.yadamuLogger.logRejected([`${operation}`,`"${tableName}"`,`${batchSize}`,`${row}`],err);
     this.rejectRow(tableName,record);
     info.forEach(function (info) {
       this.yadamuLogger.writeDirect(`${info}\n`);
     },this)

     const abort = (this.skipCount === ( this.parameters.MAX_ERRORS ? this.parameters.MAX_ERRORS : 10)) 
     if (abort) {
       this.yadamuLogger.error([`${operation}`,`"${tableName}"`],`Maximum Error Count exceeded. Skipping Table.`);
     }
     return abort;
     
  }
}

module.exports = YadamuDBI
