"use strict" 
const fs = require('fs');
const path = require('path');
const Readable = require('stream').Readable;
const util = require('util')
const async_hooks = require('async_hooks');
/* 
**
** Require Database Vendors API 
**
*/

const YadamuLibrary = require('./yadamuLibrary.js');
const YadamuRejectManager = require('./yadamuRejectManager.js');
const DBParser = require('./dbParser.js');

const DEFAULT_BATCH_SIZE   = 10000;
const DEFAULT_COMMIT_COUNT = 5;

/*
**
** YADAMU Database Inteface class 
**
*/

class YadamuDBI {
    
  get PASSWORD_KEY_NAME()   { return 'password' };
  get DATABASE_VENDOR()     { return undefined };
  get SOFTWARE_VENDOR()     { return undefined };
  get SPATIAL_FORMAT()      { return spatialFormat };
  get DEFAULT_PARAMETERS()  { return this.yadamu.getYadamuDefaults().yadmuDBI }
  get STATEMENT_SEPERATOR() { return '' }
  
  doTimeout(milliseconds) {
    
     const self = this

     return new Promise(function (resolve,reject) {
        self.yadamuLogger.info([`${self.constructor.name}.doTimeout()`],`Sleeping for ${YadamuLibrary.stringifyDuration(milliseconds)}ms.`);
        setTimeout(
          function() {
           self.yadamuLogger.info([`${self.constructor.name}.doTimeout()`],`Awake.`);
		   resolve();
          },
          milliseconds
       )
     })  
  }
 
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
  
  decomposeDataTypes(targetDataTypes) {
	 return targetDataTypes.map(function (targetDataType) {
       return this.decomposeDataType(targetDataType)
	 },this)
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
                      yadamuLogger.log([`${this.constructor.name}`,`${logEntry.tableName}`],`Rows ${logEntry.rowCount}. Elaspsed Time ${YadamuLibrary.stringifyDuration(Math.round(logEntry.elapsedTime))}s. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.`)
                      break;
                    case (logEntryType === "info") :
                      yadamuLogger.info([`${this.constructor.name}`],`"${JSON.stringify(logEntry)}".`);
                      break;
                    case (logDML && (logEntryType === "dml")) :
                      yadamuLogger.log([`${this.constructor.name}`,`${logEntry.tableName}`,`${logEntry.tableName}`],`\n${logEntry.sqlStatement}.`)
                      break;
                    case (logDDL && (logEntryType === "ddl")) :
                      yadamuLogger.log([`${this.constructor.name}`,`${logEntry.tableName}`],`\n${logEntry.sqlStatement}.`) 
                      break;
                    case (logTrace && (logEntryType === "trace")) :
                      yadamuLogger.trace([`${this.constructor.name}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`\n${logEntry.sqlStatement}.`)
                      break;
                    case (logEntryType === "error"):
   	                switch (true) {
   		              case (logEntry.severity === 'FATAL') :
                        yadamuLogger.error([`${this.constructor.name}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`Details: ${logEntry.msg}\n${logEntry.details}\n${logEntry.sqlStatement}`)
   				        break
   					  case (logEntry.severity === 'WARNING') :
                        yadamuLogger.warning([`${this.constructor.name}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
                        break;
  					  case (logEntry.severity === 'CONTENT_TOO_LARGE') :
                        yadamuLogger.error([`${this.constructor.name}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`This database does not support VARCHAR2 values longer than ${this.maxStringSize} bytes.`)
                        break;
    			      case (logEntry.severity === 'SQL_TOO_LARGE') :
                        yadamuLogger.error([`${this.constructor.name}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`This database is not configured for DLL statements longer than ${this.maxStringSize} bytes.`)
                        break;
                      case (logDDLMsgs) :
                        yadamuLogger.log([`${this.constructor.name}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
                        break;
                    } 	
                  } 
   				if ((status.sqlTrace) && (logEntry.sqlStatement)) {
   				  status.sqlTrace.write(`${logEntry.sqlStatement}\n${this.STATEMENT_SEPERATOR}\n`)
   		        }
    },this) 
  }    


  setParameters(parameters) {
	 Object.assign(this.parameters, parameters ? parameters : {})
  }
  
  setTableMappings(tableMappings) {
	this.tableMappings = tableMappings
  }

  logConnectionProperties() {    
    if (this.status.sqlTrace) {
      const pwRedacted = Object.assign({},this.connectionProperties)
      delete pwRedacted.password
      this.status.sqlTrace.write(`--\n-- Connection Properies: ${JSON.stringify(pwRedacted)}\n--\n`)
    }
  }
     
  setConnectionProperties(connectionProperties) {
	if (Object.getOwnPropertyNames(connectionProperties).length > 0) {	  
      this.connectionProperties = connectionProperties 
	}
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
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
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
  
  setOption(name,value) {
    this.options[name] = value;
  }
	
  initializeParameters(parameters) {
    
    // In production mode the Databae default parameters are merged with the command Line Parameters loaded by YADAMU.

    this.parameters = this.yadamu.cloneDefaultParameters();
    
    // Merge parameters from configuration files
    Object.assign(this.parameters, parameters ? parameters : {})

    // Merge Command line arguments
    Object.assign(this.parameters, this.yadamu.getCommandLineParameters());
    
  }

  reportAsyncOperation(...args) {
	 fs.writeFileSync(this.parameters.PERFORMANCE_TRACE, `${util.format(...args)}\n`, { flag: 'a' });
  }
  
  enablePerformanceTrace() {
 
    if (this.parameters.PERFORMANCE_TRACE) {
      const self = this;
      this.asyncHook = async_hooks.createHook({
        init(asyncId, type, triggerAsyncId, resource) {self.reportAsyncOperation(asyncId, type, triggerAsyncId, resource)}
      }).enable();
	}
  }
  

  
  constructor(yadamu,parameters) {
    
    this.options = {
	  recreateTargetSchema : false
	}
    
	this.spatialFormat = this.SPATIAL_FORMAT 
    this.yadamu = yadamu;
    this.status = yadamu.getStatus()
    this.yadamuLogger = yadamu.getYadamuLogger();
    this.initializeParameters(parameters);
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
  
  async initialize(ensurePassword) {
	this.enablePerformanceTrace();
    if (this.status.sqlTrace) {
       if (this.status.sqlTrace._writableState.ended === true) {
         this.status.sqlTrace = fs.createWriteStream(this.status.sqlTrace.path,{"flags":"a"})
       }
    }
    
    /*
    **
    ** Calculate CommitSize
    **
    */
    
    let batchSize = this.parameters.BATCH_SIZE ? Number(this.parameters.BATCH_SIZE) : DEFAULT_BATCH_SIZE
    batchSize = isNaN(batchSize) ? DEFAULT_BATCH_SIZE : batchSize
    batchSize = batchSize < 0 ? DEFAULT_BATCH_SIZE : batchSize
    batchSize = !Number.isInteger(batchSize) ? DEFAULT_BATCH_SIZE : batchSize
    this.batchSize = batchSize
    
    let commitCount = this.parameters.BATCH_COMMIT ? Number(this.parameters.BATCH_COMMIT) : DEFAULT_COMMIT_COUNT
    commitCount = isNaN(commitCount) ? DEFAULT_COMMIT_COUNT : commitCount
    commitCount = commitCount < 0 ? DEFAULT_COMMIT_COUNT : commitCount
    commitCount = !Number.isInteger(commitCount) ? DEFAULT_COMMIT_COUNT : commitCount
    this.commitSize = this.batchSize * commitCount
    
    if (this.parameters.PARAMETER_TRACE === true) {
      this.yadamuLogger.writeDirect(`${util.inspect(this.parameters,{colors:true})}\n`);
    }
	    
    if (ensurePassword) {
      await this.yadamu.ensurePassword(this.DATABASE_VENDOR, this.connectionProperties, this.PASSWORD_KEY_NAME);
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
  
  async initializeExport() {
  }
  
  async finalizeExport() {
  }
  
  /*
  **
  ** The following methods are used by the YADAMU DBWriter class
  **
  */
  
  async initializeImport() {
  }
  
  async initializeData() {
  }
  
  async finalizeData() {
    this.rejectManager.close();
  }

  async finalizeImport() {
  }
    
  async generateStatementCache(StatementGenerator,schema,executeDDL) {
    const statementGenerator = new StatementGenerator(this,schema,this.metadata,this.systemInformation.spatialFormat,this.batchSize, this.commitSize, this.status, this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL,this.systemInformation.vendor)
  }

  getTableWriter(TableWriter,table) {
    this.skipCount = 0;    
    const tableName = this.metadata[table].tableName 
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger);
  }
 
  rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    this.rejectManager.rejectRow(tableName,row);
  }
  
  async handleInsertError(operation,tableName,batchSize,row,record,err,info) {
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
  
  keepAlive(rowCount) {
  }

  configureTest(recreateSchema) {
    if (this.parameters.MAPPINGS) {
      this.loadTableMappings(this.parameters.MAPPINGS);
    }  
    if (this.parameters.SQL_TRACE) {
	  this.status.sqlTrace = fs.createWriteStream(this.parameters.SQL_TRACE,{flags : "a"});
    }
	if (recreateSchema === true) {
  	  this.setOption('recreateSchema',true);
    }
  }
	
}

module.exports = YadamuDBI
