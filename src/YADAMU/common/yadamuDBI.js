"use strict" 
const fs = require('fs');
const path = require('path');
const Readable = require('stream').Readable;
const util = require('util')
const { performance } = require('perf_hooks');const async_hooks = require('async_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const Yadamu = require('./yadamu.js');
const DBIConstants = require('./dbiConstants.js');
const YadamuLibrary = require('./yadamuLibrary.js')
const {YadamuError, InternalError, CommandLineError, ConfigurationFileError, ConnectionError, DatabaseError} = require('./yadamuError.js');
const DefaultParser = require('./defaultParser.js');
const DummyWritable = require('./nullWritable.js')

/*
**
** YADAMU Database Interface class 
**
**
*/

class YadamuDBI {

  // Instance level getters.. invoke as this.METHOD

  get PIPELINE_OPERATION_HANGS()   {return false }

  get DATABASE_VENDOR()            { return undefined };
  get SOFTWARE_VENDOR()            { return undefined };
  
  get PASSWORD_KEY_NAME()          { return 'password' };
  get STATEMENT_TERMINATOR()       { return '' }

  get SPATIAL_FORMAT()             { return this.parameters.SPATIAL_FORMAT   || DBIConstants.SPATIAL_FORMAT };
  get TABLE_MAX_ERRORS()           { return this.parameters.TABLE_MAX_ERRORS || DBIConstants.TABLE_MAX_ERRORS };
  get TOTAL_MAX_ERRORS()           { return this.parameters.TOTAL_MAX_ERRORS || DBIConstants.TOTAL_MAX_ERRORS };
  get COMMIT_RATIO()               { return this.parameters.COMMIT_RATIO     || DBIConstants.COMMIT_RATIO };

  get BATCH_SIZE() {
    this._BATCH_SIZE = this._BATCH_SIZE || (() => {
      let batchSize =  this.parameters.BATCH_SIZE || DBIConstants.BATCH_SIZE
      batchSize = isNaN(batchSize) ? this.this.BATCH_SIZE : batchSize
      batchSize = Math.abs(Math.ceil(batchSize))
      return batchSize
    })();
    return this._BATCH_SIZE 
  }

  get COMMIT_COUNT() {    
    this._COMMIT_COUNT = this._COMMIT_COUNT || (() => {
      let commitCount = isNaN(this.COMMIT_RATIO) ? DBIConstants.COMMIT_RATIO : this.COMMIT_RATIO
      commitCount = Math.abs(Math.ceil(commitCount))
      commitCount = commitCount * this.BATCH_SIZE
      return commitCount
    })();
    return this._COMMIT_COUNT
  }
  
  // Override based on local parameters object ( which under the test harnesss may differ from the one obtained from yadamu in the constructor).
  
  get FILE()                          { return this.parameters.FILE     || this.yadamu.FILE }
  get MODE()                          { return this.parameters.MODE     || this.yadamu.MODE }
  get ON_ERROR()                      { return this.parameters.ON_ERROR || this.yadamu.ON_ERROR }
  get PARALLEL()                      { return this.parameters.PARALLEL || this.yadamu.PARALLEL }
  
  get EXCEPTION_FOLDER()              { return this.parameters.FILE     || this.yadamu.EXCEPTION_FOLDER }
  get EXCEPTION_FILE_PREFIX()         { return this.parameters.FILE     || this.yadamu.EXCEPTION_FILE_PREFIX }
  get REJECTION_FOLDER()              { return this.parameters.FILE     || this.yadamu.REJECTION_FOLDER }
  get REJECTION_FILE_PREFIX()         { return this.parameters.FILE     || this.yadamu.REJECTION_FILE_PREFIX }
  get WARNING_FOLDER()                { return this.parameters.FILE     || this.yadamu.WARNING_FOLDER }
  get WARNING_FILE_PREFIX()           { return this.parameters.FILE     || this.yadamu.WARNING_FILE_PREFIX }

  get TABLE_FILTER()                  { return this.parameters.TABLES || [] }
  
  get INPUT_METRICS()                 { return this._INPUT_METRICS }
  set INPUT_METRICS(v) {
	this._INPUT_METRICS =  Object.assign({},v);
  }
  
  get ATTEMPT_RECONNECTION() {
    this._ATTEMPT_RECONNECTION = this._ATTEMPT_RECONNECTION || (() => {
      switch (this.ON_ERROR) {
	    case undefined:
	    case 'ABORT':
  		  return false;
	    case 'SKIP':
	    case 'FLUSH':
	      return true;
	    default:
	      return false;
      }
    })();
    return this._ATTEMPT_RECONNECTION
  }

  // Not available until configureConnection() has been called 

  get DB_VERSION()             { return this._DB_VERSION }

  get SPATIAL_SERIALIZER()                    { return this._SPATIAL_SERIALIZER }
  set SPATIAL_SERIALIZER(v)                   { this._SPATIAL_SERIALIZER = v }
   
  constructor(yadamu,parameters) {
  
    this.options = {
      recreateTargetSchema : false
    }
    
    this._DB_VERSION = 'N/A'    
    this.yadamu = yadamu;
    this.sqlTraceTag = '';
    this.status = yadamu.STATUS
    this.yadamuLogger = yadamu.LOGGER;
    this.initializeParameters(parameters);
    this.systemInformation = undefined;
    this.metadata = undefined;
    this.connectionProperties = this.getConnectionProperties()   
    this.connection = undefined;
    this.reconnectInProgress = false
	
    this.statementCache = undefined;
	
	// Track Transaction and Savepoint state.
	// Needed to restore transacation state when reconnecting.
	
	this.transactionInProgress = false;
	this.savePointSet = false;
 
    this.tableName  = undefined;
    this.tableInfo  = undefined;
    this.insertMode = 'Empty';
    this.skipTable = true;

    this.setTableMappings(undefined);
    if (this.parameters.MAPPINGS) {
      this.loadTableMappings(this.parameters.MAPPINGS);
    }   
 
    this.sqlTraceTag = `/* Manager */`;	
    this.sqlCumlativeTime = 0
    this.sqlTerminator = `\n${this.STATEMENT_TERMINATOR}\n`
	this.firstError = undefined
	this.latestError = undefined    
    
  }
  
  applyTableFilter(tableName) { 
    return ((this.TABLE_FILTER.length === 0) || this.TABLE_FILTER.includes(tableName))
  }
  
  traceSQL(msg) {
     // this.yadamuLogger.trace([this.DATABASE_VENDOR,'SQL'],msg)
     return(`${msg.trim()} ${this.sqlTraceTag} ${this.sqlTerminator}`);
  }
  
  traceTiming(startTime,endTime) {      
    const sqlOperationTime = endTime - startTime;
    this.status.sqlTrace.write(`--\n-- ${this.sqlTraceTag} Elapsed Time: ${YadamuLibrary.stringifyDuration(sqlOperationTime)}s.\n--\n`);
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,'SQL'],`${this.sqlTraceTag} Elapsed Time: ${YadamuLibrary.stringifyDuration(sqlOperationTime)}s`)
    this.sqlCumlativeTime = this.sqlCumlativeTime + sqlOperationTime
  }
 
  traceComment(comment) {
    return `/* ${comment} */\n`
  }


  stringToJSON(value) {
    // Poor man's test for JSON Object or Array
    if ((typeof value === "string") && ((value.indexOf('{') === 0) || (value.indexOf('[') === 0))) {
	  try {
	    return JSON.parse(value)
	  } catch (e) {
		return value
      }
	}
	else {
	  // Convert Buffers to Hex
      if (Buffer.isBuffer(value)) {
		return value.toString('hex')
	  }
	  else {
	    return value
	  }
	}
  }

  doTimeout(milliseconds) {
    
	return new Promise((resolve,reject) => {
        this.yadamuLogger.info([`${this.constructor.name}.doTimeout()`],`Sleeping for ${YadamuLibrary.stringifyDuration(milliseconds)}ms.`);
        setTimeout(
          () => {
           this.yadamuLogger.info([`${this.constructor.name}.doTimeout()`],`Awake.`);
           resolve();
          },
          milliseconds
       )
     })  
  }
  

  
  processError(yadamuLogger,logEntry,summary,logDDL) {
	 
	let warning = true;
	  
    switch (logEntry.severity) {
      case 'CONTENT_TOO_LARGE' :
        yadamuLogger.error([`${this.DATABASE_VENDOR}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`This database does not support VARCHAR2 values longer than ${this.MAX_STRING_SIZE} bytes.`)
        return;
      case 'SQL_TOO_LARGE':
        yadamuLogger.error([`${this.DATABASE_VENDOR}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`This database is not configured for DLL statements longer than ${this.MAX_STRING_SIZE} bytes.`)
        return;
      case 'FATAL':
        summary.errors++
		const err =  new Error(logEntry.msg)
		err.SQL = logEntry.sqlStatement
		err.details = logEntry.details
		summary.exceptions.push(err)
        // yadamuLogger.error([`${this.DATABASE_VENDOR}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''}`],`Details: ${logEntry.msg}\n${logEntry.details}\n${logEntry.sqlStatement}`)
        return
      case 'WARNING':
        summary.warnings++
        break;
      case 'IGNORE':
        summary.warnings++
        break;
      case 'DUPLICATE':
        summary.duplicates++
        break;
      case 'REFERENCE':
        summary.reference++
        break;
      case 'AQ RELATED':
        summary.aq++
        break;
      case 'RECOMPILATION':
        summary.recompilation++
        break;
      default:
	    warning = false
    }
    if (logDDL) { 
	  if (warning) {
        yadamuLogger.warning([`${this.DATABASE_VENDOR}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''}`],`Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
	  }
	  else {
        yadamuLogger.ddl([`${this.DATABASE_VENDOR}`,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''}`],`Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
	  }
	}
  }
          
  processLog(log, operation, status,yadamuLogger) {

    const logDML         = (status.loglevel && (status.loglevel > 0));
    const logDDL         = (status.loglevel && (status.loglevel > 1));
    const logDDLMsgs     = (status.loglevel && (status.loglevel > 2));
    const logTrace       = (status.loglevel && (status.loglevel > 3));

    if (status.logTrace) {
      yadamuLogger.writeLogToFile([this.DATABASE_VENDOR],log);
    }
     
    const summary = {
       errors        : 0
      ,warnings      : 0
      ,ignoreable    : 0
      ,duplicates    : 0
      ,reference     : 0
      ,aq            : 0
      ,recompilation : 0
	  ,exceptions    : []
    };
      	  
	log.forEach((result) => { 
      const logEntryType = Object.keys(result)[0];
      const logEntry = result[logEntryType];
      switch (true) {
        case (logEntryType === "message") : 
          yadamuLogger.info([`${this.DATABASE_VENDOR}`],`${logEntry}.`)
          break;
        case (logEntryType === "dml") : 
          yadamuLogger.info([`${logEntry.tableName}`,`SQL`],`Rows ${logEntry.rowCount}. Elaspsed Time ${YadamuLibrary.stringifyDuration(Math.round(logEntry.elapsedTime))}s. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.`)
          break;
        case (logEntryType === "info") :
          yadamuLogger.info([`${this.DATABASE_VENDOR}`],`"${JSON.stringify(logEntry)}".`);
          break;
        case (logDML && (logEntryType === "dml")) :
          yadamuLogger.dml([`${this.DATABASE_VENDOR}`,`${logEntry.tableName}`,`${logEntry.tableName}`],`\n${logEntry.sqlStatement}.`)
          break;
        case (logDDL && (logEntryType === "ddl")) :
          yadamuLogger.ddl([`${this.DATABASE_VENDOR}`,`${logEntry.tableName}`],`\n${logEntry.sqlStatement}.`) 
          break;
        case (logTrace && (logEntryType === "trace")) :
          yadamuLogger.trace([`${this.DATABASE_VENDOR}`,`${logEntry.tableName ? logEntry.tableName : ''}`],`\n${logEntry.sqlStatement}.`)
          break;
        case (logEntryType === "error"):
		  this.processError(yadamuLogger,logEntry,summary,logDDLMsgs);
      } 
      if (logEntry.sqlStatement) { 
        status.sqlTrace.write(this.traceSQL(logEntry.sqlStatement))
      }
    }) 
	
    if (summary.exceptions.length > 0) {
  	  const err = new Error(`${this.DATABASE_VENDOR} ${operation} failed.`);
	  err.causes = summary.exceptions
      throw err
    }
	return summary;
  }    

  logDisconnect() {
    const pwRedacted = Object.assign({},this.connectionProperties)
    delete pwRedacted.password
    this.status.sqlTrace.write(this.traceComment(`DISCONNECT : Properies: ${JSON.stringify(pwRedacted)}`))
  }
  
  logConnectionProperties() {    
    const pwRedacted = Object.assign({},this.connectionProperties)
    delete pwRedacted.password
    this.status.sqlTrace.write(this.traceComment(`CONNECT : Properies: ${JSON.stringify(pwRedacted)}`))
  }
     
  setConnectionProperties(connectionProperties) {
	if (!YadamuLibrary.isEmpty(connectionProperties)) {
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
  
  captureException(err) {
    // Reset by passing undefined 
    this.firstError = this.firstError === undefined ? err : this.firstError
	this.latestError = err
    return err
  }	
   
  invalidConnection() {
	return ((this.latestError instanceof DatabaseError) && (this.latestError.lostConnection() || this.latestError.serverUnavailable()))
  }
  
  setMetrics(metrics) {
	// Share metrics with the Writer so adjustments can be made if the connection is lost.
    this.metrics = metrics
  }
  
  trackLostConnection() {
   
    /*
    **
    ** Invoked by the DBI when the connection is lost. Assume a rollback took place. Any rows written but not committed are lost. 
    **
    */

  	if ((this.metrics !== undefined) && (this.metrics.lost  !== undefined) && (this.metrics.written  !== undefined)) {
      this.metrics.lost += this.metrics.written;
	  this.metrics.written = 0;
	}
  }	  
  
  setSystemInformation(systemInformation) {
    this.systemInformation = systemInformation
  }
  
  setMetadata(rawMetadata) {

    /*
	**
	** Apply current tableMappings to the metadata
    ** Check the result does not required further transformation	
	** Apply additional transformations as required
	**
	*/

    const patchedMetadata = this.tableMappings ? this.applyTableMappings(rawMetadata,this.tableMappings) : rawMetadata
	const generatedMappings = this.validateIdentifiers(patchedMetadata)
    // ### TODO Improve logic for merging generatedMappings with existing tableMappings - Make sure column mappings are merged correctly
    this.setTableMappings((this.tableMappings || generatedMappings) ? Object.assign({},this.tableMappings,generatedMappings) : undefined)
	this.metadata = this.tableMappings ? this.applyTableMappings(patchedMetadata,this.tableMappings) : patchedMetadata

  }
  
  setParameters(parameters) {
    Object.assign(this.parameters, parameters ? parameters : {})
    // Force ATTEMPT_RECONNECTION to be re-evaluated next time it is used.
    this._ATTEMPT_RECONNECTION = undefined
    this._COMMIT_COUNT = undefined
  }
  
  /*
  **
  ** ### TABLE MAPPINGS ###
  **
  ** Some databases have restrictions on the length of names or the characters that can appear in names
  ** Table Mappings provide a mechanism to map non-compliant names to compliant names. Mappings apply to both table names and column names.
  **
  ** The application of Table Mappings is bi-directional. When importing data the DBI should apply Table Mappings to table names and columns names
  ** before attempting to insert data into a database. When exporting data the DBI should apply TableMappings to a the content of the metadata and 
  ** data objects generated by the export process.
  ** 
  ** Table Mappings are not applied when generating DDL statements as part of an export operation or when processing DDL statements during an import operation
  **
  ** Most YADAMU interfaces will generate conforming names from unconformrnt names by truncation. Truncation is a very crude solution
  ** as it can lead to clashes, and meaningless names. The preferred solution is to provide a mappings that contains the desired mappings.
  **
  ** function setTableMappings() sets the TableMappings object to be used by the DBI
  **
  ** function loadMappingsFile() loads the TableMappings object from a file disk. The file is specified using the TABLE_MAPPINGS parameter.
  ** 
  ** function validateIdentifiers() is a placeholder that the DBI can override if it needs to validate identifiers and generate a TableMappings object
  ** The default function returns undefined indicating that no mappings are required.
  ** 
  ** function getTableMappings() returns the current TableMappings object
  **
  ** function reverseTableMappings() generates the inverse mappings for a given TableMappings object. Mappings supplied to the DBI are treated as inbound. 
  ** E.g. they map names generated by external sources to names that compliant with the target database. 
  **
  ** function transformMetadata() uses a reversed TableMappings object to modify the contents of metadata objects emitted by the DBI during an export operation.
  **
  ** function transformTableName() uses a reversed TableMappings object to modify the contes of talbe objects emitted by the DBI during an export operations
  **
  */

  setTableMappings(tableMappings) {
    this.tableMappings = tableMappings
	this.inverseTableMappings = this.reverseTableMappings(tableMappings)
  }
  
  loadTableMappings(mappingFile) {
	mappingFile = path.resolve(mappingFile)
    this.yadamuLogger.info([this.DATABASE_VENDOR,'MAPPINGS'],`Using mappings file "${mappingFile}".`)
    this.setTableMappings(YadamuLibrary.loadJSON(mappingFile,this.yadamuLogger))
  }

  validateIdentifiers(metadata) {
	this.setTableMappings(undefined)
  }
  
  getTableMappings() {
	return this.tableMappings
  }

  getInverseTableMappings() {
	return this.inverseTableMappings
  }

  reverseTableMappings(tableMappings) {

    if (tableMappings) {
      const reverseMappings = {}
      Object.keys(tableMappings).forEach((table) => {
        const newKey = tableMappings[table].tableName
        reverseMappings[newKey] = { "tableName" : table};
        if (tableMappings[table].columnNames) {
          const columnNames = {};
          Object.keys(tableMappings[table].columnNames).forEach((column) => {
            const newKey = tableMappings[table].columnNames[column]
            columnNames[newKey] = column;
          });
          reverseMappings[newKey].columnNames = columnNames
        }
      })
      return reverseMappings;
    }
    return tableMappings;
  }
    
  applyTableMappings(metadata,mappings) {
	  
	// This function does not change the names of the keys in the metadata object.
	// It only changes the value of the tableName property associated with a mapped tables.
    const tables = Object.keys(metadata).map((key) => {
      return metadata[key].tableName
	})
    tables.forEach((table) => {
      const tableMappings = mappings[table]
      if (tableMappings) {
		if (this.DATABASE_VENDOR,metadata[table].tableName !== tableMappings.tableName) {
          this.yadamuLogger.info([this.DATABASE_VENDOR,metadata[table].tableName],`Table mapped to "${tableMappings.tableName}".`)
		  metadata[table].tableName = tableMappings.tableName
		}
        if (tableMappings.columnNames) {
          const columnNames = metadata[table].columnNames
          Object.keys(tableMappings.columnNames).forEach((columnName,cidx) => {
            const idx = columnNames.indexOf(columnName);
            if (idx > -1) {
              this.yadamuLogger.info([this.DATABASE_VENDOR,metadata[table].tableName,columnName],`Column mapped to "${tableMappings.columnNames[columnName]}".`)
              columnNames[idx] = tableMappings.columnNames[columnName]          
            }
          });
          metadata[table].columnNames = columnNames
        }
      }   
    });
    return metadata	
  }
  
  transformTableName(tableName,mappings) {
	return (mappings && mappings.hasOwnProperty(tableName)) ? mappings[tableName].tableName : tableName
  }
  
  transformMetadata(metadata,mappings) {
    if (mappings) {
      const mappedMetadata = this.applyTableMappings(metadata,mappings)
	  const outboundMetadata = {}
	  Object.keys(mappedMetadata).forEach((tableName) => { outboundMetadata[this.transformTableName(tableName,mappings)] = mappedMetadata[tableName] })
	  return outboundMetadata
	}
	else {
      return metadata
	}
  }
	  
  async _executeDDL(ddl) {
	let results
	try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
		this.status.sqlTrace.write(this.traceSQL(ddlStatement));
        return this.executeSQL(ddlStatement,{});
      }))
    } catch (e) {
	 this.yadamuLogger.handleException([this.DATABASE_VENDOR,'DDL'],e)
	 results = e;
    }
    return results;
  }
  
  prepareDDLStatements(ddlStatements) {
	return ddlStatements
  }
  
  async executeDDL(ddl) {
	if (ddl.length > 0) {
      const startTime = performance.now();
      const results = await this._executeDDL(ddl);
	  return results
	}
	return []
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
    Object.assign(this.parameters, this.yadamu.COMMAND_LINE_PARAMETERS);
    
  }
  
  enablePerformanceTrace() { 
    const self = this;
    this.asyncHook = async_hooks.createHook({
      init(asyncId, type, triggerAsyncId, resource) {self.reportAsyncOperation(asyncId, type, triggerAsyncId, resource)}
    }).enable();
  }

  reportAsyncOperation(...args) {
     fs.writeFileSync(this.parameters.PERFORMANCE_TRACE, `${util.format(...args)}\n`, { flag: 'a' });
  }
  
  async _getDatabaseConnection() {
    try {
      await this.createConnectionPool();
      this.connection = await this.getConnectionFromPool();
      await this.configureConnection();
    } catch (e) {
      const err = new ConnectionError(e,this.connectionProperties);
      throw err
    }

  }  

  waitForRestart(delayms) {
    return new Promise((resolve, reject) => {
        setTimeout(resolve, delayms);
    });
  }
  
  abortOnError() {
	return !this.ATTEMPT_RECONNECTION
  }
    
  async _reconnect() {
    throw new Error(`Database Reconnection Not Implimented for ${this.DATABASE_VENDOR}`)
	
	// Default code for databases that support reconnection
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()

  }
  
  async reconnect(cause,operation) {

    let retryCount = 0;
    let connectionUnavailable 
    
    const transactionInProgress = this.transactionInProgress 
    const savePointSet = this.savePointSet
	
	this._ATTEMPT_RECONNECTION = false
    this.reconnectInProgress = true;
	this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,`${operation}`],cause)
	
	/*
	**
	** If a connection is lost while performing batched insert operatons using a table writer, adjust the table writers running total of records written but not committed. 
	** When a connection is lost records that have written but not committed will be lost (rolled back by the database) when cleaning up after the lost connection.
	** Table Writers invoke trackCounters and pass a counter object to the database interface before consuming rows in order for this to work correctly.
	** To avoid the possibility of lost batches set COMMIT_RATIO to 1, so each batch is committed as soon as it is written.
	**
	*/
	
    this.trackLostConnection();
	
    while (retryCount < 10) {
		
      /*
      **
      ** Attempt to close the connection. Handle but do not throw any errors...
      **
      */	
	
	  try {
        await this.closeConnection()
      } catch (e) {
	    if (!e.invalidConnection()) {
          this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`RECONNECT`],`Error closing existing connection.`);
		  this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,`RECONNECT`],e)
	    }
	  }	 
		 
	  try {
        this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`RECONNECT`],`Attemping reconnection.`);
        await this._reconnect()
	    await this.configureConnection();
		if (transactionInProgress) {
		  await this.beginTransaction()
		}
		if (this.savePointSet) {
		  await this.createSavePoint()
		}
        this.reconnectInProgress = false;
        this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`RECONNECT`],`New connection available.`);
        this._ATTEMPT_RECONNECTION = undefined
		return;
      } catch (connectionFailure) {
		if ((typeof connectionFailure.serverUnavailable == 'function') && connectionFailure.serverUnavailable()) {
		  connectionUnavailable = connectionFailure;
          this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`RECONNECT`],`Waiting for restart.`)
          await this.waitForRestart(5000);
          retryCount++;
        }
        else {
   	      this.reconnectInProgress = false;
          this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,`RECONNECT`],connectionFailure);
          this._ATTEMPT_RECONNECTION = undefined
          throw cause;
        }
      }
    }
    // this.yadamuLogger.trace([`${this.constructor.name}._reconnect()`],`Unable to re-establish connection.`)
    this.reconnectInProgress = false;
    this.ATTEMPT_RECONNECTION = undefined
    throw connectionUnavailable 	
  }
  
  async getDatabaseConnection(requirePassword) {
    let interactiveCredentials = (requirePassword && ((this.connectionProperties[this.PASSWORD_KEY_NAME] === undefined) || (this.connectionProperties[this.PASSWORD_KEY_NAME].length === 0))) 
    let retryCount = interactiveCredentials ? 3 : 1;
    
    let prompt = `Enter password for ${this.DATABASE_VENDOR} connection: `
    while (retryCount > 0) {
      retryCount--
      if (interactiveCredentials)  {
        const pwQuery = this.yadamu.createQuestion(prompt);
        const password = await pwQuery;
        this.connectionProperties[this.PASSWORD_KEY_NAME] = password;
      }
      try {
        await this._getDatabaseConnection()  
        return;
      } catch (e) {     
        switch (retryCount) {
          case 0: 
            if (interactiveCredentials) {
              throw new CommandLineError(`Unable to establish connection to ${this.DATABASE_VENDOR} after 3 attempts. Operation aborted.`);
              break;
            }
            else {
              throw (e)
            }
            break;
          case 1:
            console.log(`Connection Error: ${e.message}`)
            break;
          case 2:           
            prompt = `Unable to establish connection. Re-${prompt}`;
            console.log(`Database Error: ${e.message}`)
            break;
          default:
            throw e
        }
      } 
    }
  }
    
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */

  async initialize(requirePassword) {

    this.yadamu.initializeSQLTrace();  
    /*
    **
    ** Calculate CommitSize
    **
    */
    
    if (this.parameters.PARAMETER_TRACE === true) {
      this.yadamuLogger.writeDirect(`${util.inspect(this.parameters,{colors:true})}\n`);
    }
    
    if (this.parameters.PERFORMANCE_TRACE) {
      this.enablePerformanceTrace();
    }
    
    if (this.isDatabase()) {
      await this.getDatabaseConnection(requirePassword);
    }
  }

  /*
  **
  **  Gracefully close down the database connection and pool.
  **
  */

  async releaseWorkerConnection() {
	await this.closeConnection()
  }

  async releasePrimaryConnection() {
	// Defer until finalize()
	// await this.closeConnection()
  }
  
  async finalize(options) {
	// this.yadamuLogger.trace([this.constructor.name,`finalize(${poolOptions})`],'')
	options = options === undefined ? {abort: false} : Object.assign(options,{abort:false})
    await this.closeConnection(options)
    await this.closePool(options);
	this.logDisconnect();
  }

  /*
  **
  **  Abort the database connection and pool
  **
  */
  
  lostConnection(e) {
	
	return((e instanceof DatabaseError) && e.lostConnection())
	
  }

  async abort(e,options) {
	
	// this.yadamuLogger.trace([this.constructor.name,`abort(${poolOptions})`],'')

	// Log all errors other than lost connection errors. Do not throw otherwise underlying cause of the abort will be lost. 
				
	options = options === undefined ? {abort: true} : Object.assign(options,{abort:true})
    options.err = e
				
    try {
      await this.closeConnection(options);
	} catch (e) {
	  if (!this.lostConnection(e)) {
        this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,'ABORT','Connection'],e);
	  }
	}
	
    try {
	  // Force Termnination of All Current Connections.
	  await this.closePool(options);
	} catch (e) {
	  if (!this.lostConnection(e)) {
        this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,'ABORT','Pool'],e);
	  }
	}
	
	this.logDisconnect();
	
  }
  
  checkConnectionState(cause) {
	 
	// Throw cause if cause is a lost connection. Used by drivers to prevent attempting rollback or restore save point operations when the connection is lost.
	  
  	if ((cause instanceof DatabaseError) && cause.lostConnection()) {
      throw cause;
	}
  }

  checkCause(operation,cause,newError) {
	 
	 // Used by Rollback and Restore save point to log errors encountered while performing the required operation and throw the original cause.

	  if (cause instanceof Error) {
        this.yadamuLogger.handleException([this.DATABASE_VENDOR,operation],newError)
	    throw cause
	  }
	  throw newError
  }

  /*
  **
  ** Begin the current transaction
  **
  */
  
  beginTransaction() {
    this.transactionInProgress = true;  
	this.savePointSet = false;
  }

  /*
  **
  ** Commit the current transaction
  **
  */
    
  commitTransaction() {
	this.transactionInProgress = false;  
	this.savePointSet = false;
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  rollbackTransaction(cause) {
	this.transactionInProgress = false;  
	this.savePointSet = false;
  }
  
  /*
  **
  ** Set a Save Point
  **
  */
    
  createSavePoint() {
	this.savePointSet = true;
  }

  /*
  **
  ** Revert to a Save Point
  **
  */

  restoreSavePoint(cause) {
	this.savePointSet = false;
  }

  releaseSavePoint(cause) {
	this.savePointSet = false;
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
  
  async getSystemInformation() {     
    throw new Error('Unimplemented Method')
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    // Undefined means database does not provide mechanism to obtain DDL statements. Different to returning an empty Array.
    return undefined
  }
  
  generateSchemaInfo(schemaInfo) {
	this.schemaInfo = schemaInfo.map((table) => {
	  return {
	    TABLE_SCHEMA          : table[0]
	  , TABLE_NAME            : table[1]
	  , COLUMN_NAME_ARRAY     : table[2]
	  , DATA_TYPE_ARRAY       : table[3]
	  , SIZE_CONSTRAINT_ARRAY : table[4]
	  , CLIENT_SELECT_LIST    : table[5]
	  }
    })
	return this.schemaInfo;
  }
  
  generateMetadata(schemaInformation) {   

    if (this.TABLE_FILTER.length > 0)  {
      this.yadamuLogger.info([this.DATABASE_VENDOR],`Operations restricted to the following tables: ${JSON.stringify(this.TABLE_FILTER)}.`)
    }
   
    const metadata = {}
    schemaInformation.forEach((table,idx) => {
      table.COLUMN_NAME_ARRAY     = typeof table.COLUMN_NAME_ARRAY     === 'string' ? JSON.parse(table.COLUMN_NAME_ARRAY)     : table.COLUMN_NAME_ARRAY
      table.DATA_TYPE_ARRAY       = typeof table.DATA_TYPE_ARRAY       === 'string' ? JSON.parse(table.DATA_TYPE_ARRAY)       : table.DATA_TYPE_ARRAY
      table.SIZE_CONSTRAINT_ARRAY = typeof table.SIZE_CONSTRAINT_ARRAY === 'string' ? JSON.parse(table.SIZE_CONSTRAINT_ARRAY) : table.SIZE_CONSTRAINT_ARRAY
      table.INCLUDE_TABLE = this.applyTableFilter(table.TABLE_NAME)
      if (table.INCLUDE_TABLE) {
        const tableMetadata =  {
          tableSchema              : table.TABLE_SCHEMA
         ,tableName                : table.TABLE_NAME
         ,columnNames              : table.COLUMN_NAME_ARRAY
         ,dataTypes                : table.DATA_TYPE_ARRAY
         ,sizeConstraints          : table.SIZE_CONSTRAINT_ARRAY
        }
        metadata[table.TABLE_NAME] = tableMetadata
      }
    }) 
	return metadata
  }  
  
  async getSchemaInfo(keyName) {
    
    /*
    ** Returns an array of information about each table in the schema being exported.
    **
    ** The following item are mandatory, since they are required to build the "metadata" object that forms part of the YADAMU export file 
    ** and which is used as the starting point when for database to database copy operations.
    ** 
    ** TABLE_SCHEEMA, TABLE_NAME, COLUMN_NAME_ARRAY, DATA_TYPE_ARRAY, SIZE_CONSTRAINT_ARRAY
    **
    ** The Arrays are expected to be valid JSON arrays.
    **
    ** The query may also return additional information about the SQL that should be used to retieve the data from the schema
    **
    ** Implimentations should provde a custom impliemtnation of generateMetadata() if they need more than the minimum set of information about the schema.
    **
    */
          
    throw new Error('Unimplemented Method')
    return []
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
  }

  async finalizeImport() {
  }
    
  async generateStatementCache(StatementGenerator,schema) {
	const statementGenerator = new StatementGenerator(this,schema,this.metadata,this.systemInformation.spatialFormat,this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache()
	return this.statementCache
  }

  async finalizeRead(tableInfo) {
  }
  
  getTableInfo(tableName) {
	  
	// Statement Cache is keyed by actual table name so we need the mapped name if there is a mapping.

    if (this.statementCache === undefined) {
      this.yadamuLogger.logInternalError([this.constructor.name,`getTableInfo()`,tableName],`Statement Cache undefined. Cannot obtain required information.`)
	}
	
	let mappedTableName = this.transformTableName(tableName,this.tableMappings)
	const tableInfo = this.statementCache[mappedTableName]
	 
	if (tableInfo === undefined) {
      this.yadamuLogger.logInternalError([this.constructor.name,`getTableInfo()`,tableName,mappedTableName],`No Statement Cache entry for "${mappedTableName}". Current entries: ${JSON.stringify(Object.keys(this.statementCache))}`)
	}
	
	tableInfo.tableName = mappedTableName
	return tableInfo
  }

  generateQueryInformation(tableMetadata) {
    const tableInfo = Object.assign({},tableMetadata);   
	tableInfo.SQL_STATEMENT = `select ${tableMetadata.CLIENT_SELECT_LIST} from "${tableMetadata.TABLE_SCHEMA}"."${tableMetadata.TABLE_NAME}" t`; 
	
    // ### TESTING ONLY: Uncomment folllowing line to force Table Not Found condition
    // tableInfo.SQL_STATEMENT = tableInfo.SQL_STATEMENT.replace(tableInfo.TABLE_NAME,tableInfo.TABLE_NAME + "1")

	tableInfo.MAPPED_TABLE_NAME = this.transformTableName(tableInfo.TABLE_NAME,this.getInverseTableMappings())
    return tableInfo
  }   

  createParser(tableInfo) {
    return new DefaultParser(tableInfo,this.yadamuLogger);      
  }
 
  streamingError(cause,sqlStatement) {
    return this.captureException(new DatabaseError(cause,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(tableInfo) {
	throw new Error('Unimplemented Method')
	this.streamingStackTrace = new Error().stack;
	return inputStream;
  }      

  async getInputStreams(tableInfo) {
	const streams = []
	this.INPUT_METRICS = DBIConstants.NEW_TIMINGS
	const inputStream = await this.getInputStream(tableInfo)
    inputStream.once('readable',() => {
	  this.INPUT_METRICS.readerStartTime = performance.now()
	}).on('error',(err) => { 
      this.INPUT_METRICS.readerEndTime = performance.now()
	  this.INPUT_METRICS.readerError = err
	  this.INPUT_METRICS.failed = true;
    }).on('end',() => {
      this.INPUT_METRICS.readerEndTime = performance.now()
    })
	streams.push(inputStream)
	
	const parser = this.createParser(tableInfo)
	parser.once('readable',() => {
	  this.INPUT_METRICS.parserStartTime = performance.now()
	}).on('end',() => {
	  this.INPUT_METRICS.parserEndTime = performance.now()
	  this.INPUT_METRICS.rowsRead = parser.getRowCount()
	}).on('error',(err) => {
	  this.INPUT_METRICS.parserEndTime = performance.now()
	  this.INPUT_METRICS.rowsRead = parser.getRowCount()
	  this.INPUT_METRICS.parserError = err
	  this.INPUT_METRICS.failed = true;
	})
	
	streams.push(parser)
    return streams;
  }
  
  getOutputStream(TableWriter,tableName,ddlComplete) {
    // this.yadamuLogger.trace([this.constructor.name,`getOutputStream(${tableName})`],'')
    const os = new TableWriter(this,tableName,ddlComplete,this.status,this.yadamuLogger)
	return os;
  }
  
  getOutputStreams(tableName,ddlComplete) {
	return [this.getOutputStream(tableName,ddlComplete)]
    // return [this.getOutputStream(tableName,ddlComplete),new DummyWritable()]
  }
  
  keepAlive(rowCount) {
  }

  configureTest(recreateSchema) {
    if (this.parameters.MAPPINGS) {
      this.loadTableMappings(this.parameters.MAPPINGS);
    }  
    if (recreateSchema === true) {
      this.setOption('recreateSchema',true);
    }
  }

  reloadStatementCache() {
    if (!this.isManager()) {
      this.statementCache = this.manager.statementCache
	}	 
  }
  

  isManager() {

    return (this.workerNumber === undefined)
   
  }

  getWorkerNumber() {

    return this.isManager() ? 'Manager' : this.workerNumber

  }
  
  classFactory() {
	 throw new Error(` Parallel operations not supported. Class Factory implementation not provided for "${this.constructor.name}". Cannot create worker.`)
  }
  
  async setWorkerConnection() {
    // DBI implementations that do not use a pool / connection mechansim need to overide this function. eg MSSQLSERVER
	this.connection = await this.manager.getConnectionFromPool()	
  }

  async cloneCurrentSettings(manager) {
	this.StatementLibrary   = manager.StatementLibrary
	this.StatementGenerator = manager.StatementGenerator
	
	this.systemInformation  = manager.systemInformation
    this.metadata           = manager.metadata
    this.schemaCache        = manager.schemaCache
    this.statementGenerator = manager.statementGenerator
    
    this.setParameters(manager.parameters);
	this.setTableMappings(manager.tableMappings)
  }   

  async workerDBI(workerNumber) {
      
    // Invoked on the DBI that is being cloned. Parameter dbi is the cloned interface.
	
	const dbi = this.classFactory(this.yadamu)  
	dbi.manager = this
    dbi.workerNumber = workerNumber
    dbi.sqlTraceTag = ` /* Worker [${dbi.getWorkerNumber()}] */`;
	await dbi.setWorkerConnection()
	await dbi.configureConnection();
	await dbi.cloneCurrentSettings(this);
	return dbi
  }
  
  testLostConnection() {
	const supportedModes = ['DATA_ONLY','DDL_AND_DATA']
    return (
	         (supportedModes.indexOf(this.MODE) > -1)
	         && 
			 (
			   ((this.PARALLEL === undefined) || (this.PARALLEL < 1))
			   ||
			   ((this.PARALLEL > 1) && (this.workerNumber !== undefined) && (this.workerNumber === this.parameters.KILL_WORKER_NUMBER))
			 )
			 && 
			 (
		       (this.parameters.FROM_USER && this.parameters.KILL_READER_AFTER && (this.parameters.KILL_READER_AFTER > 0)) 
		       || 
			   (this.parameters.TO_USER && this.parameters.KILL_WRITER_AFTER && (this.parameters.KILL_WRITER_AFTER > 0))
		     )
		   ) === true
  }

  
  async getConnectionID() {
	// ### Get a Unique ID for the connection
    throw new Error('Unimplemented Method')
  }
  
  
}

module.exports = YadamuDBI
