"use strict" 

import fs                 from 'fs';
import path               from 'path';

import { 
  performance 
}                         from 'perf_hooks';
import EventEmitter       from 'events'

import {
  setTimeout 
}                         from 'timers/promises'

import Yadamu             from '../../core/yadamu.js';
import YadamuConstants    from '../../lib/yadamuConstants.js';
import YadamuLibrary      from '../../lib/yadamuLibrary.js'
import NullWriteable      from '../../util/nullWritable.js'

import {
  YadamuError, 
  InternalError, 
  CommandLineError, 
  ConfigurationFileError, 
  ConnectionError, 
  DatabaseError, 
  BatchInsertError, 
  IterativeInsertError, 
  InputStreamError, 
  UnimplementedMethod
}                         from '../../core/yadamuException.js';

import DBIConstants       from './dbiConstants.js';

import {
  FileNotFound, 
  FileError
}                         from '../file/fileException.js';


/*
**
** YADAMU Database Interface class 
**
**
*/
  
class YadamuDBI extends EventEmitter {

  // Instance level getters.. invoke as this.METHOD


  get DATABASE_KEY()                 { return 'yadamu' };
  get DATABASE_VENDOR()              { return 'YADAMU' };
  get SOFTWARE_VENDOR()              { return 'YABASC - Yet Another Bay Area Software Company'};

  get DRIVER_ID()                    { return this._DRIVER_ID }
  set DRIVER_ID(v)                   { this._DRIVER_ID = v }
  
  get DATA_STAGING_SUPPORTED()       { return false } 
  get SQL_COPY_OPERATIONS()          { return false }
  get PARALLEL_READ_OPERATIONS()     { return true };
  get PARALLEL_WRITE_OPERATIONS()    { return true }
  get PARALLEL_OPERATIONS()          { return this.PARALLEL_READ_OPERATIONS && this.PARALLEL_WRITE_OPERATIONS }
  
  get PASSWORD_KEY_NAME()            { return 'password' };
  get STATEMENT_TERMINATOR()         { return ';' }
  get STATEMENT_SEPERATOR()          { return '\n--\n' }
  
  get SPATIAL_FORMAT()               { return this.parameters.SPATIAL_FORMAT              || DBIConstants.SPATIAL_FORMAT };
  get PARSE_DELAY()                  { return this.parameters.PARSE_DELAY                 || DBIConstants.PARSE_DELAY };
  get TABLE_MAX_ERRORS()             { return this.parameters.TABLE_MAX_ERRORS            || DBIConstants.TABLE_MAX_ERRORS };
  get TOTAL_MAX_ERRORS()             { return this.parameters.TOTAL_MAX_ERRORS            || DBIConstants.TOTAL_MAX_ERRORS };
  get COMMIT_RATIO()                 { return this.parameters.hasOwnProperty('COMMIT_RATIO') ?  this.parameters.COMMIT_RATIO : DBIConstants.COMMIT_RATIO };
  get MODE()                         { return this.parameters.MODE                        || DBIConstants.MODE }
  get ON_ERROR()                     { return this.parameters.ON_ERROR                    || DBIConstants.ON_ERROR }
  get INFINITY_MANAGEMENT()          { return this.parameters.INFINITY_MANAGEMENT         || DBIConstants.INFINITY_MANAGEMENT };
  get LOCAL_STAGING_AREA()           { return YadamuLibrary.macroSubstitions((this.parameters.LOCAL_STAGING_AREA     || DBIConstants.LOCAL_STAGING_AREA || ''), this.yadamu.MACROS || '') }
  get REMOTE_STAGING_AREA()          { return YadamuLibrary.macroSubstitions((this.parameters.REMOTE_STAGING_AREA    || DBIConstants.REMOTE_STAGING_AREA || ''), this.yadamu.MACROS || '') }
  get STAGING_FILE_RETENTION()       { return this.parameters.STAGING_FILE_RETENTION      || DBIConstants.STAGING_FILE_RETENTION }
  get TIMESTAMP_PRECISION()          { return this.parameters.TIMESTAMP_PRECISION         || DBIConstants.TIMESTAMP_PRECISION }
  get BYTE_TO_CHAR_RATIO()           { return this.parameters.BYTE_TO_CHAR_RATIO          || DBIConstants.BYTE_TO_CHAR_RATIO }
  get RETRY_COUNT()                  { return 3 }
  get IDENTIFIER_TRANSFORMATION()    { return this.yadamu.IDENTIFIER_TRANSFORMATION }
  get PARTITION_LEVEL_OPERATIONS()   { return this.parameters.PARTITION_LEVEL_OPERATIONS  || DBIConstants.PARTITION_LEVEL_OPERATIONS }
  
  get RECONNECT_ON_ABORT()           { const retVal = this._RECONNECT_ON_ABORT; this._RECONNECT_ON_ABORT = false; return retVal }
  set RECONNECT_ON_ABORT(v)          { this._RECONNECT_ON_ABORT = v }

  get BATCH_SIZE() {
    this._BATCH_SIZE = this._BATCH_SIZE || (() => {
      let batchSize =  this.parameters.BATCH_SIZE || DBIConstants.BATCH_SIZE
      batchSize = isNaN(batchSize) ? this.parameters.BATCH_SIZE : batchSize
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
  get PARALLEL()                      { return this.parameters.PARALLEL === 0 ? 0 : (this.parameters.PARALLEL || this.yadamu.PARALLEL)}
  
  get EXCEPTION_FOLDER()              { return this.parameters.FILE     || this.yadamu.EXCEPTION_FOLDER }
  get EXCEPTION_FILE_PREFIX()         { return this.parameters.FILE     || this.yadamu.EXCEPTION_FILE_PREFIX }
  get REJECTION_FOLDER()              { return this.parameters.FILE     || this.yadamu.REJECTION_FOLDER }
  get REJECTION_FILE_PREFIX()         { return this.parameters.FILE     || this.yadamu.REJECTION_FILE_PREFIX }
  get WARNING_FOLDER()                { return this.parameters.FILE     || this.yadamu.WARNING_FOLDER }
  get WARNING_FILE_PREFIX()           { return this.parameters.FILE     || this.yadamu.WARNING_FILE_PREFIX }
  
  get TRANSACTION_IN_PROGRESS()       { return this._TRANSACTION_IN_PROGRESS === true }
  set TRANSACTION_IN_PROGRESS(v)      { this._TRANSACTION_IN_PROGRESS = v }
  
  get RECONNECT_IN_PROGRESS()         { return this._RECONNECT_IN_PROGRESS === true }
  set RECONNECT_IN_PROGRESS(v)        { this._RECONNECT_IN_PROGRESS = v }
  
  get SAVE_POINT_SET()                { return this._SAVE_POINT_SET === true }
  set SAVE_POINT_SET(v)               { this._SAVE_POINT_SET = v }

  // get ATTEMPT_RECONNECTION()          { return ((this.ON_ERROR !== 'ABORT' || this.RECONNECT_ON_ABORT)) && !this.RECONNECT_IN_PROGRESS}
  get ATTEMPT_RECONNECTION()          { return !this.RECONNECT_IN_PROGRESS}

  get SOURCE_DIRECTORY()              { return this.parameters.SOURCE_DIRECTORY || this.parameters.DIRECTORY }
  get TARGET_DIRECTORY()              { return this.parameters.TARGET_DIRECTORY || this.parameters.DIRECTORY }

  get IS_READER()                     { return this.parameters.hasOwnProperty('FROM_USER') && !this.parameters.hasOwnProperty('TO_USER') }
  get IS_WRITER()                     { return !this.parameters.hasOwnProperty('FROM_USER') && this.parameters.hasOwnProperty('TO_USER') }

  get CURRENT_SCHEMA()                { 
    this._CURRENT_SCHEMA = this._CURRENT_SCHEMA || (() => {
      switch (true) { 
	    case this.IS_READER: 
		  return this.parameters.FROM_USER; 
		case this.IS_WRITER: 
		  return this.parameters.TO_USER; 
		default: 
		  return undefined 
	  }
    })();
    return this._CURRENT_SCHEMA 
  }
  
  get ROLE()                          {                     
    this._ROLE = this._ROLE || (() => {
      switch (true) { 
	    case this.IS_READER: 
		  return YadamuConstants.READER_ROLE; 
		case this.IS_WRITER: 
		  return YadamuConstants.WRITER_ROLE; 
		default: 
		  return undefined 
	  }
    })();
    return this._ROLE  
  }
  
  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.STAGING_UNSUPPORTED }

  // Not available until configureConnection() has been called 

  get DB_VERSION()                    { return this._DB_VERSION }

  get SPATIAL_SERIALIZER()            { return this._SPATIAL_SERIALIZER }
  set SPATIAL_SERIALIZER(v)           { this._SPATIAL_SERIALIZER = v }
   
  get INBOUND_SPATIAL_FORMAT()        { return this.systemInformation?.typeMappings?.spatialFormat || this.SPATIAL_FORMAT};
  get INBOUND_CIRCLE_FORMAT()         { return this.systemInformation?.typeMappings?.circleFormat || null};

  get TABLE_FILTER()                  { 
    this._TABLE_FILTER || this._TABLE_FILTER || (() => {
	  const tableFilter =  typeof this.parameters.TABLES === 'string' ? this.loadTableList(this.parameters.TABLES) : (this.parameters.TABLES || [])
	  // Filter for Unqiueness just to be safe.
	  this._TABLE_FILTER =  tableFilter.filter((value,index) => {
	    return tableFilter.indexOf(value) === index
	  }) 
    })();
    return this._TABLE_FILTER
  }
  
  get UPLOAD_FILE()                     {

    /*
	**
	** Rules for File Location are as follows
	**
	
	Parameter FILE is absolute: FILE
    OTHERWISE: 
	
	  Parameter DIRECTORY is not supplied: conn:directory/FILE
	  OTHERWISE
    
        Paramter DIRECTORY is absolute: DIRECTORY/FILE
	    OTHERWISE: conn:directory/DIRECTORY/FILE
	
	**
	*/
	
    return this._UPLOAD_FILE || (() => {
	  let file =  this.parameters.FILE || 'yadamu.json'
	  if (!path.isAbsolute(file)) {
   	    file = path.join(this.SOURCE_DIRECTORY,file)
	  }
	  file = YadamuLibrary.macroSubstitions(file,this.yadamu.MACROS)
	  this._UPLOAD_FILE = path.resolve(file)
	  return this._UPLOAD_FILE
    })()
  }		
     
  get DESCRIPTION()                   { return this._DESCRIPTION }
  set DESCRIPTION(v)                  { this._DESCRIPTION = v }

  get FEEDBACK_MODEL()     { return this._FEEDBACK_MODEL }
  get FEEDBACK_INTERVAL()  { return this._FEEDBACK_INTERVAL }
  get FEEDBACK_DISABLED()  { return this.FEEDBACK_MODEL === undefined }
  
  get REPORT_COMMITS()     { 
    return this._REPORT_COMMITS || (() => { 
	  this._REPORT_COMMITS = ((this._FEEDBACK_MODEL === 'COMMIT') || (this._FEEDBACK_MODEL === 'ALL')); 
	  return this._REPORT_COMMITS
	})() 
  }
  
  get REPORT_BATCHES()     { 
     return this._REPORT_BATCHES || (() => { 
	   this._REPORT_BATCHES = ((this._FEEDBACK_MODEL === 'BATCH') || (this._FEEDBACK_MODEL === 'ALL')); 
	   return this._REPORT_BATCHES
	 })() 
  }
  
  set FEEDBACK_MODEL(feedback)  {
    if (!isNaN(feedback)) {
	  this._FEEDBACK_MODEL    = 'ALL'
	  this._FEEDBACK_INTERVAL = parseInt(feedback)
	}
	else {
	  this._FEEDBACK_MODEL    = feedback
	  this._FEEDBACK_INTERVAL = 0
    }
  } 
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    
	super()
	this.DRIVER_ID = performance.now()
	
	yadamu.activeConnections.add(this)

    this.yadamu = yadamu;
    this.setConnectionProperties(connectionSettings || {})
    this.status = yadamu.STATUS
    this.yadamuLogger = yadamu.LOGGER;
	this.initializeParameters(parameters || {});
    this.FEEDBACK_MODEL = this.parameters.FEEDBACK

    this.options = {
      recreateSchema : false
    }

    this._DB_VERSION = 'N/A'    
    this.sqlTraceTag = '';
    
    this.vendorProperties = this.getVendorProperties()   

	this.systemInformation = undefined;
    this.metadata = undefined;
    this.connection = undefined;	
    this.statementCache = undefined;
	
	// Track Transaction and Savepoint state.
	// Needed to restore transacation state when reconnecting.
	
    this.RECONNECT_IN_PROGRESS = false
	this.TRANSACTION_IN_PROGRESS = false;
	this.SAVE_POINT_SET = false;
    this.DESCRIPTION = this.getSchemaIdentifer()
	
    this.tableInfo  = undefined;
    this.insertMode = 'Batch'
    this.skipTable = true;

    this.sqlTraceTag = `/* Manager */`;	
    this.sqlCumlativeTime = 0
	this.firstError = undefined
	this.latestError = undefined    
	
	this.ddlInProgress = false;
    this.activeWriters = new Set()
    
    if (manager) {
	  this.workerReady = new Promise((resolve,reject) => {
	    this.initializeWorker(manager).then(() => { resolve() }).catch((e) => { console.log(e); reject(e) })
      })
	}
	else {
      this.initializeManager() 
	}
	
	this.failedPrematureClose = false;

  }
  
  initializeManager() {
    this.activeWorkers = new Set()
  
	// dbConnected will resolve when the database connection has been established
	
    this.dbConnected = new Promise((resolve,reject) => {
	  if (this.isDatabase()) {
        this.once(YadamuConstants.DB_CONNECTED,() => {
		  resolve(true)
	    })
	  }
	  else {
		resolve(true)
	  }
    })
   
	  // cacheLoaded will resolve when the statement Cache has been generated 
	
    this.cacheLoaded = new Promise((resolve,reject) => {
      this.once(YadamuConstants.CACHE_LOADED  ,() => {
	    resolve(true)
	  })
    })
    
    this.ddlComplete = new Promise((resolve,reject) => {
	  this.once(YadamuConstants.DDL_COMPLETE,(startTime,state) => {
        // this.yadamuLogger.trace([this.constructor.name],`${this.constructor.name}.on(ddlComplete): (${state instanceof Error}) "${state ? `${state.constructor.name}(${state.message})` : state}"`)
   	    this.ddlInProgress = false
	    if (state instanceof Error) {
	      this.yadamuLogger.ddl([this.DATABASE_VENDOR],`One or more DDL operations Failed. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
		  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'DDL'],state);
		  state.ignoreUnhandledRejection = true;
          reject(state)
        }
		else {
    	  this.yadamuLogger.ddl([this.DATABASE_VENDOR],`Executed ${Array.isArray(state) ? state.length : undefined} DDL operations. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
		  if (this.PARTITION_LEVEL_OPERATIONS) {
		    // Need target schema metadata to determine if we can perform partition level write operations.
			this.getSchemaMetadata().then((metadata) => { this.partitionMetadata = this.getPartitionMetadata(metadata) ;resolve(true) }).catch((e) => { reject(e) })
		  }
		  else {
		    resolve(true);
		  }
	    }
	  })
	  this.once(YadamuConstants.DDL_UNNECESSARY,() => {
	    resolve(true)
	  })
	})
  }

  async initializeWorker(manager) {
    this.manager = manager
	this.workerNumber = -1
    this.cloneSettings();
	 
	// Set up a promise to make it possible to wait on AllSettled..	  
    this.workerState = new Promise((resolve,reject) => {
      this.on(YadamuConstants.DESTROYED,() => {
	    manager.activeWorkers.delete(this)
		resolve(YadamuConstants.DESTROYED)
	  })
	})
      
	manager.activeWorkers.add(this)
	  
	await this.setWorkerConnection()
	await this.configureConnection();
  }
  
  loadTableList(tableListPath) {
	try {
      const tableList = YadamuLibrary.loadJSON(tableListPath,this.yadamuLogger) 
      if (Array.isArray(tableList)) {
		return tableList
	  }
	  throw new CommandLineError(`Expected a JSON array containig a case sensitive list of table names e.g. ["Table1","Table2"]. Received ${tableList}.`)
	} catch (e) {
	  throw new CommandLineError(`Expected a JSON array containig a case sensitive list of table names e.g. ["Table1","Table2"]. Encountered errror "${e.message}" while loading "${tableListPath}.`)
	}
  }
  
  getPartitionMetadata(metadata) {
	return {}
  }

  setOption(name,value) {
    this.options[name] = value;
  }
        
  initializeParameters(parameters){

	// Merge default parameters for this driver with parameters from configuration files and command line parameters.
    this.parameters = Object.assign({}, this.YADAMU_DBI_PARAMETERS, this.vendorParameters, parameters, this.yadamu.COMMAND_LINE_PARAMETERS);
  }

  setParameters(parameters) {
	// Used when creating a worker.
	Object.assign(this.parameters, parameters || {})
	this._COMMIT_COUNT = undefined
	this.FEEDBACK_MODEL = this.parameters.FEEDBACK
  }
  
  traceSQL(msg,rows,lobCount) {
     // this.yadamuLogger.trace([this.DATABASE_VENDOR,'SQL'],msg)
     return(`${msg.trim()}${this.STATEMENT_TERMINATOR} ${rows ? `/* Rows: ${rows}. */ ` : ''} ${lobCount ? `/* LOBS: ${lobCount}. */ ` : ''}${this.sqlTraceTag}${this.STATEMENT_SEPERATOR}`);
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

  processError(yadamuLogger,logEntry,summary,logDDL) {
	 
	let warning = true;
	  
    switch (logEntry.severity) {
      case 'CONTENT_TOO_LARGE' :
        yadamuLogger.error([this.DATABASE_VENDOR,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`Exceeded maximum string size [${this.MAX_STRING_SIZE} bytes].`)
        return;
      case 'SQL_TOO_LARGE':
        yadamuLogger.error([this.DATABASE_VENDOR,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''} `],`Exceeded maximum DDL statement length [${this.MAX_STRING_SIZE} bytes].`)
        return;
      case 'FATAL':
        summary.errors++
		const err =  new Error(logEntry.msg)
		err.SQL = logEntry.sqlStatement
		err.details = logEntry.details
		summary.exceptions.push(err)
        // yadamuLogger.error([this.DATABASE_VENDOR,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''}`],`Details: ${logEntry.msg}\n${logEntry.details}\n${logEntry.sqlStatement}`)
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
        yadamuLogger.warning([this.DATABASE_VENDOR,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''}`],`Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
	  }
	  else {
        yadamuLogger.ddl([this.DATABASE_VENDOR,`${logEntry.severity}`,`${logEntry.tableName ? logEntry.tableName : ''}`],`Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}`)
	  }
	}
  }
          
  processLog(log, operation, status, yadamuLogger) {

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
          yadamuLogger.info([this.DATABASE_VENDOR],`${logEntry}.`)
          break;
        case (logEntryType === "dml") : 
          yadamuLogger.info([`${logEntry.tableName}`,`SQL`],`Rows ${logEntry.rowCount}. Elaspsed Time ${YadamuLibrary.stringifyDuration(Math.round(logEntry.elapsedTime))}s. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.`)
          break;
        case (logEntryType === "info") :
          yadamuLogger.info([this.DATABASE_VENDOR],`"${JSON.stringify(logEntry)}".`);
          break;
        case (logDML && (logEntryType === "dml")) :
          yadamuLogger.dml([this.DATABASE_VENDOR,`${logEntry.tableName}`,`${logEntry.tableName}`],`\n${logEntry.sqlStatement}.`)
          break;
        case (logDDL && (logEntryType === "ddl")) :
          yadamuLogger.ddl([this.DATABASE_VENDOR,`${logEntry.tableName}`],`\n${logEntry.sqlStatement}.`) 
          break;
        case (logTrace && (logEntryType === "trace")) :
          yadamuLogger.trace([this.DATABASE_VENDOR,`${logEntry.tableName ? logEntry.tableName : ''}`],`\n${logEntry.sqlStatement}.`)
          break;
        case (logEntryType === "error"):
		  this.processError(yadamuLogger,logEntry,summary,logDDLMsgs);
      } 
      if (logEntry.sqlStatement) { 
        status.sqlTrace.write(this.traceSQL(logEntry.sqlStatement))
      }
    }) 
	
    if (summary.exceptions.length > 0) {
	  this.yadamuLogger.error([this.DATABASE_VENDOR, status.operation, operation],`Server side operation resulted in ${summary.exceptions.length} errors.`)
  	  const err = new Error(`${this.DATABASE_VENDOR} ${operation} failed.`);
	  err.causes = summary.exceptions
      throw err
    }
	return summary;
  }    
  
  async createConnectionPool() {
    throw new UnimplementedMethod('createConnectionPool()',`YadamuDBI`,this.constructor.name)
  }
  
  async getConnectionFromPool() {
    throw new UnimplementedMethod('getConnectionFromPool()',`YadamuDBI`,this.constructor.name)
  }
  
  async closeConnection() {
    throw new UnimplementedMethod('closeConnection()',`YadamuDBI`,this.constructor.name)
  }
  
  async closePool() {
    throw new UnimplementedMethod('closePool()',`YadamuDBI`,this.constructor.name)
  }

  logDisconnect() {
    const pwRedacted = Object.assign({},this.vendorProperties)
    delete pwRedacted.password
    this.status.sqlTrace.write(this.traceComment(`DISCONNECT : Properies: ${JSON.stringify(pwRedacted)}`))
  }
  
  logConnectionProperties() {    
    const pwRedacted = Object.assign({},this.vendorProperties)
    delete pwRedacted.password
    this.status.sqlTrace.write(this.traceComment(`CONNECT : Properies: ${JSON.stringify(pwRedacted)}`))
  }
  
  updateVendorProperties(vendorProperties) {
  }

  getVendorProperties() {

    const vendorProperties = this.vendorProperties || {}
    this.updateVendorProperties(vendorProperties)
	return vendorProperties

  }
  
  setVendorProperties(connectionSettings) {
	if (!YadamuLibrary.isEmpty(connectionSettings[this.DATABASE_KEY])) {
      this.vendorProperties = connectionSettings[this.DATABASE_KEY] 
      delete connectionSettings[this.DATABASE_KEY] 
    }
	else {
	  this.vendorProperties = {}
    }	 
  }
     
  setConnectionProperties(connectionSettings) {
	this.setVendorProperties(connectionSettings)
    this.vendorParameters = connectionSettings.parameters || {}
	this.vendorSettings = connectionSettings.settings || {}
  }
   
  isValidDDL() {
    return ((this.systemInformation.vendor === this.DATABASE_VENDOR) && (this.systemInformation.dbVersion <= this.DB_VERSION))
  }
  
  isDatabase() {
    return true;
  }
  
  trackExceptions(err) {
    // Reset by passing undefined 
    this.firstError = this.firstError === undefined ? err : this.firstError
	this.latestError = err
    return err
  }	
  
  resetExceptionTracking() {
    this.firstError = undefined
	this.latestError = undefined
  }
   
  setSystemInformation(systemInformation) {
    this.systemInformation = systemInformation
  }
  
  /*
  **
  ** ### TABLE MAPPINGS ###
  **
  ** Some databases have restrictions on the length of identifiers or on he characters that can appear in identifiers.
  ** Identifier Mappings provide a mechanism to map non-compliant names to compliant names. Mappings apply to both table names and column names.
  ** 
  ** Identifier Mappings can also be used to force all identifers names to uppercase or lowercase via the IDENTIFIER_TRANSFORMATION parameters.
  **
  ** An explicit mapping table can be provided via the parameter IDENTIFIER_MAPPING_FILE
  **
  ** Mappings are applied to the target database during COPY, IMPORT, UPLOAD and LOAD operations. It is illegal to specifiy the IDENTIFIER_MAPPINGS or 
  ** IDENTIFIER_TRANSFORMATION parameters during EXPORT and UNLOAD operations
  ** 
  ** Table Mappings are not applied to DDL statements generated by the RDBMS (eg DLL obtained using Oracle's DBMS_METADATA package)
  **
  ** function loadExplicitMappings() loads the IdentifierMappings object from a file disk. The file is specified using the IDENTIFIER_MAPPING_FILE parameter.
  ** 
  ** ### The application of Table Mappings is bi-directional. When importing data the DBI should apply Table Mappings to table names and columns names
  ** ### before attempting to insert data into a database. When exporting data the DBI should apply IdentifierMappings to a the content of the metadata and 
  ** ### data objects generated by the export process.
  **
  */

  mergeMappings(target,source) {
	  
    const sourceKeys = Object.keys(source);
    sourceKeys.forEach((key) => {
      if (target.hasOwnProperty(key)) {
        if (source[key].hasOwnProperty('tableName')) {
          target[key].tableName = source[key].tableName
        }
        if (source[key].hasOwnProperty('columnMappings')) {
          target[key].columnMappings = target[key].columnMappings || []
          Object.keys(source[key].columnMappings).forEach((sourceName) => {
            target[key].columnMappings[sourceName] = source[key].columnMappings[sourceName]
          })
        }
      }
      else {
        target[key] = source[key]
      }
    })
    return target
  }

  generateDatabaseMappings(metadata) {
    return {}
  }
     
  generateIdentifierMappings(metadata) {
    const identifierMappings = {}
    switch (this.IDENTIFIER_TRANSFORMATION) {
      case 'NONE':
        break;
      case 'UPPERCASE':
        Object.keys(metadata).forEach((table) => {
          identifierMappings[table] = { 
            tableName      : metadata[table].tableName.toUpperCase()
          , columnMappings : {}
          }
          metadata[table].columnNames.forEach((columnName) => { identifierMappings[table].columnMappings[columnName] = { name : columnName.toUpperCase()}})
        })
        break;
      case 'LOWERCASE':
        Object.keys(metadata).forEach((table) => {
          identifierMappings[table] = { 
            tableName      : metadata[table].tableName.toLowerCase()
          , columnMappings : {}
          }
          metadata[table].columnNames.forEach((columnName) => { identifierMappings[table].columnMappings[columnName] = { name : columnName.toLowerCase()}})
        })
        break;
      case 'LOWERCASE_TABLE_NAMES':
        Object.keys(metadata).forEach((table) => {
          identifierMappings[table] = { 
            tableName      : metadata[table].tableName.toLowerCase()
          , columnMappings : {}
          }
        })
        break;
      case 'CUSTOM':
        throw new YadamuError([this.DATABSE_VENDOR],`IDENTIFIER_TRANSFORMATION="${this.IDENTIFIER_TRANSFORMATION}": Unsupported Feature in YADAMU ${YadamuConstants.YADAMU_VERSION}.`)       
      default:
        throw new YadamuError([this.DATABSE_VENDOR],`Invalid IDENTIFIER_TRANSFORMATION specified (${this.IDENTIFIER_TRANSFORMATION}). Valid Values are ${YadamuConstants.SUPPORTED_IDENTIFIER_TRANSFORMATION}.`)
    } 
   
    return identifierMappings
  }            
  
  setMetadata(metadata) {

    /*
	**
	** Apply current tableMappings to the metadata
    ** Check the result does not required further transformation	
	** Apply additional transformations as required
	**
	*/
    
    // Explicit mappings specified in an IDENTIFIER_MAPPINGS file are sacroscant.
    
    // Phase 1: Generate a set of identifier mappings from the source metadata according to the parameter IDENTIFIER_TRANSFORMATION;
    // Update the metadata based on the Mappings
      
    const generatedMappings = this.generateIdentifierMappings(metadata)    
    const mappedMetadata = YadamuLibrary.isEmpty(generatedMappings) ? metadata :this.applyIdentifierMappings(metadata,generatedMappings,false)
    
    // Phase 2: Generate a set of identifier mappings from the mapped metadata according to the rules of the database
    // Overwrite the generated mappings with  the contents of the IDENTIFIER_MAPPING_FILE
    // Update the metadata based on the Mappings

    const databaseMappings = this.generateDatabaseMappings(mappedMetadata);
    this.mergeMappings(databaseMappings, this.yadamu.IDENTIFIER_MAPPINGS)
	this.metadata = YadamuLibrary.isEmpty(databaseMappings) ?  mappedMetadata : this.applyIdentifierMappings(mappedMetadata,databaseMappings,true)
    this.setIdentifierMappings( this.mergeMappings(generatedMappings,databaseMappings))
  }

  setIdentifierMappings(identifierMappings) {
    // console.log(this.constructor.name,identifierMappings)
    this.identifierMappings = identifierMappings
  }
 
  getIdentifierMappings() {
	return this.identifierMappings
  }

  applyIdentifierMappings(metadata,mappings,reportMappedIdentifiers) {
	  
	// This function does not change the names of the keys in the metadata object.
	// It only changes the value of the tableName property associated with a mapped tables.
    
    Object.keys(metadata).forEach((table) => {
      const tableMappings = mappings[table]
      if (tableMappings) {
		if ((tableMappings.tableName) && (metadata[table].tableName !== tableMappings.tableName)) {
          if (reportMappedIdentifiers) { 
            this.yadamuLogger.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',metadata[table].tableName],`Table name re-mapped to "${tableMappings.tableName}".`)
          }
		  metadata[table].tableName = tableMappings.tableName
		}
        if (tableMappings.columnMappings) {
          const columnNames = metadata[table].columnNames
          const dataTypes = metadata[table].dataTypes
          Object.keys(tableMappings.columnMappings).forEach((columnName) => {
            const idx = columnNames.indexOf(columnName);
            const mappedColumnName = tableMappings.columnMappings[columnName].name || columnNames[idx]
			const mappedDataType = tableMappings.columnMappings[columnName].dataType || dataTypes[idx]
            if (idx > -1) {
              if (reportMappedIdentifiers) { 
			    if (columnNames[idx] !== mappedColumnName) {
                  this.yadamuLogger.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',table,columnName],`Column name re-mapped to "${mappedColumnName}".`)
				}
			    if (dataTypes[idx] !== mappedDataType) {
                  this.yadamuLogger.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',table,columnName],`Data type "${dataTypes[idx] }" re-mapped to "${mappedDataType}".`)
				}
              }
              columnNames[idx] = mappedColumnName          
              dataTypes[idx] = mappedDataType       
            }
          });
          // metadata[table].columnNames = columnNames
        }
      }   
    });
    return metadata	
  }
  
  getMappedTableName(tableName,identifierMappings) {
	  
	// map tableName according to supplied identifierMappings. 
	      
	if (identifierMappings && identifierMappings.hasOwnProperty(tableName) && identifierMappings[tableName].hasOwnProperty('tableName')) {	
 	  return identifierMappings[tableName].tableName 
	}
    return tableName
  }

  transformMetadata(metadata,identifierMappings) {
    if (identifierMappings) {
      const mappedMetadata = this.applyIdentifierMappings(metadata,identifierMappings)
	  const outboundMetadata = {}
	  Object.keys(mappedMetadata).forEach((tableName) => { outboundMetadata[this.getMappedTableName(tableName,identifierMappings)] = mappedMetadata[tableName] })
	  return outboundMetadata
	}
	else {
      return metadata
	}
  }
	  
  async _executeDDL(ddl) {
	let results
	const startTime = performance.now();
	try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA);
		// this.status.sqlTrace.write(this.traceSQL(ddlStatement));
        return this.executeSQL(ddlStatement,{});
      }))
    } catch (e) {
	 const exceptionFile = this.yadamuLogger.handleException([this.DATABASE_VENDOR,'DDL'],e)
	 await this.yadamuLogger.writeMetadata(exceptionFile,this.yadamu,this.systemInformation,this.metadata)
	 results = e;
    }
    return results;
  }
  
  async skipDDLOperations() {
	 this.emit(YadamuConstants.DDL_UNNECESSARY,performance.now,[])
  }
  
 
  async executeDDL(ddl) {
	if (ddl.length > 0) {
      const startTime = performance.now();
	  this.ddlInProgress = true
	  try {
        const results = await this._executeDDL(ddl);
	    this.emit(YadamuConstants.DDL_COMPLETE,startTime,results)
	    return results
      } catch (e) {
  	    this.emit(YadamuConstants.DDL_COMPLETE,startTime,e)
	  }
	}
	else {
      this.emit(YadamuConstants.DDL_UNNECESSARY)
	  return []
	}
  }

  prepareDDLStatements(ddlStatements) {
	return ddlStatements
  }
  	
  analyzeStatementCache(statementCache,startTime) {
  
	let dmlStatementCount = 0
	let ddlStatements = []
	Object.values(statementCache).forEach((tableInfo) => {
	  if (tableInfo.ddl !== null) {
		ddlStatements.push(tableInfo.ddl)
	  }
	  if (tableInfo.dml !== null) {
		dmlStatementCount++;
      }
    })	 
	this.yadamuLogger.ddl([this.DATABASE_VENDOR],`Generated ${ddlStatements.length === 0 ? 'no' : ddlStatements.length} "Create Table" statements and ${dmlStatementCount === 0 ? 'no' : dmlStatementCount} DML statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
    ddlStatements = this.prepareDDLStatements(ddlStatements)	
	return ddlStatements	
  }  
  
  async _getDatabaseConnection() {
	  
	let connected = false;
    try {
      await this.createConnectionPool();
      this.connection = await this.getConnectionFromPool();
      connected = true
      await this.configureConnection();
    } catch (e) {
      const err = connected ? e : new ConnectionError(e,this.vendorProperties);
      throw err
    }

  }  
  
  async _reconnect() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
  }

  trackLostConnection() {
   
    /*
    **
    ** Invoked when the connection is lost. A lost connection scenario is an implicit rollback. All uncommitted rows are lost. 
    **
    */
 
  	if ((this.COPY_METRICS !== undefined) && (this.COPY_METRICS.lost  !== undefined) && (this.COPY_METRICS.written  !== undefined)) {
      if (this.COPY_METRICS.written > 0) {
        this.yadamuLogger.error([`RECONNECT`,this.DATABASE_VENDOR],`${this.COPY_METRICS.written} uncommitted rows discarded when connection lost.`);
        this.COPY_METRICS.lost += this.COPY_METRICS.written;
	    this.COPY_METRICS.written = 0;
      }
	}
  }	  
  
  async reconnect(cause,operation) {

    // Reconnect. If rows were lost as a result of the reconnect, the original error will be thrown after the reconnection is complete
	
    cause.yadamuReconnected = false;

    this.RECONNECT_IN_PROGRESS = true;
    const TRANSACTION_IN_PROGRESS = this.TRANSACTION_IN_PROGRESS 
	const SAVE_POINT_SET = this.SAVE_POINT_SET

    this.yadamuLogger.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`Connection Lost: Attemping reconnection.`);
	
    if (cause instanceof Error) {
  	  this.yadamuLogger.handleWarning([`RECONNECT`,this.DATABASE_VENDOR,operation],cause)
    }
	
	/*
	**
	** If a connection is lost while performing batched insert operatons using a table writer, adjust the table writers running total of records written but not committed. 
	** When a connection is lost records that have written but not committed will be lost (rolled back by the database) when cleaning up after the lost connection.
	** To avoid the possibility of lost batches set COMMIT_RATIO to 1, so each batch is committed as soon as it is written.
	**
	*/
	
    this.trackLostConnection();
	
    let retryCount = 0;
    let connectionUnavailable
    while (retryCount < 10) {
		
      /*
      **
      ** Attempt to close the connection. Handle but do not throw any errors...
	  ** This is important for MsSQL which has transaction housekeeping state that needs to be updated even through the connection is dead..
      **
      */	
	
	  try {
        await this.closeConnection()
      } catch (e) {
	    if (!YadamuError.lostConnection(e)) {
          this.yadamuLogger.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`Error closing existing connection.`);
		  this.yadamuLogger.handleException([`RECONNECT`,this.DATABASE_VENDOR,operation],e)
	    }
	  }	 
		 
	  try {
        await this._reconnect()
	    await this.configureConnection();
		if (TRANSACTION_IN_PROGRESS) {
		  await this.beginTransaction()
		  if (SAVE_POINT_SET) {
		    await this.createSavePoint()
		  }
		}

        this.yadamuLogger.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`New connection available.`);
	    this.failedPrematureClose = false;
        cause.yadamuReconnected = true;
      } catch (connectionFailure) {
        // Reconnection failed. If cause is "server unavailable" wait 0.5 seconds and retry (up to 10 times)
		if (YadamuError.serverUnavailable(connectionFailure)) {
		  connectionUnavailable = connectionFailure;
          this.yadamuLogger.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`Waiting for restart.`)
          await setTimeout(5000);
          retryCount++;
          continue;
        }
        else {
          // Reconnection attempt failed for some other reason. Throw the error
   	      this.RECONNECT_IN_PROGRESS = false;
          this.yadamuLogger.handleException([`RECONNECT`,this.DATABASE_VENDOR,operation],connectionFailure);
          throw cause;
        }
      }
      // Sucessfully reonnected. Throw the original error if rows were lost as a result of the lost connection
      this.RECONNECT_IN_PROGRESS = false;
      if ((this.COPY_METRICS !== undefined) && (this.COPY_METRICS.lost > 0)) { 
        throw cause
      }
      return
    }
    // Unable to reconnect after 10 attempts
    this.RECONNECT_IN_PROGRESS = false;
    throw connectionUnavailable 	
  }
  
  async getDatabaseConnection(requirePassword) {
    let interactiveCredentials = (requirePassword && ((this.vendorProperties[this.PASSWORD_KEY_NAME] === undefined) || (this.vendorProperties[this.PASSWORD_KEY_NAME].length === 0))) 
    let retryCount = interactiveCredentials ? this.RETRY_COUNT : 1
    
	
    let prompt = `Enter password for ${this.DATABASE_VENDOR} connection: `
	if (process.env.YADAMU_PASSWORD) {
      retryCount++
	}
    while (retryCount > 0) {
      retryCount--
      if (interactiveCredentials)  {
		if (retryCount === this.RETRY_COUNT) {
		  console.log('Loaded database password from environment variable "YADAMU_PASSWORD".')
	      this.vendorProperties[this.PASSWORD_KEY_NAME] = process.env.YADAMU_PASSWORD
		}
	    else {
          const pwQuery = this.yadamu.createQuestion(prompt);
          const password = await pwQuery;
          this.vendorProperties[this.PASSWORD_KEY_NAME] = password;
		}
      }
      try {
        await this._getDatabaseConnection()  
		this.emit(YadamuConstants.DB_CONNECTED)
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
	      case this.RETRY_COUNT:
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

  async setLibraries() {
  }
  
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
    
    if (this.isDatabase()) {
      await this.getDatabaseConnection(requirePassword);
   	  await this.setLibraries()
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
  

  
  newBatch() {
	return []
  }

  releaseBatch(batch) {
	if (Array.isArray(batch)) {
	  batch.length = 0;
	}
  }

  getCloseOptions(err) {
	 	
	return err ? {abort  : false } : { abort : true, error : err }

  }
  
  async final(){
	
    // this.yadamuLogger.trace([this.constructor.name,'final()',this.ROLE],`Waiting for ${this.activeWorkers.size} Writers to terminate. [${this.activeWorkers}]`);
	  
	if (this.ddlInProgress) {
	  await this.ddlComplete
	}

	const closeOptions = this.getCloseOptions()
				
	await this.writersFinished()

	if (this.isManager()) {
      // this.yadamuLogger.trace([this.constructor.name,'final()',this.ROLE,'ACTIVE_WORKERS'],`WAITING [${this.activeWorkers.size}]`)
	  const stillWorking = Array.from(this.activeWorkers).map((worker) => { return worker.workerState })
	  await Promise.all(stillWorking);
      // this.yadamuLogger.trace([this.constructor.name,'final()',this.ROLE,'ACTIVE_WORKERS'],'PROCESSING')
	}	  

    await this.closeConnection(closeOptions);
    this.logDisconnect();
	await this.closePool(closeOptions);
		
  }	

  async destroy(err) {
 
	// this.yadamuLogger.trace([this.constructor.name,this.ROLE,this.getWorkerNumber()],`doDestroy(${this.activeWorkers.size},${(this.err ? this.err.message : 'normal'})})`)
	
    /*
    **
    **  Abort the database connection and pool
    **
    */

    if ((this.isManager()) &&  (this.activeWorkers.size > 0))  {
      // Active Workers contains the set of Workers that have not terminated.
	  // We need to force them to terminate and release any database connections they own.
	  this.yadamuLogger.error([this.DATABASE_VENDOR,this.ROLE,'destroy()'],`Aborting ${this.activeWorkers.size} active workers).`)
	  this.activeWorkers.forEach((worker) => {
	    try {
          this.yadamuLogger.log([this.DATABASE_VENDOR,this.ROLE,'destroy()','Worker',this.getWorkerNumber()],`Found Active Worker ${worker.getWorkerNumber()}`);
	      worker.destroy()
		} catch(e) {
          this.yadamuLogger.handleException([this.DATABASE_VENDOR,this.ROLE,'destroy()','Worker',worker.getWorkerNumber()],e);
		}
	  })
	}

	const closeOptions = this.getCloseOptions(err)
	
	if (this.connection) {		
      try {
        await this.closeConnection(closeOptions);
    	this.logDisconnect();
	  } catch (e) {
	    if (!YadamuError.lostConnection(e)) {
          this.yadamuLogger.handleException([this.DATABASE_VENDOR,this.ROLE,'destroy()','closeConnection()'],e);
	    }
	  }
	}
	
	if (this.pool) {
      try {
	    // Force Termnination of All Current Connections.
	    await this.closePool(closeOptions);
	  } catch (e) {
	    if (!YadamuError.lostConnection(e)) {
          this.yadamuLogger.handleException([this.DATABASE_VENDOR,this.ROLE,'destroy()','closePool()'],e);
	    }
	  }
	}
	
	this.yadamu.activeConnections.delete(this)
	
  }	
    
  checkConnectionState(cause) {
	 
	// Throw cause if cause is a lost connection. Used by drivers to prevent attempting rollback or restore save point operations when the connection is lost.
	
    if ((cause instanceof BatchInsertError)  || (cause instanceof IterativeInsertError)) {
	  cause = cause.cause
    }
	  
  	if (YadamuError.lostConnection(cause) && !cause.yadamuReconnected) {
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
    this.TRANSACTION_IN_PROGRESS = true;  
	this.SAVE_POINT_SET = false;
  }

  /*
  **
  ** Commit the current transaction
  **
  */
    
  commitTransaction() {
	this.TRANSACTION_IN_PROGRESS = false;  
	this.SAVE_POINT_SET = false;
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  rollbackTransaction(cause) {
	this.TRANSACTION_IN_PROGRESS = false;  
	this.SAVE_POINT_SET = false;
  }
  
  /*
  **
  ** Set a Save Point
  **
  */
    
  createSavePoint() {
	this.SAVE_POINT_SET = true;
  }

  /*
  **
  ** Revert to a Save Point
  **
  */

  restoreSavePoint(cause) {
	this.SAVE_POINT_SET = false;
  }

  releaseSavePoint(cause) {
	this.SAVE_POINT_SET = false;
  }

  /*
  **
  ** The following methods are used by JSON_TABLE() style import operations  
  **
  */
  
  async upload() {

	let stack 
	try {
      stack = new Error().stack		
      const stats = fs.statSync(this.UPLOAD_FILE)
      const fileSizeInBytes = stats.size    

      const startTime = performance.now();
      const jsonHndl = await this.uploadFile(this.UPLOAD_FILE);
      const elapsedTime = performance.now() - startTime;
      this.yadamuLogger.info([this.DATABASE_VENDOR,`UPLOAD`],`File "${this.UPLOAD_FILE}". Size ${fileSizeInBytes}. Elapsed time ${YadamuLibrary.stringifyDuration(elapsedTime)}s.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.`)
	  await this.initializeImport()
      const log = await this.processFile(jsonHndl)
	  await this.finalizeImport()
	  return log
    } catch (err) {
      throw err.code === 'ENOENT' ? new FileNotFound(this.DRIVER_ID,err,stack,this.UPLOAD_FILE) : new FileError(this.DRIVER_ID,err,stack,this.UPLOAD_FILE)
	}
  }


  /*
  **
  **  Upload a JSON File to the server. Optionally return a handle that can be used to process the file
  **
  */
  
  async uploadFile(importFilePath) {
    throw new UnimplementedMethod('uploadFile()',`YadamuDBI`,this.constructor.name)
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  async processFile(hndl) {
    throw new UnimplementedMethod('processFile()',`YadamuDBI`,this.constructor.name)
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
  
  getTypeMappings() {
	return {
      spatialFormat: this.SPATIAL_FORMAT
	}
  }
  
  getSystemInformation() {     
  
    return {
      yadamuVersion      : YadamuConstants.YADAMU_VERSION
    , date               : new Date().toISOString()
    , timeZoneOffset     : new Date().getTimezoneOffset()
    , typeMappings       : this.getTypeMappings()
	, tableFilter        : this.getTABLE_FILTER
    , schema             : this.CURRENT_SCHEMA ? this.CURRENT_SCHEMA : this.CURRENT_SCHEMA
    , vendor             : this.DATABASE_VENDOR
	, dbVersion          : this.DB_VERSION
	, softwareVendor     : this.SOFTWARE_VENDOR
    , nodeClient         : {
        version          : process.version
       ,architecture     : process.arch
       ,platform         : process.platform
      }
    }
  }
  
  async getYadamuInstanceInfo() {
	const systemInfo = await this.getSystemInformation();
	return {
	  yadamuInstanceID: systemInfo.yadamuInstanceID
	, yadamuInstallationTimestamp: systemInfo.yadamuInstallationTimestamp
    }
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
	this.schemaMetadata = schemaInfo.map((table) => {
	  return {
	    TABLE_SCHEMA          : table[0]
	  , TABLE_NAME            : table[1]
	  , COLUMN_NAME_ARRAY     : table[2]
	  , DATA_TYPE_ARRAY       : table[3]
	  , SIZE_CONSTRAINT_ARRAY : table[4]
	  , CLIENT_SELECT_LIST    : table[5]
	  }
    })

	return this.schemaMetadata;
  }
  
  applyTableFilter(schemaInformation) {
	    
    // Restrict operations to the list of tables specified.
	// Order operations according to the order in which the tables were specified
		
    if (this.TABLE_FILTER.length > 0) {
	  
	  // Check table names are valid.
	  // For each name in the Table Filter check there is a corresponding entry in the schemaInformation collection
	
	  const tableNames = schemaInformation.map((tableInformation) => {
		return tableInformation.TABLE_NAME
	  })
	  
	  const invalidTableNames = this.TABLE_FILTER.filter((tableName) => {
		 // Return true if the table does not have an entry in the schemaInformstion collection
		 return !tableNames.includes(tableName)
	  })
	  
	  if (invalidTableNames.length > 0) {
        throw new CommandLineError(`Could not resolve the following table names : "${invalidTableNames}".`)
      }
	
      this.yadamuLogger.info([this.DATABASE_VENDOR],`Operations restricted to the following tables: ${JSON.stringify(this.TABLE_FILTER)}.`)
	  	 
	  schemaInformation = this.TABLE_FILTER.flatMap((tableName) => {
        return schemaInformation.filter((tableInformation) => {
		   return (tableInformation.TABLE_NAME === tableName)
		 })
	  })
    }
	return schemaInformation
  }
  
  expandTaskList(schemaInformation) {
    return schemaInformation
  }
  
  generateMetadata(schemaInformation) {   
   
    const metadata = {}
	
    schemaInformation.forEach((table,idx) => {
      table.COLUMN_NAME_ARRAY     = typeof table.COLUMN_NAME_ARRAY     === 'string' ? JSON.parse(table.COLUMN_NAME_ARRAY)     : table.COLUMN_NAME_ARRAY
      table.DATA_TYPE_ARRAY       = typeof table.DATA_TYPE_ARRAY       === 'string' ? JSON.parse(table.DATA_TYPE_ARRAY)       : table.DATA_TYPE_ARRAY
      table.STORAGE_TYPE_ARRAY    = typeof table.STORAGE_TYPE_ARRAY    === 'string' ? JSON.parse(table.STORAGE_TYPE_ARRAY)    : table.STORAGE_TYPE_ARRAY || table.DATA_TYPE_ARRAY
      table.SIZE_CONSTRAINT_ARRAY = typeof table.SIZE_CONSTRAINT_ARRAY === 'string' ? JSON.parse(table.SIZE_CONSTRAINT_ARRAY) : table.SIZE_CONSTRAINT_ARRAY
      const tableMetadata =  {
        tableSchema              : table.TABLE_SCHEMA
       ,tableName                : table.TABLE_NAME
       ,columnNames              : table.COLUMN_NAME_ARRAY
       ,dataTypes                : table.DATA_TYPE_ARRAY
       ,storageTypes             : table.STORAGE_TYPE_ARRAY
       ,sizeConstraints          : table.SIZE_CONSTRAINT_ARRAY
      }
	  if (table.PARTITION_COUNT) {
		tableMetadata.partitionCount = table.PARTITION_COUNT
      }
      metadata[table.TABLE_NAME] = tableMetadata
    }) 
	return metadata
  }  
  
  generateSelectListEntry(columnInfo) {
	return `"${columnInfo[2]}"`
  }
  
  buildSchemaInfo(schemaColumnInfo) {
	const schemaInfo = []
	let tableInfo = undefined
    let tableName = undefined
    schemaColumnInfo.forEach((columnInfo) => {
	  if (tableName !== columnInfo[1] ) {
	    if (tableName) {
	      tableInfo.CLIENT_SELECT_LIST = tableInfo.CLIENT_SELECT_LIST.substring(1)
 		  schemaInfo.push(tableInfo)
	    }
		tableName = columnInfo[1]
        tableInfo = {
		  TABLE_SCHEMA          : columnInfo[0]
	    , TABLE_NAME            : columnInfo[1]
	    , COLUMN_NAME_ARRAY     : []
	    , DATA_TYPE_ARRAY       : []
	    , SIZE_CONSTRAINT_ARRAY : []
		, CLIENT_SELECT_LIST    : ""
	    }
	  }
	  const dataType = YadamuLibrary.decomposeDataType(columnInfo[3])
      tableInfo.COLUMN_NAME_ARRAY.push(columnInfo[2])
	  tableInfo.DATA_TYPE_ARRAY.push(dataType.typeQualifier ? `${dataType.type} ${dataType.typeQualifier}` : dataType.type)
	  tableInfo.SIZE_CONSTRAINT_ARRAY.push(dataType.length ? dataType.scale ? `${dataType.length},${dataType.scale}` : `${dataType.length}` : '')
	  tableInfo.CLIENT_SELECT_LIST = `${tableInfo.CLIENT_SELECT_LIST},${this.generateSelectListEntry(columnInfo)}`
    })
	if (tableInfo) {
      tableInfo.CLIENT_SELECT_LIST = tableInfo.CLIENT_SELECT_LIST.substring(1)
	  schemaInfo.push(tableInfo)
	}
	return schemaInfo
  }
  
  async getSchemaMetadata() {
    
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
    ** The query may also return the SQL that should be used to retieve the data from the schema
    **
    ** Implimentations should provde a custom impliemtnation of generateMetadata() if they need more than the minimum set of information about the schema.
    **
    */
          
    throw new UnimplementedMethod('getSchemaMetadata()',`YadamuDBI`,this.constructor.name)
    return []
  }
     
  /*
  **
  **
  The following methods are used by the YADAMU DBwriter class
  **
  */
  
  getSchemaIdentifer() {
	return this.CURRENT_SCHEMA
  }

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
	const statementGenerator = new StatementGenerator(this,schema,this.metadata,this.yadamuLogger)
    this.statementCache = await statementGenerator.generateStatementCache(this.systemInformation.vendor)
	this.emit(YadamuConstants.CACHE_LOADED  )
	return this.statementCache

  }

  async finalizeRead(tableInfo) {
  }
  
  getTableInfo(tableName) {
	  
    if (this.statementCache === undefined) {
      this.yadamuLogger.logInternalError([this.constructor.name,`getTableInfo()`,tableName],`Statement Cache undefined. Cannot obtain required information.`)
	}
	
	// Statement Cache is keyed by actual table name so we need the mapped name if there is a mapping.

	let mappedTableName = this.getMappedTableName(tableName,this.identifierMappings)
  	const tableInfo = this.statementCache[mappedTableName]
	
	// Add Some Common Settings
	
	tableInfo.insertMode = tableInfo.insertMode || 'Batch';    
	
	tableInfo.columnCount = tableInfo.columnCount || tableInfo.columnNames.length;      
    tableInfo.skipTable = tableInfo.skipTable || this.MODE === 'DDL_ONLY';    

	if (tableInfo === undefined) {
      this.yadamuLogger.logInternalError([this.constructor.name,`getTableInfo()`,tableName,mappedTableName],`No Statement Cache entry for "${mappedTableName}". Current entries: ${JSON.stringify(Object.keys(this.statementCache))}`)
	}
    
	tableInfo.tableName = mappedTableName
	return tableInfo
  }

  generateSQLQuery(tableMetadata) {
    const queryInfo = Object.assign({},tableMetadata);   
	queryInfo.SQL_STATEMENT = `select ${tableMetadata.CLIENT_SELECT_LIST} from "${tableMetadata.TABLE_SCHEMA}"."${tableMetadata.TABLE_NAME}" t`; 
	
    // ### TESTING ONLY: Uncomment folllowing line to force Table Not Found condition
    // queryInfo.SQL_STATEMENT = queryInfo.SQL_STATEMENT.replace(queryInfo.TABLE_NAME,queryInfo.TABLE_NAME + "1")

	queryInfo.MAPPED_TABLE_NAME = this.getMappedTableName(queryInfo.TABLE_NAME,this.identifierMappings) || queryInfo.TABLE_NAME
    return queryInfo
  }   

  createParser(queryInfo,parseDelay) {
    return new DefaultParser(queryInfo,this.yadamuLogger,parseDelay);      
  }
 
  inputStreamError(cause,sqlStatement) {
    return this.trackExceptions(new InputStreamError(cause,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(queryInfo) {
	this.streamingStackTrace = new Error().stack;
    throw new UnimplementedMethod('getInputStream()',`YadamuDBI`,this.constructor.name)
	return inputStream;
  }      

  async getInputStreams(queryInfo,parseDelay) {
	const streams = []
	const metrics = DBIConstants.NEW_COPY_METRICS
	metrics.SOURCE_DATABASE_VENDOR = this.DATABASE_VENDOR
	const inputStream = await this.getInputStream(queryInfo)
	inputStream.COPY_METRICS = metrics
    inputStream.once('readable',() => {
	  inputStream.COPY_METRICS.readerStartTime = performance.now()
	}).on('error',(err) => { 
      metrics.readerEndTime = performance.now()
	  metrics.failed = true;
	  this.failedPrematureClose = YadamuError.prematureClose(err)
	  this.underlyingCause =  this.failedPrematureClose ? null : this.inputStreamError(err,queryInfo.SQL_STATEMENT)
	  metrics.readerError = this.underlyingCause
    }).on('end',() => {
	  inputStream.COPY_METRICS.readerEndTime = performance.now()
    })
	streams.push(inputStream)
	
	const parser = this.createParser(queryInfo,parseDelay)
	parser.COPY_METRICS = metrics
	parser.once('readable',() => {
	  metrics.parserStartTime = performance.now()
	}).on('finish',() => {
	  metrics.parserEndTime = performance.now()
	}).on('error',(err) => {
	  metrics.parserEndTime = performance.now()
	  metrics.failed = true;
	  metrics.parserError = YadamuError.prematureClose(err) ? null : err
	})
	
	streams.push(parser)
    return streams;
  }
  
  getOutputManager(OutputManager,tableName,metrics) {
    return new OutputManager(this,tableName,metrics,this.status,this.yadamuLogger)
  }

  getOutputStream(TableWriter,tableName,metrics) {
    // this.yadamuLogger.trace([this.constructor.name,`getOutputStream(${tableName})`],'')
    return new TableWriter(this,tableName,metrics,this.status,this.yadamuLogger)
  }
  
  
  getOutputStreams(tableName,metrics) {
	// A Writer needs to track the metrics do it can make decisions about whether or not to honor a reconnect() request following a lost connection. 
	this.COPY_METRICS = metrics
    const streams = []
	metrics.TARGET_DATABASE_VENDOR = this.DATABASE_VENDOR
	const outputManager = this.getOutputManager(tableName,metrics)
	outputManager.once('readable',() => {
	  metrics.managerStartTime = performance.now()
	}).on('finish',() => { 
 	  metrics.managerEndTime = performance.now()
	}).on('error',(err) => {
 	  metrics.managerEndTime = performance.now()
	  metrics.failed = true;
	  metrics.managerError = YadamuError.prematureClose(err) ? null : err
    })
	streams.push(outputManager)
	
	const tableWriter = this.getOutputStream(tableName,metrics)
	tableWriter.once('pipe',() => {
	  metrics.writerStartTime = performance.now()
	}).on('error',(err) => {
	  metrics.writerEndTime = performance.now()
	  metrics.failed = true;
	  metrics.writerError = YadamuError.prematureClose(err) ? null : err
	})
	
	streams.push(tableWriter)
    return streams;
  }
  
  keepAlive(rowCount) {
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

  cloneSettings() {
    this.dbConnected = this.manager.dbConnected
	this.cacheLoaded = this.manager.cacheLoaded
    this.ddlComplete = this.manager.ddlComplete
		  
	this.StatementLibrary   = this.manager.StatementLibrary
	this.StatementGenerator = this.manager.StatementGenerator

    this.setParameters(this.manager.parameters);
	
	this.systemInformation  = this.manager.systemInformation
	this.metadata           = this.manager.metadata
    this.statementCache     = this.manager.statementCache
    this.statementGenerator = this.manager.statementGenerator
	this.ddlComplete        = this.manager.ddlComplete
	this.partitionMetadata  = this.manager.partitionMetadata
	
	this.setIdentifierMappings(this.manager.getIdentifierMappings())
  }   

  workerDBI(workerNumber) {
      
    // Invoked on the DBI that is being cloned. Parameter dbi is the cloned interface.
	
	const dbi = this.classFactory(this.yadamu)  
    dbi.workerNumber = workerNumber
    dbi.sqlTraceTag = ` /* Worker [${dbi.getWorkerNumber()}] */`;
	return dbi
  }
 
  async getConnectionID() {
	// ### Get a Unique ID for the connection
    throw new UnimplementedMethod('getConnectionID()',`YadamuDBI`,this.constructor.name)
  }
  
  async writersFinished() {
	  
    // this.yadamuLogger.trace([this.constructor.name,'writersFinished()',this.ROLE,this.getWorkerNumber()],`Waiting for ${this.activeWriters.size} Writers to terminate. [${this.activeWriters}]`)

    // this.yadamuLogger.trace([this.constructor.name,'writersFinished()','ACTIVE_WRITERS',this.getWorkerNumber()],'WAITING')
    await Promise.allSettled(this.activeWriters)
    // this.yadamuLogger.trace([this.constructor.name,'writersFinished()','ACTIVE_WRITERS',this.getWorkerNumber()],'PROCESSING')

  }
  
  async destroyWorker() {
	// this.yadamuLogger.trace([this.constructor.name,'destroyWorker()',this.ROLE,this.getWorkerNumber()],`Termianting Worker`);
	await this.writersFinished()
	await this.releaseWorkerConnection() 
	this.emit(YadamuConstants.DESTROYED)
  }
  
  
  /*
  **
  ** Copy Operation Support.
  **
  */
  
  getCredentials(key) {
    return ''
  }
	
	
  validStagedDataSet(vendor,controlFilePath,controlFile) {

    /*
	**
	** Return true if, based on te contents of the control file, the data set can be consumed directly by the RDBMS using a COPY operation.
	** Return false if the data set cannot be consumed using a Copy operation
	** Do not throw errors if the data set cannot be used for a COPY operatio
	** Generate Info messages to explain why COPY cannot be used.
	**
	*/

    if (!this.SUPPORTED_STAGING_PLATFORMS.includes(vendor)) {
       return false;
	}

	return this.reportCopyOperationMode(controlFile.settings.contentType === 'CSV',controlFilePath,controlFile.settings.contentType)
  }

  reportCopyOperationMode(copyEnabled,controlFilePath,contentType) {
    this.yadamuLogger.info([this.DATABASE_VENDOR,'COPY',`${contentType}`],`Processing ${controlFilePath}" using ${copyEnabled ? 'COPY' : 'PIPELINE' } mode.`)
	return copyEnabled
  } 
  
  verifyStagingSource(source) {   
    if (!this.SUPPORTED_STAGING_PLATFORMS.includes(source)) {
      throw new YadamuError(`COPY operations not supported between "${source}" and "${this.DATABASE_VENDOR}".`)
	}
  }

  async reportCopyErrors(tableName,metrics) {
  }
 
  async initializeCopy() {
	await this.initializeImport()
  }
  
  async copyOperation(tableName,copyOperation,metrics) {
	
    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
	
	try {
	  metrics.writerStartTime = performance.now();
	  let results = await this.beginTransaction();
	  results = await this.executeSQL(copyOperation.dml);
  	  metrics.read = results.affectedRows
	  metrics.written = results.affectedRows
	  metrics.writerEndTime = performance.now();
	  results = await this.commitTransaction()
	  metrics.committed = metrics.written 
	  metrics.written = 0
  	} catch(e) {
	  metrics.writerError = e
	  try {
  	    this.yadamuLogger.handleException([this.DATABASE_VENDOR,'COPY',tableName],e)
	    let results = await this.rollbackTransaction()
	  } catch (e) {
		e.cause = metrics.writerError
		metrics.writerError = e
	  }
	}
	return metrics
  }
  		  
  async finalizeCopy() {
	await this.finalizeImport();
  }
  
}

export { YadamuDBI as default}