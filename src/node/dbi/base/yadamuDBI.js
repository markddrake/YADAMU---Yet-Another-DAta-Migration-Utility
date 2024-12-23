
import fs                 from 'fs';
import path               from 'path';
import EventEmitter       from 'events'

import { 
  performance 
}                         from 'perf_hooks';

import {
  setTimeout 
}                         from 'timers/promises'

import Yadamu             from '../../core/yadamu.js'
import YadamuConstants    from '../../lib/yadamuConstants.js'
import YadamuLibrary      from '../../lib/yadamuLibrary.js'
import NullWriter         from '../../util/nullWriter.js'
import NullWritable       from '../../util/nullWritable.js'

import Comparitor         from './yadamuCompare.js'

import {
  YadamuError, 
  InternalError, 
  CommandLineError, 
  ConfigurationFileError, 
  ConnectionError, 
  DatabaseError, 
  BatchInsertError, 
  CopyOperationAborted,
  IterativeInsertError,
  UnimplementedMethod
}                         from '../../core/yadamuException.js'

import DBIConstants       from './dbiConstants.js'
import YadamuDataTypes    from './yadamuDataTypes.js'

import {
  FileNotFound, 
  FileError
}                         from '../file/fileException.js'

class SQLTrace {
    
  get SQL_CUMLATIVE_TIME()      { return this._SQL_CUMLATIVE_TIME }

  constructor(writer,seperator,terminator,role) {
	  
	this.writer = writer
	
    this.STATEMENT_SEPERATOR = seperator
    this.STATEMENT_TERMINATOR = terminator
    this.ROLE = role
    this.MARKER =  `/* [${role}] Manager */` 
    this._SQL_CUMLATIVE_TIME = 0
    
    this.disabledWriter = undefined;
    this.enabled = true;
  }
  
  setWorkderId(id) {
    this.MARKER =  ` /* [${this.ROLE}] Worker(${id})] */`;
  }
  
  trace(msg) {
	this.writer.write(msg)
  }
  
  disable() {
    this.disabledWriter = this.disabledWriter || this.writer
    this.writer = NullWriter.NULL_WRITER
    this.enabled = false
  }
  
  enable() {
    this.writer = this.disabledWriter || this.writer
    this.disabledWriter = undefined
    this.enabled = true
  }
    
  traceSQL(msg,rows,lobCount) {
     this.trace(`${msg.trim()}${this.STATEMENT_TERMINATOR} ${rows ? `/* Rows: ${rows}. */ ` : ''} ${lobCount ? `/* LOBS: ${lobCount}. */ ` : ''}${this.MARKER}${this.STATEMENT_SEPERATOR}`)
  }
  
  recordTime(time) {
    this._SQL_CUMLATIVE_TIME+= time
  }
  
  traceTiming(startTime,endTime) {      
    const sqlOperationTime = endTime - startTime;
    this.trace(`--\n-- ${this.MARKER} Elapsed Time: ${YadamuLibrary.stringifyDuration(sqlOperationTime)}s.}${this.STATEMENT_SEPERATOR}`)
    this.recordTime(sqlOperationTime)
    return sqlOperationTime
  }
 
  comment(comment) {
    this.trace(`/* ${comment} */\n`)
  }

}
  
/*
**
** YADAMU Database Interface class 
**
** Notes:
**
**   The SQL used to fetch metadata about the database schema should generate virtualized Data Types, eg JSON, XML, BOOLEAN rather than actual types.
**   This is done by inspecting constraints etc where the actual type can contain different types of content.
*/

class YadamuDBI extends EventEmitter {

  // Instance level getters.. invoke as this.METHOD

  get DATABASE_KEY()                 { return 'yadamu' };
  get DATABASE_VENDOR()              { return 'YADAMU' };
  get SOFTWARE_VENDOR()              { return 'YABASC - Yet Another Bay Area Software Company'};
  get DATATYPE_IDENTITY_MAPPING()    { return true }
  
  get DESTROYED()                    { return this._DESTROYED }
  set DESTROYED(v)                   { this._DESTROYED = v }

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
  get RETRY_COUNT()                  { return 3 }
  
  get TABLE_MAX_ERRORS()             { return this.parameters.TABLE_MAX_ERRORS            || DBIConstants.TABLE_MAX_ERRORS };
  get TOTAL_MAX_ERRORS()             { return this.parameters.TOTAL_MAX_ERRORS            || DBIConstants.TOTAL_MAX_ERRORS };
  get MODE()                         { return this.parameters.MODE                        || DBIConstants.MODE }
  get ON_ERROR()                     { return this.parameters.ON_ERROR                    || DBIConstants.ON_ERROR }
  get INFINITY_MANAGEMENT()          { return this.parameters.INFINITY_MANAGEMENT         || DBIConstants.INFINITY_MANAGEMENT };
  get STAGING_FILE_RETENTION()       { return this.parameters.STAGING_FILE_RETENTION      || DBIConstants.STAGING_FILE_RETENTION }
  get BYTE_TO_CHAR_RATIO()           { return this.parameters.BYTE_TO_CHAR_RATIO          || DBIConstants.BYTE_TO_CHAR_RATIO }

  get SPATIAL_FORMAT()               { return this.parameters.SPATIAL_FORMAT              || this.DATA_TYPES?.storageOptions.SPATIAL_FORMAT};
  get CIRCLE_FORMAT()                { return this.parameters.CIRCLE_FORMAT               || this.DATA_TYPES?.storageOptions.CIRCLE_FORMAT};
  get TIMESTAMP_PRECISION()          { return this.parameters.TIMESTAMP_PRECISION         || this.DATA_TYPES?.TIMESTAMP_PRECISION }

  get COMMIT_RATIO()                 { return this.parameters.hasOwnProperty('COMMIT_RATIO') ?  this.parameters.COMMIT_RATIO : DBIConstants.COMMIT_RATIO };
  get BATCH_LIMIT()                  { return this.parameters.hasOwnProperty('BATCH_LIMIT') ?  this.parameters.BATCH_LIMIT : DBIConstants.BATCH_LIMIT };

  get LOCAL_STAGING_AREA()           { return YadamuLibrary.macroSubstitions((this.parameters.LOCAL_STAGING_AREA     || DBIConstants.LOCAL_STAGING_AREA || ''), this.yadamu.MACROS || '') }
  get REMOTE_STAGING_AREA()          { return YadamuLibrary.macroSubstitions((this.parameters.REMOTE_STAGING_AREA    || DBIConstants.REMOTE_STAGING_AREA || ''), this.yadamu.MACROS || '') }

  get IDENTIFIER_TRANSFORMATION()    { return this.yadamu.IDENTIFIER_TRANSFORMATION }
  get PARTITION_LEVEL_OPERATIONS()   { return this.parameters.PARTITION_LEVEL_OPERATIONS  || DBIConstants.PARTITION_LEVEL_OPERATIONS }
  
  get RECONNECT_ON_ABORT()           { const retVal = this._RECONNECT_ON_ABORT; this._RECONNECT_ON_ABORT = false; return retVal }
  set RECONNECT_ON_ABORT(v)          { this._RECONNECT_ON_ABORT = v }

  #SQL_TRACE                         = undefined
  get SQL_TRACE()                    { return this.#SQL_TRACE }
  set SQL_TRACE(writer)              { this.#SQL_TRACE = this.#SQL_TRACE || new SQLTrace(writer,this.STATEMENT_SEPERATOR,this.STATEMENT_TERMINATOR,this.ROLE)}
  get SQL_CUMLATIVE_TIME()           { return this.SQL_TRACE.SQL_CUMLATIVE_TIME }
  
  get SQL_TRUNCATE_TABLE_OPERATION() { return 'TRUNCATE TABLE' }
        
  get BATCH_SIZE() {
    this._BATCH_SIZE = this._BATCH_SIZE || (() => {
      let batchSize =  this.parameters.BATCH_SIZE || DBIConstants.BATCH_SIZE
      batchSize = isNaN(batchSize) ? this.parameters.BATCH_SIZE : batchSize
      batchSize = Math.abs(Math.ceil(batchSize))
      return batchSize

    })()
    return this._BATCH_SIZE 
  }

  get COMMIT_COUNT() {    
    this._COMMIT_COUNT = this._COMMIT_COUNT || (() => {
      let commitCount = isNaN(this.COMMIT_RATIO) ? DBIConstants.COMMIT_RATIO : this.COMMIT_RATIO
      commitCount = Math.abs(Math.ceil(commitCount))
      commitCount = commitCount * this.BATCH_SIZE
      return commitCount
    })()
    return this._COMMIT_COUNT
  }
  
  // Override based on local parameters object ( which under the test harnesss may differ from the one obtained from yadamu in the constructor).
  
  get PARALLEL()                      { return this.parameters.PARALLEL === 0 ? 0 : (this.parameters.PARALLEL || this.yadamu.PARALLEL)}

  get FILE()                          { return this.parameters.FILE        || this.yadamu.FILE }  
  get CIPHER()                        { return this.parameters.CIPHER      || this.yadamu.CIPHER }
  get SALT()                          { return this.parameters.SALT        || this.yadamu.SALT }
  get COMPRESSION()                   { return this.parameters.COMPRESSION || this.yadamu.COMPRESSION }
  get PASSPHRASE()                    { return this.parameters.PASSPHRASE }
  
  get ENCRYPTION()                    { return this.parameters.hasOwnProperty('ENCRYPTION') ? this.parameters.ENCRYPTION : this.yadamu.ENCRYPTION }

  get ENCRYPTION_KEY_AVAILABLE()      { return this.parameters.ENCRYPTION_KEY_AVAILABLE }
  get ENCRYPTION_KEY()                { return this._ENCRYPTION_KEY }
  set ENCRYPTION_KEY(v)               { this._ENCRYPTION_KEY = v}  	
    
  get EXCEPTION_FOLDER()              { return this.parameters.EXCEPTION_FOLDER        || this.yadamu.EXCEPTION_FOLDER }
  get EXCEPTION_FILE_PREFIX()         { return this.parameters.EXCEPTION_FILE_PREFIX   || this.yadamu.EXCEPTION_FILE_PREFIX }
  get REJECTION_FOLDER()              { return this.parameters.REJECTION_FOLDER        || this.yadamu.REJECTION_FOLDER }
  get REJECTION_FILE_PREFIX()         { return this.parameters.REJECTION_FILE_PREFIX   || this.yadamu.REJECTION_FILE_PREFIX }
  get WARNING_FOLDER()                { return this.parameters.WARNING_FOLDER          || this.yadamu.WARNING_FOLDER }
  get WARNING_FILE_PREFIX()           { return this.parameters.WARNING_FILE_PREFIX     || this.yadamu.WARNING_FILE_PREFIX }
  
  get TRANSACTION_IN_PROGRESS()       { return this._TRANSACTION_IN_PROGRESS === true }
  set TRANSACTION_IN_PROGRESS(v)      { this._TRANSACTION_IN_PROGRESS = v }
  
  get RECONNECT_IN_PROGRESS()         { return this._RECONNECT_IN_PROGRESS === true }
  set RECONNECT_IN_PROGRESS(v)        { this._RECONNECT_IN_PROGRESS = v }
  
  get SAVE_POINT_SET()                { return this._SAVE_POINT_SET === true }
  set SAVE_POINT_SET(v)               { this._SAVE_POINT_SET = v }

  set ACTIVE_INPUT_STREAM(v)          { this._ACTIVE_INPUT_STREAM = v }
  get ACTIVE_INPUT_STREAM()           { return this._ACTIVE_INPUT_STREAM }

  set PREVIOUS_PIPELINE_ABORTED(v)    { this._PREVIOUS_PIPELINE_ABORTED = v }
  get PREVIOUS_PIPELINE_ABORTED()     { return this._PREVIOUS_PIPELINE_ABORTED }

  // get ATTEMPT_RECONNECTION()       { return ((this.ON_ERROR !== 'ABORT' || this.RECONNECT_ON_ABORT)) && !this.RECONNECT_IN_PROGRESS}
  get ATTEMPT_RECONNECTION()          { return !this.RECONNECT_IN_PROGRESS}

  get SOURCE_DIRECTORY()              { return this.parameters.SOURCE_DIRECTORY || this.parameters.DIRECTORY }
  get TARGET_DIRECTORY()              { return this.parameters.TARGET_DIRECTORY || this.parameters.DIRECTORY }

  get IS_READER()                     { return this.parameters.hasOwnProperty('FROM_USER') && !this.parameters.hasOwnProperty('TO_USER') }
  get IS_WRITER()                     { return !this.parameters.hasOwnProperty('FROM_USER') && this.parameters.hasOwnProperty('TO_USER') }
  
  get IS_FILE_BASED()                 { return false }

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
    })()
    return this._CURRENT_SCHEMA 
  }
  
  get ROLE()                          {                     
    this._ROLE = this._ROLE || (() => {
      switch (true) { 
	    case (this.parameters.MODE === YadamuConstants.COMAPRE_ROLE):
		  return YadamuConstants.COMARE_ROLE
        case this.IS_READER: 
          return YadamuConstants.READER_ROLE; 
        case this.IS_WRITER: 
          return YadamuConstants.WRITER_ROLE; 
        default: 
          return undefined 
      }
    })()
    return this._ROLE  
  }
  
  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.STAGING_UNSUPPORTED }

  // Not available until configureConnection() has been called 

  get DATABASE_VERSION()              { return this._DATABASE_VERSION }
  get CONNECTION_NAME()               { return this.parameters.CONNECTION_NAME}

  get SPATIAL_SERIALIZER()            { return this._SPATIAL_SERIALIZER }
  set SPATIAL_SERIALIZER(v)           { this._SPATIAL_SERIALIZER = v }
   
  get IDENTIFIER_MAPPINGS()            { return this._IDENTIFIER_MAPPINGS }
  set IDENTIFIER_MAPPINGS(v)           { this._IDENTIFIER_MAPPINGS = v }
   
  get INBOUND_SPATIAL_FORMAT()        { return this.systemInformation?.driverSettings?.spatialFormat       || this.DATA_TYPES.storageOptions.SPATIAL_FORMAT};
  get INBOUND_CIRCLE_FORMAT()         { return this.systemInformation?.driverSettings?.circleFormat        || this.DATA_TYPES.storageOptions.CIRCLE_FORMAT};
  get INBOUND_TIMESTAMP_PRECISION()   { return this.systemInformation?.driverSettings?.timestampPrecision  || this.DATA_TYPES.TIMESTAMP_PRECISION};

  get TABLE_FILTER()                  { 
    this._TABLE_FILTER || this._TABLE_FILTER || (() => {
      const tableFilter =  typeof this.parameters.TABLES === 'string' ? this.loadTableList(this.parameters.TABLES) : (this.parameters.TABLES || [])
      // Filter for Unqiueness just to be safe.
      this._TABLE_FILTER =  tableFilter.filter((value,index) => {
        return tableFilter.indexOf(value) === index
      }) 
    })()
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
      this._REPORT_COMMITS = ((this._FEEDBACK_MODEL === 'COMMIT') || (this._FEEDBACK_MODEL === 'ALL')) 
      return this._REPORT_COMMITS
    })() 
  }
  
  get REPORT_BATCHES()     { 
     return this._REPORT_BATCHES || (() => { 
       this._REPORT_BATCHES = ((this._FEEDBACK_MODEL === 'BATCH') || (this._FEEDBACK_MODEL === 'ALL')) 
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
  
  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }
  
  get ERROR_STATE()        { return this._ERROR_STATE }
  
  get LATEST_ERROR()       { return this._ERROR_STATE.LATEST_ERROR }
  set LATEST_ERROR(v)      { this._ERROR_STATE.LATEST_ERROR = v }

  get PARTITIONED_TABLE_STATE() {
    return this.isManager() ? this._PARTITIONED_TABLE_STATE : this.manager.PARTITIONED_TABLE_STATE[this.PIPELINE_STATE.partitionedTableName]
  }
  
  set PARTITIONED_TABLE_STATE(v) {
    if (this.isManager()) {
      this._PARTITIONED_TABLE_STATE = v
    }
    else {
     this.manager.PARTITIONED_TABLE_STATE[this.PIPELINE_STATE.partitionedTableName] = v
    }
  }

  setConnectionProperties(connectionSettings) {
    this.CONNECTION_SETTINGS = connectionSettings
	this.vendorParameters  = {}
	Object.keys(connectionSettings.parameters || {}).forEach((key) => {
  	  this.vendorParameters[key.toUpperCase()] = connectionSettings.parameters[key];
    })
    this.vendorSettings = connectionSettings.settings || {}
  }
   
  redactPasswords() {
	   
	const connectionProperties = structuredClone(this.CONNECTION_PROPERTIES)
	connectionProperties.password = '#REDACTED'
	return connectionProperties
	
  }

  setDefaultConnectionProperties(connectionProperties) {
    connectionProperties.user      = this.parameters.USERNAME  || connectionProperties.user
    connectionProperties.host      = this.parameters.HOSTNAME  || connectionProperties.host
    connectionProperties.database  = this.parameters.DATABASE  || connectionProperties.database 
    connectionProperties.password  = this.parameters.PASSWORD  || connectionProperties.password
    connectionProperties.port      = this.parameters.PORT      || connectionProperties.port 
  }
  
  addVendorExtensions(connectionProperties) {
  }
  
  #CONNECTION_PROPERTIES = undefined
  
  get CONNECTION_PROPERTIES () {
    this.#CONNECTION_PROPERTIES  = this.#CONNECTION_PROPERTIES || (() => {
	  const connectionProperties = {
		... this.CONNECTION_SETTINGS[this.DATABASE_KEY]
	  }
	  this.setDefaultConnectionProperties(connectionProperties)
	  return this.addVendorExtensions(connectionProperties)
    })();
    return this.#CONNECTION_PROPERTIES 
  }
  
  #CONNECTION_SETTINGS = {}
  
  get CONNECTION_SETTINGS()  { return this.#CONNECTION_SETTINGS }
  set CONNECTION_SETTINGS(v) { 0
	this.#CONNECTION_SETTINGS = v 
  }
  
  #COMPARITOR_CLASS                     = Comparitor 
  get COMPARITOR_CLASS()                { return this.#COMPARITOR_CLASS }
  set COMPARITOR_CLASS(v)               { this.#COMPARITOR_CLASS = v }
   
  #DATA_TYPES                           = undefined
  get DATA_TYPES()                      { return this.#DATA_TYPES }
  set DATA_TYPES(DriverDataTypes)       { this.#DATA_TYPES = this.#DATA_TYPES || new DriverDataTypes(); return this.#DATA_TYPES }

  #PARSER_CLASS                         = undefined
  get PARSER_CLASS()                    { return this.#PARSER_CLASS }
  set PARSER_CLASS(v)                   { this.#PARSER_CLASS = v }

  #STATEMENT_GENERATOR_CLASS            = undefined
  get STATEMENT_GENERATOR_CLASS()       { return this.#STATEMENT_GENERATOR_CLASS }
  set STATEMENT_GENERATOR_CLASS(v)      { this.#STATEMENT_GENERATOR_CLASS = v }

  #STATEMENT_LIBRARY_CLASS              = undefined
  get STATEMENT_LIBRARY_CLASS()         { return this.#STATEMENT_LIBRARY_CLASS }
  set STATEMENT_LIBRARY_CLASS(v)        { this.#STATEMENT_LIBRARY_CLASS = v }
  
  #STATEMENT_LIBRARY                    = undefined
  get STATEMENT_LIBRARY()               { return this.#STATEMENT_LIBRARY }
  set STATEMENT_LIBRARY(v)              { this.#STATEMENT_LIBRARY = v }
  
  #OUTPUT_MANAGER_CLASS                 = undefined  
  get OUTPUT_MANAGER_CLASS()            { return this.#OUTPUT_MANAGER_CLASS }
  set OUTPUT_MANAGER_CLASS(v)           { this.#OUTPUT_MANAGER_CLASS = v }

  #WRITER_CLASS                         = undefined  
  get WRITER_CLASS()                    { return this.#WRITER_CLASS }
  set WRITER_CLASS(v)                   { this.#WRITER_CLASS = v }

  #DATABASE_ERROR_CLASS                 = DatabaseError
  get DATABASE_ERROR_CLASS()            { return this.#DATABASE_ERROR_CLASS }
  set DATABASE_ERROR_CLASS(v)           { this.#DATABASE_ERROR_CLASS = v }

  constructor(yadamu,manager,connectionSettings,parameters) {
	  
    super()
    this.DRIVER_ID = performance.now()
    yadamu.activeConnections.add(this)
    this.DESTROYED = false;
    
    this.yadamu = yadamu;
    this.setConnectionProperties(connectionSettings || {})
    this.status = yadamu.STATUS
    
    this.LOGGER = yadamu.LOGGER;
	this.DEBUGGER = yadamu.DEBUGGER
	
    this.initializeParameters(parameters || {})
    this.FEEDBACK_MODEL = this.parameters.FEEDBACK
    // if (manager) console.log(this.parameters,this.constructor.name,manager.ROLE,manager.CURRENT_SCHEMA,manager.FROM_USER,manager.TO_USER,this.parameters.FROM_USER,this.parameters.TO_USER,this.IS_READER,this.IS_WRITER,this.ROLE)
    this.SQL_TRACE = this.status.sqlLogger

    this.options = {
      recreateSchema : false
    }
    
    this._DATABASE_VERSION = 'N/A'    

	this._ERROR_STATE = {
	  LATEST_ERROR  : undefined
	}

    this.systemInformation = undefined;
    this.metadata = undefined;
    this.connection = undefined;    
    this.statementCache = undefined;
    
    // Track Transaction and Savepoint state.
    // Needed to restore transacation state when reconnecting.
    
    this.RECONNECT_IN_PROGRESS = false
    this.TRANSACTION_IN_PROGRESS = false;
    this.SAVE_POINT_SET = false;
	
	this.ACTIVE_INPUT_STREAM = false
	this.PREVIOUS_PIPELINE_ABORTED = false
    
    this.tableInfo  = undefined;
    this.insertMode = 'Batch'
    this.skipTable = true;

    this.ddlInProgress = false;
    this.activeWriters = new Set()
    
    if (manager) {
	  // Add the Worker to the list of Active Workers here so it synchronous
      manager.activeWorkers.add(this)
      this.workerReady = new Promise((resolve,reject) => {
        this.initializeWorker(manager).then(() => { resolve() }).catch((e) => { this.LOGGER.handleException([this.DATABASE_VENDOR,'INITIALIZE WORKER'],e) })
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
        // this.LOGGER.trace([this.constructor.name],`${this.constructor.name}.on(ddlComplete): (${state instanceof Error}) "${state ? `${state.constructor.name}(${state.message})` : state}"`)
        this.ddlInProgress = false
        if (state instanceof Error) {
          this.LOGGER.ddl([this.DATABASE_VENDOR],`One or more DDL operations Failed. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`)
          this.LOGGER.handleException([this.DATABASE_VENDOR,'DDL'],state)
          state.ignoreUnhandledRejection = true;
          reject(state)
        }
        else {
          this.LOGGER.ddl([this.DATABASE_VENDOR],`Executed ${Array.isArray(state) ? state.length : undefined} DDL operations. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`)
          if (this.PARTITION_LEVEL_OPERATIONS) {
            // Need target schema metadata to determine if we can perform partition level write operations.
            this.getSchemaMetadata().then((metadata) => { this.partitionMetadata = this.getPartitionMetadata(metadata) ;resolve(true) }).catch((e) => { reject(e) })
          }
          else {
            resolve(true)
          }
        }
      }).once(YadamuConstants.DDL_UNNECESSARY,() => {
        resolve(true)
      })
    })
	
	this.readyForData = new Promise((resolve,reject) => { resolve(false) })
 
    this.PARTITIONED_TABLE_STATE = {}

  }
  
  isReadyForData() {
	return this.isManager() ? this.readyForData : this.manager.readyForData
  }

  initializeDataTypes(DataTypes) {
    
    // this.yadamu.initializeDataTypes(DataTypes,this.TYPE_CONFIGURATION);
  }

  async initializeWorker(manager) {
	  	
    // this.LOGGER.trace([this.constructor.name,this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`initializeWorker(${manager.activeWorkers.size})`)
	
	try {
      this.manager = manager
      this.workerNumber = -1
      this.cloneSettings()
     
      // Set up a promise to make it possible to wait on AllSettled..   
      this.workerState = new Promise((resolve,reject) => {
        this.on(YadamuConstants.DESTROYED,() => {
          manager.activeWorkers.delete(this)
          resolve(YadamuConstants.DESTROYED)
        })
      })
      
      
      await this.setWorkerConnection()
      await this.configureConnection()
	} catch (e) {
	  this.LOGGER.handleException([this.DATABASE_VENDOR,'INITIALIZE WORKER'],e)
	  manager.activeWorkers.delete(this)
	}
  }
  
  loadTableList(tableListPath) {
    try {
      const tableList = YadamuLibrary.loadJSON(tableListPath,this.LOGGER) 
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

  setSchema(schema,key) {
    
     switch (true) {
       case (schema.owner === 'dbo'):
         this.parameters[key] = schema.database
		 break;
	   case (schema.hasOwnProperty('owner')):
         this.parameters[key] = schema.owner
		 break
       default:
         this.parameters[key] = schema.schema
     }
	 this.DESCRIPTION = `"${this.CURRENT_SCHEMA}"`
  }
  
  getSchema(schema) {
    
     switch (true) {
       case (schema.owner === 'dbo'):
         return schema.database
		 break;
	   case (schema.hasOwnProperty('owner')):
         return schema.owner
		 break
       default:
         return schema.schema
     }
  }
   
  initializeParameters(parameters){
    
	// Merge default parameters for this driver with parameters from configuration files and command line parameters. Map Key names to uppercase
	
	// Override values from DBI_PARAMETER with values from the YADAMU controller
	
    this.parameters = Object.fromEntries(Object.entries({
	  ...this.DBI_PARAMETERS
	, ...(this.yadamu.ENCRYPTION) || (this.yadamu.ENCRYPTION === false) && {ENCRYPTION : this.yadamu.ENCRYPTION}
	, ...(this.yadamu.CIPHER)                                           && {CIPHER     : this.yadamu.CIPHER}
	, ...(this.yadamu.SALT)                                             && {SALT       : this.yadamu.SALT}
	, ...(this.yadamu.FILE)                                             && {FILE       : this.yadamu.FILE}
	, ...(this.yadamu.parameters.BUCKET)                                && {BUCKET     : this.yadamu.parameters.BUCKET}
	, MODE : this.yadamu.MODE
	, ...this.vendorParameters
	, ...parameters
	, ...this.yadamu.COMMAND_LINE_PARAMETERS
	}).map(([k, v]) => [k.toUpperCase(), v]))

	
    /*
	**

	console.log(this.constructor.name,'initializeParameters','Begin Parameter Dump')
	console.log('DBI_PARAMETERS',this.DBI_PARAMETERS)
	console.log('vendorParameters',this.vendorParameters)
	console.log('parameters',parameters)
	console.log('COMMAND_LINE_PARAMETERS',this.yadamu.COMMAND_LINE_PARAMETERS)
    console.log('this.parameters',this.parameters)
    console.log('this.yadamu.parameters',this.yadamu.parameters)
    console.log('this.parameters',this.yadamu.parameters)
	console.log(this.constructor.name,'initializeParameters','End Parameter Dump')
	
	**
	*/
    
  }

  setParameters(parameters) {
    // Used when creating a worker.
    Object.assign(this.parameters, parameters || {})
    this._COMMIT_COUNT = undefined
    this.FEEDBACK_MODEL = this.parameters.FEEDBACK
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

    const logDML         = (status.loglevel && (status.loglevel > 0))
    const logDDL         = (status.loglevel && (status.loglevel > 1))
    const logDDLMsgs     = (status.loglevel && (status.loglevel > 2))
    const logTrace       = (status.loglevel && (status.loglevel > 3))

    if (status.logTrace) {
      yadamuLogger.writeLogToFile([this.DATABASE_VENDOR],log)
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
          yadamuLogger.info([this.DATABASE_VENDOR],`"${JSON.stringify(logEntry)}".`)
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
          this.processError(yadamuLogger,logEntry,summary,logDDLMsgs)
      } 
      if (logEntry.sqlStatement) { 
        this.SQL_TRACE.traceSQL(logEntry.sqlStatement)
      }
    }) 
    
    if (summary.exceptions.length > 0) {
      this.LOGGER.error([this.DATABASE_VENDOR, status.operation, operation],`Server side operation resulted in ${summary.exceptions.length} errors.`)
      const err = new Error(`${this.DATABASE_VENDOR} ${operation} failed.`)
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
    this.SQL_TRACE.comment(`DISCONNECT : Properies: ${JSON.stringify(this.redactPasswords())}`)
  }
  
  logConnectionProperties() {    
    this.SQL_TRACE.comment(`CONNECT : Properies: ${JSON.stringify(this.redactPasswords())}`)
  }
         
  isValidDDL() {
    return ((this.systemInformation.vendor === this.DATABASE_VENDOR) && (this.systemInformation.dbVersion <= this.DATABASE_VERSION))
  }
  
  isDatabase() {
    return true;
  }

  resetExceptionTracking() {
    this.LATEST_ERROR = undefined
  }
   
  trackExceptions(err) {
    this.LATEST_ERROR = err
    return err
  } 
   
  createDatabaseError(cause,stack,sql) {
    return new this.DATABASE_ERROR_CLASS(this,cause,stack,sql)
  }
  
  getDatabaseException(cause,stack,sql) {
    const err = this.createDatabaseError(cause,stack,sql)
    return this.trackExceptions(err)
  }

  raisedError(error) {
	return error === this.LATEST_ERROR
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
      
    const sourceKeys = Object.keys(source)
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

    const databaseMappings = this.generateDatabaseMappings(mappedMetadata)
    this.mergeMappings(databaseMappings, this.yadamu.IDENTIFIER_MAPPINGS)
    this.metadata = YadamuLibrary.isEmpty(databaseMappings) ?  mappedMetadata : this.applyIdentifierMappings(mappedMetadata,databaseMappings,true)
    this.IDENTIFIER_MAPPINGS = this.mergeMappings(generatedMappings,databaseMappings)
  }
 
  applyIdentifierMappings(metadata,mappings,reportMappedIdentifiers) {
	  
	  console.log(metadata,mappings,reportMappedIdentifiers, new Error().stack) 
      
    // This function does not change the names of the keys in the metadata object.
    // It only changes the value of the tableName property associated with a mapped tables.
    
    Object.keys(metadata).forEach((table) => {
      const tableMappings = mappings[table]
      if (tableMappings) {
        if ((tableMappings.tableName) && (metadata[table].tableName !== tableMappings.tableName)) {
          if (reportMappedIdentifiers) { 
            this.LOGGER.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',metadata[table].tableName],`Table name re-mapped to "${tableMappings.tableName}".`)
          }
          metadata[table].tableName = tableMappings.tableName
        }
        if (tableMappings.columnMappings) {
          const columnNames = metadata[table].columnNames
          const dataTypes = metadata[table].dataTypes
          Object.keys(tableMappings.columnMappings).forEach((columnName) => {
            const idx = columnNames.indexOf(columnName)
            const mappedColumnName = tableMappings.columnMappings[columnName].name || columnNames[idx]
            // const mappedDataType = tableMappings.columnMappings[columnName].dataType || dataTypes[idx]
            if (idx > -1) {
              if (reportMappedIdentifiers) { 
                if (columnNames[idx] !== mappedColumnName) {
                  this.LOGGER.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',table,columnName],`Column name re-mapped to "${mappedColumnName}".`)
                }
                /*
                if (dataTypes[idx] !== mappedDataType) {
                  this.LOGGER.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',table,columnName],`Data type "${dataTypes[idx] }" re-mapped to "${mappedDataType}".`)
                }
                */
              }
              columnNames[idx] = mappedColumnName          
              // dataTypes[idx] = mappedDataType       
            }
          })
          // metadata[table].columnNames = columnNames
        }
      }   
    })
    return metadata 
  }
  
  applyIdentifierMappings(metadata,mappings,reportMappedIdentifiers) {
      
    // This function does not change the names of the keys in the metadata object.
    // It only changes the value of the tableName property associated with a mapped tables.
    
    Object.keys(metadata).forEach((table) => {
      const tableMappings = mappings[table]
      if (tableMappings) {
        if ((tableMappings.tableName) && (metadata[table].tableName !== tableMappings.tableName)) {
          if (reportMappedIdentifiers) { 
            this.LOGGER.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',metadata[table].tableName],`Table name re-mapped to "${tableMappings.tableName}".`)
          }
          metadata[table].tableName = tableMappings.tableName
        }
        if (tableMappings.columnMappings) {
          const columnNames = metadata[table].columnNames
          Object.keys(tableMappings.columnMappings).forEach((columnName) => {
            const idx = columnNames.indexOf(columnName)
            const mappedColumnName = tableMappings.columnMappings[columnName].name || columnNames[idx]
            if (idx > -1) {
              if (reportMappedIdentifiers) { 
                if (columnNames[idx] !== mappedColumnName) {
                  this.LOGGER.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',table,columnName],`Column name re-mapped to "${mappedColumnName}".`)
                }
              }
              columnNames[idx] = mappedColumnName          
            }
          })
        }
      }   
    })
    return metadata 
  }
  
  applyDataTypeMappings(tableName,columnNames,mappedDataTypes,mappings,reportMappedIdentifiers) {
      
    const tableMappings = mappings[tableName]
    if (tableMappings && tableMappings.columnMappings) {
      Object.keys(tableMappings.columnMappings).forEach((columnName) => {
        const idx = columnNames.indexOf(columnName)
        const mappedDataType = tableMappings.columnMappings[columnName].dataType || mappedDataTypes[idx]
        if (idx > -1) {
          if (mappedDataTypes[idx] !== mappedDataType) {
            this.LOGGER.info([this.DATABASE_VENDOR,'IDENTIFIER MAPPING',tableName,columnName],`Data type "${mappedDataTypes[idx] }" re-mapped to "${mappedDataType}".`)
          }
          mappedDataTypes[idx] = mappedDataType       
        }
      })
    }
    return mappedDataTypes  
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
    const startTime = performance.now()
    try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA)
        // this.SQL_TRACE.traceSQL(ddlStatement)
        return this.executeSQL(ddlStatement,{})
      }))
    } catch (e) {
     const exceptionFile = this.LOGGER.handleException([this.DATABASE_VENDOR,'DDL'],e)
     await this.LOGGER.writeMetadata(exceptionFile,this.yadamu,this.systemInformation,this.metadata)
     results = e;
    }
    return results;
  }
    
  async skipDDLOperations() {
     this.emit(YadamuConstants.DDL_UNNECESSARY,performance.now,[])
  }
  
 
  async executeDDL(ddl) {
    if (ddl.length > 0) {
      const startTime = performance.now()
      this.ddlInProgress = true
      try {
        const results = await this._executeDDL(ddl)
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
  
  async truncateTable(schema,tableName) {
	 
	 
	const activeTransaction = this.TRANSACTION_IN_PROGRESS
	if (this.TRANSACTION_IN_PROGRESS) {
	  await this.rollbackTransaction()
	}
	
	const sqlStatement = `${this.SQL_TRUNCATE_TABLE_OPERATION} "${schema}"."${tableName}"`
	
	await this.beginTransaction()
	await this.executeSQL(sqlStatement)
	await this.commitTransaction()
	
	if (activeTransaction) {
	  await this.beginTransaction()
	}
	
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
    this.LOGGER.ddl([this.DATABASE_VENDOR],`Generated ${ddlStatements.length === 0 ? 'no' : ddlStatements.length} "Create Table" statements and ${dmlStatementCount === 0 ? 'no' : dmlStatementCount} DML statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`)
    ddlStatements = this.prepareDDLStatements(ddlStatements)    
    return ddlStatements    
  }  
  
  async _getDatabaseConnection() {
      
    let connected = false;
    try {
      await this.createConnectionPool()
      this.connection = await this.getConnectionFromPool()
      connected = true
      await this.configureConnection()
    } catch (e) {
      const err = connected ? e : new ConnectionError(e,this.redactPasswords())
      throw err
    }

  }  
  
  async _reconnect() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
  }

  trackLostConnection() {
   
    /*
    **
	** Invoked when the connection is lost. 
	** If the WRITE connection is lost any rows written but committed cannot be recovered and are lost.
    ** If a READ connection is lost the mode (ABORT/SKIP/FLUSH/RETRY) will determine what happens to rows written but not yet committed.
    **
    */
 
    if (this.IS_WRITER === true) {
      if (this.PIPELINE_STATE?.written > 0) {
        this.LOGGER.error([`RECONNECT`,this.DATABASE_VENDOR],`${this.PIPELINE_STATE.written} uncommitted rows discarded when connection lost.`)
        this.PIPELINE_STATE.lost += this.PIPELINE_STATE.written;
        this.PIPELINE_STATE.written = 0;
      }
    }
  }   
  
  async reconnect(cause,operation) {

    // Reconnect. If rows were lost as a result of the reconnect, the original error will be thrown after the reconnection is complete
    
    cause.yadamuReconnected = false;

    this.RECONNECT_IN_PROGRESS = true;
    const TRANSACTION_IN_PROGRESS = this.TRANSACTION_IN_PROGRESS 
    const SAVE_POINT_SET = this.SAVE_POINT_SET

    this.LOGGER.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`Connection Lost: Attemping reconnection.`)
    
    if (cause instanceof Error) {
      this.LOGGER.handleWarning([`RECONNECT`,this.DATABASE_VENDOR,operation],cause)
    }
    
    /*
    **
    ** If a connection is lost while performing batched insert operatons using a table writer, adjust the table writers running total of records written but not committed. 
    ** When a connection is lost records that have written but not committed will be lost (rolled back by the database) when cleaning up after the lost connection.
    ** To avoid the possibility of lost batches set COMMIT_RATIO to 1, so each batch is committed as soon as it is written.
    **
    */
    
    this.trackLostConnection()
    
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
          this.LOGGER.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`Error closing existing connection.`)
          this.LOGGER.handleException([`RECONNECT`,this.DATABASE_VENDOR,operation],e)
        }
      }  
         
      try {
        await this._reconnect()
        await this.configureConnection()
        if (TRANSACTION_IN_PROGRESS) {
          await this.beginTransaction()
          if (SAVE_POINT_SET) {
            await this.createSavePoint()
          }
        }

        this.LOGGER.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`New connection available.`)
        this.failedPrematureClose = false;
        cause.yadamuReconnected = true;
      } catch (connectionFailure) {
        // Reconnection failed. If cause is "server unavailable" wait 0.5 seconds and retry (up to 10 times)
        if (YadamuError.serverUnavailable(connectionFailure)) {
          connectionUnavailable = connectionFailure;
          this.LOGGER.info([`RECONNECT`,this.DATABASE_VENDOR,operation],`Waiting for restart.`)
          await setTimeout(5000)
          retryCount++;
          continue;
        }
        else {
          // Reconnection attempt failed for some other reason. Throw the error
          this.RECONNECT_IN_PROGRESS = false;
          this.LOGGER.handleException([`RECONNECT`,this.DATABASE_VENDOR,operation],connectionFailure)
          throw cause;
        }
      }
      // Sucessfully reonnected. Throw the original error if rows were lost as a result of the lost connection
      this.RECONNECT_IN_PROGRESS = false;
      if ((this.PIPELINE_STATE !== undefined) && (this.PIPELINE_STATE.lost > 0)) { 
        throw cause
      }
      return
    }
    // Unable to reconnect after 10 attempts
    this.RECONNECT_IN_PROGRESS = false;
    throw connectionUnavailable     
  }
  
  async getDatabaseConnection(requirePassword) {
    let interactiveCredentials = (requirePassword && ((this.CONNECTION_PROPERTIES[this.PASSWORD_KEY_NAME] === undefined) || (this.CONNECTION_PROPERTIES[this.PASSWORD_KEY_NAME].length === 0))) 
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
          this.CONNECTION_PROPERTIES[this.PASSWORD_KEY_NAME] = process.env.YADAMU_PASSWORD
        }
        else {
          const pwQuery = this.yadamu.createQuestion(prompt)
          const password = await pwQuery;
          this.CONNECTION_PROPERTIES[this.PASSWORD_KEY_NAME] = password;
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
              throw new CommandLineError(`Unable to establish connection to ${this.DATABASE_VENDOR} after 3 attempts. Operation aborted.`)
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

  setLibraries() {
  }
  
  async initialize(requirePassword) {

    this.yadamu.initializeSQLTrace()  
    /*
    **
    ** Calculate CommitSize
    **
    */
    if (this.parameters.PARAMETER_TRACE === true) {
      this.LOGGER.writeDirect(`${util.inspect(this.parameters,{colors:true})}\n`)
    }
    
    if (this.isDatabase()) {
      await this.getDatabaseConnection(requirePassword)
      this.setLibraries()
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

  getCloseOptions(err) {    
    return err ? {abort  : false } : { abort : true, error : err }
  }
  
  async final(){
    
    // this.LOGGER.trace([this.constructor.name,'final()',this.ROLE],`Waiting for ${this.activeWorkers.size} Writers to terminate. [${this.activeWorkers}]`)
      
    if (this.ddlInProgress) {
      await this.ddlComplete
    }

    const closeOptions = this.getCloseOptions()
                
    await this.workersFinished()

    if (this.isManager()) {
      // this.LOGGER.trace([this.constructor.name,'final()',this.ROLE,'ACTIVE_WORKERS'],`WAITING [${this.activeWorkers.size}]`)
      const stillWorking = Array.from(this.activeWorkers).map((worker) => { return worker.workerState })
      await Promise.all(stillWorking)
      // this.LOGGER.trace([this.constructor.name,'final()',this.ROLE,'ACTIVE_WORKERS'],'PROCESSING')
    }     

    await this.closeConnection(closeOptions)
    this.logDisconnect()
    await this.closePool(closeOptions)
        
  } 

  async destroy(err) {
	 
    // this.LOGGER.trace([this.constructor.name,this.ROLE,this.getWorkerNumber(),this.DESTROYED],`doDestroy(${this.activeWorkers.size},${err ? err.message : 'normal'})`)

    /*
    **
    **  Abort the database connection and pool
    **
    */

    await this.workersFinished() 

    if (!this.DESTROYED) {

	  this.DESTROYED = true
	  
      if ((this.isManager()) &&  (this.activeWorkers.size > 0))  {
        // Active Workers contains the set of Workers that have not terminated.
        // We need to force them to terminate and release any database connections they own.
        this.LOGGER.error([this.DATABASE_VENDOR,this.ROLE,'destroy()'],`Terminating ${this.activeWorkers.size} active workers.`)
        await Promise.allSettled(Array.from(this.activeWorkers).map((worker) => {
          try {
            // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,'destroy()','Worker',this.getWorkerNumber()],`Terminating Worker ${worker.getWorkerNumber()}`)
            return worker.destroyWorker()
          } catch(e) {
            this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'destroy()','Worker',worker.getWorkerNumber()],e)
			
          }
        }))
        this.LOGGER.error([this.DATABASE_VENDOR,this.ROLE,'destroy()'],`Workers terminated.`)
      }
     
      const closeOptions = this.getCloseOptions(err)
      if (this.connection) {      
        try {
          await this.closeConnection(closeOptions)
          this.logDisconnect()
        } catch (e) {
          if (!YadamuError.lostConnection(e)) {
            this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'destroy()','closeConnection()'],e)
          }
        }
      }
      
	  if (this.pool) {
        try {
          // Force Termnination of All Current Connections.
          await this.closePool(closeOptions)
        } catch (e) {
          if (!YadamuError.lostConnection(e)) {
            this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'destroy()','closePool()'],e)
          }
        }
      }
      
      this.yadamu.activeConnections.delete(this)
	}
        
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
        this.LOGGER.handleException([this.DATABASE_VENDOR,operation],newError)
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

      const startTime = performance.now()
      const jsonHndl = await this.uploadFile(this.UPLOAD_FILE)
      const elapsedTime = performance.now() - startTime;
      this.LOGGER.info([this.DATABASE_VENDOR,`UPLOAD`],`File "${this.UPLOAD_FILE}". Size ${fileSizeInBytes}. Elapsed time ${YadamuLibrary.stringifyDuration(elapsedTime)}s.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.`)
      await this.initializeImport()
      const log = await this.processFile(jsonHndl)
      await this.finalizeImport()
      return log
    } catch (err) {
      throw err.code === 'ENOENT' ? new FileNotFound(this,err,stack,this.UPLOAD_FILE) : new FileError(this,err,stack,this.UPLOAD_FILE)
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
  
  getDriverSettings() {
    return {
      spatialFormat      : this.SPATIAL_FORMAT
    , timestampPrecision : this.TIMESTAMP_PRECISION
    }
  }
  
  getSystemInformation() {     
  
    return {
      yadamuVersion      : YadamuConstants.YADAMU_VERSION
    , date               : new Date().toISOString()
    , timeZoneOffset     : new Date().getTimezoneOffset()
    , schema             : this.CURRENT_SCHEMA
    , tableFilter        : this.getTABLE_FILTER
    , dbiKey             : this.DATABASE_KEY
    , vendor             : this.DATABASE_VENDOR
    , dbVersion          : this.DATABASE_VERSION
    , softwareVendor     : this.SOFTWARE_VENDOR
    , driverSettings     : this.getDriverSettings()
    , nodeClient         : {
        version          : process.version
       ,architecture     : process.arch
       ,platform         : process.platform
      }
    }
  }
  
  async getYadamuInstanceInfo() {
    const systemInfo = await this.getSystemInformation()
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
         // Return true if the table does not have an entry in the schemaInformation collection
         return !tableNames.includes(tableName)
      })
      
      if (invalidTableNames.length > 0) {
        throw new CommandLineError(`Could not resolve the following table names : "${invalidTableNames}".`)
      }
    
      this.LOGGER.info([this.DATABASE_VENDOR],`Operations restricted to the following tables: ${JSON.stringify(this.TABLE_FILTER)}.`)
         
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
      table.SIZE_CONSTRAINT_ARRAY = typeof table.SIZE_CONSTRAINT_ARRAY === 'string' ? JSON.parse(table.SIZE_CONSTRAINT_ARRAY) : table.SIZE_CONSTRAINT_ARRAY
      const tableMetadata =  {
        tableSchema              : table.TABLE_SCHEMA
      , tableName                : table.TABLE_NAME
      , columnNames              : table.COLUMN_NAME_ARRAY
      , dataTypes                : table.DATA_TYPE_ARRAY.map((DATA_TYPE) => {return YadamuDataTypes.decomposeDataType(DATA_TYPE).type})
      , sizeConstraints          : table.SIZE_CONSTRAINT_ARRAY
      , vendor                   : this.DATABASE_VENDOR 
	  , skipColumnReordering       : table.SKIP_COLUMN_REORDERING === true
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
    schemaColumnInfo.filter((columnInfo) => {
      return ((this.TABLE_FILTER.length === 0) || (this.TABLE_FILTER.includes(columnInfo[1])))
    }).forEach((columnInfo) => {
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
      const dataType = YadamuDataTypes.decomposeDataType(columnInfo[3])
      tableInfo.COLUMN_NAME_ARRAY.push(columnInfo[2])
      tableInfo.DATA_TYPE_ARRAY.push(dataType.typeQualifier ? `${dataType.type} ${dataType.typeQualifier}` : dataType.type)
      tableInfo.SIZE_CONSTRAINT_ARRAY.push(dataType.length ? dataType.scale ? [dataType.length,dataType.scale] : [dataType.length] : [])
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
	 // Called dbWriter when all the data for all tables has been written.  Used to restore any changes that were mode to the Database Schema state prior to closing connection.
  }

  async finalizeImport() {
  }
  
  async getVendorDataTypeMappings() {
    const statementGenerator = new this.STATEMENT_GENERATOR_CLASS(this,this.systemInformation.vendor, this.CURRENT_SCHEMA, {}, this.LOGGER);
    await statementGenerator.init()
    return JSON.stringify(Array.from(statementGenerator.TYPE_MAPPINGS.entries()))
  }
  
  async generateStatementCache(schema) {
    const statementGenerator = new this.STATEMENT_GENERATOR_CLASS(this,this.systemInformation.vendor,schema,this.metadata,this.LOGGER)
    this.statementCache = await statementGenerator.generateStatementCache()
    this.emit(YadamuConstants.CACHE_LOADED  )
    return this.statementCache
  }

  async finalizeRead(tableInfo) {
  }
  
  getTableInfo(tableName) {
      
    if (this.statementCache === undefined) {
      this.LOGGER.logInternalError([this.constructor.name,`getTableInfo()`,tableName],`Statement Cache undefined. Cannot obtain required information.`)
    }
    
    // Statement Cache is keyed by actual table name so we need the mapped name if there is a mapping.

    let mappedTableName = this.getMappedTableName(tableName,this.IDENTIFIER_MAPPINGS)
    const tableInfo = this.statementCache[mappedTableName]
    
    // Add Some Common Settings
    
    tableInfo.insertMode = tableInfo.insertMode || 'Batch';    
    
    tableInfo.columnCount = tableInfo.columnCount || tableInfo.columnNames.length;      
    tableInfo.skipTable = tableInfo.skipTable || this.MODE === 'DDL_ONLY';    

    if (tableInfo === undefined) {
      this.LOGGER.logInternalError([this.constructor.name,`getTableInfo()`,tableName,mappedTableName],`No Statement Cache entry for "${mappedTableName}". Current entries: ${JSON.stringify(Object.keys(this.statementCache))}`)
    }
    
    tableInfo.tableName = mappedTableName
    return tableInfo
  }
  
  adjustQuery(queryInfo) {
  }

  generateSQLQuery(tableMetadata) {
    const queryInfo = {...tableMetadata}
    queryInfo.SQL_STATEMENT = `select ${tableMetadata.CLIENT_SELECT_LIST} from "${tableMetadata.TABLE_SCHEMA}"."${tableMetadata.TABLE_NAME}" t`; 
    
    // ### TESTING ONLY: Uncomment folllowing line to force Table Not Found condition
    // queryInfo.SQL_STATEMENT = queryInfo.SQL_STATEMENT.replace(queryInfo.TABLE_NAME,queryInfo.TABLE_NAME + "1")

    queryInfo.MAPPED_TABLE_NAME = this.getMappedTableName(queryInfo.TABLE_NAME,this.IDENTIFIER_MAPPINGS) || queryInfo.TABLE_NAME
    return queryInfo
  }   

  inputStreamError(err,sqlStatement) {
     return this.trackExceptions(((err instanceof DatabaseError) || (err instanceof CopyOperationAborted)) ? err : this.createDatabaseError(err,new Error().stack,sqlStatement))
  }

  handleInputStreamError(inputStream,err,sqlStatement) {
	this.PREVIOUS_PIPELINE_ABORTED = true;
    inputStream.STREAM_STATE.state = 'ERROR'
    inputStream.STREAM_STATE.readableLength = inputStream.readableLength || 0
    inputStream.STREAM_STATE.endTime = performance.now()
    inputStream.PIPELINE_STATE.failed = true;
    inputStream.PIPELINE_STATE.errorSource = inputStream.PIPELINE_STATE.errorSource || DBIConstants.INPUT_STREAM_ID
    inputStream.STREAM_STATE.error = inputStream.STREAM_STATE.error || (inputStream.PIPELINE_STATE.errorSource === DBIConstants.INPUT_STREAM_ID) ? this.inputStreamError(err,sqlStatement) : inputStream.STREAM_STATE.error
    this.failedPrematureClose = YadamuError.prematureClose(err)
    err.pipelineComponents = [...err.pipelineComponents || [], inputStream.constructor.name]
	err.pipelineIgnoreErrors = true
    err.pipelineState = YadamuError.clonePipelineState(inputStream.PIPELINE_STATE)  
  }
  
  
  async getInputStream(queryInfo,pipelineState) {
	
	const inputStream = await this._getInputStream(queryInfo)
    
	this.ACTIVE_INPUT_STREAM = false
	this.PREVIOUS_PIPELINE_ABORTED = false;
	
	inputStream.PIPELINE_STATE = pipelineState
	inputStream.STREAM_STATE = { 
	  vendor : this.DATABASE_VENDOR 
	, state  : 'CREATED'
	}
    pipelineState[DBIConstants.INPUT_STREAM_ID] = inputStream.STREAM_STATE
    inputStream.once('readable',() => {
	  this.ACTIVE_INPUT_STREAM = true
	  inputStream.STREAM_STATE.state = 'READABLE'
      inputStream.STREAM_STATE.startTime = performance.now()
    }).on('end',() => {
	  this.ACTIVE_INPUT_STREAM = false
      inputStream.STREAM_STATE.state = 'END'
      inputStream.STREAM_STATE.endTime = performance.now()
      inputStream.STREAM_STATE.readableLength = inputStream.readableLength || 0
    }).on('error',async (err) => {
      this.handleInputStreamError(inputStream,err,queryInfo.SQL_STATEMENT)
    })	
    return inputStream;
  }      

  getParser(queryInfo,pipelineState) {
	const parser = new this.PARSER_CLASS(this,queryInfo,pipelineState,this.LOGGER)
	return parser
  }

  async getInputStreams(queryInfo,pipelineState) {

    this.PIPELINE_STATE = pipelineState
	pipelineState.readerState = this.ERROR_STATE
	this.resetExceptionTracking()

    const streams = []

	const inputStream = await this.getInputStream(queryInfo,pipelineState)
    streams.push(inputStream)
    
    const parser = this.getParser(queryInfo,pipelineState)
    streams.push(parser)
	return streams;
  }
  
  getOutputManager(tableName,pipelineState) {
    const outputManager = new this.OUTPUT_MANAGER_CLASS(this,tableName,pipelineState,this.status,this.LOGGER)
    return outputManager
  }

  getOutputStream(tableName,pipelineState) {
    // this.LOGGER.trace([this.constructor.name,`getOutputStream(${tableName})`],'')
    const outputStream = new this.WRITER_CLASS(this,tableName,pipelineState,this.status,this.LOGGER)
    return outputStream
  }
  
  getOutputStreams(tableName,pipelineState) {
    // A Writer needs to track the pipelineState do it can make decisions about whether or not to honor a reconnect() request following a lost connection. 
    this.PIPELINE_STATE = pipelineState
	pipelineState.writerState = this.ERROR_STATE
	this.resetExceptionTracking()
    const streams = []
    	
    const transformationManager = this.getOutputManager(tableName,pipelineState)
    streams.push(transformationManager)
    
    const outputStream = this.getOutputStream(tableName,pipelineState)
	streams.push(outputStream)
    
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
    this.dbConnected  = this.manager.dbConnected
    this.cacheLoaded  = this.manager.cacheLoaded
    this.ddlComplete  = this.manager.ddlComplete

    this.setParameters(this.manager.parameters)
    
    this.systemInformation      = this.manager.systemInformation
    this.metadata               = this.manager.metadata
    this.statementCache         = this.manager.statementCache
    this.statementGenerator     = this.manager.statementGenerator
    this.partitionMetadata      = this.manager.partitionMetadata
	this.#CONNECTION_PROPERTIES = this.manager.CONNECTION_PROPERTIES
    
    this.IDENTIFIER_MAPPINGS    =  this.manager.IDENTIFIER_MAPPINGS
  }   

  workerDBI(workerNumber) {
      
    // Invoked on the DBI that is being cloned. Parameter dbi is the cloned interface.
    const dbi = this.classFactory(this.yadamu)  
    dbi.workerNumber = workerNumber
    dbi.SQL_TRACE.setWorkderId(workerNumber)
    return dbi
  }
 
  async getConnectionID() {
    // ### Get a Unique ID for the connection
    throw new UnimplementedMethod('getConnectionID()',`YadamuDBI`,this.constructor.name)
  }
  
  async workersFinished() {
      
    // this.LOGGER.trace([this.constructor.name,'workersFinished()',this.ROLE,this.getWorkerNumber()],`Waiting for ${this.activeWriters.size} Writers to terminate. [${this.activeWriters}]`)

    // this.LOGGER.trace([this.constructor.name,'workersFinished()','ACTIVE_WRITERS',this.getWorkerNumber()],'WAITING')
    await Promise.allSettled(this.activeWriters)
    // this.LOGGER.trace([this.constructor.name,'workersFinished()','ACTIVE_WRITERS',this.getWorkerNumber()],'PROCESSING')

  }
  
  async destroyWorker() {
    // this.LOGGER.trace([this.constructor.name,'destroyWorker()',this.ROLE,this.getWorkerNumber()],`Terminating Worker`)
    await this.workersFinished()
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

  verifyStagingSource(source) {   
    if (!this.SUPPORTED_STAGING_PLATFORMS.includes(source)) {
      throw new YadamuError(`COPY operations not supported between "${source}" and "${this.DATABASE_VENDOR}".`)
    }
  }
  
  reportCopyOperationMode(copyEnabled,controlFilePath,contentType) {
    this.LOGGER.info([this.DATABASE_VENDOR,'COPY',`${contentType}`],`Processing ${controlFilePath}" using ${copyEnabled ? 'COPY' : 'PIPELINE' } mode.`)
    return copyEnabled
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

  async generateCopyStatements(metadata) {
    await this.setMetadata(metadata)   
    const statementCache = await this.generateStatementCache(this.CURRENT_SCHEMA)
    return statementCache
  }     

  async reportCopyErrors(tableName,pipelineState) {
  }
 
  async initializeCopy() {
  }
  
  async copyOperation(tableName,copyOperation,copyState) {
    
    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
    
    try {
      copyState.startTime = performance.now()
      let results = await this.beginTransaction()
      results = await this.executeSQL(copyOperation.dml)
      copyState.read = results.affectedRows
      copyState.written = results.affectedRows
      copyState.endTime = performance.now()
      results = await this.commitTransaction()
      copyState.committed = copyState.written 
      copyState.written = 0
    } catch(e) {
      copyState.writerError = e
      try {
        this.LOGGER.handleException([this.DATABASE_VENDOR,'COPY',tableName],e)
        let results = await this.rollbackTransaction()
      } catch (e) {
        e.cause = copyState.writerError
        copyState.writerError = e
      }
    }
    return copyState
  }
          
  async finalizeCopy() {
  }

  async getComparator(configuration) {
	 this.options.recreateSchema = false
	 await this.initialize()
	 return new this.COMPARITOR_CLASS(this,configuration)
  }

  trackPartitionedTableState(partitionState) {

    if (!this.PARTITIONED_TABLE_STATE) {
      this.PARTITIONED_TABLE_STATE = { 
        startTime : partitionState.startTime
      , endTime   : partitionState.endTime
      , read      : partitionState.read
      , committed : partitionState.committed
      , lost      : partitionState.lost
      , skipped   : partitionState.skipped
      , sqlTime   : partitionState.sqlTime
      , remaining : partitionState.partitionCount - 1
      }
    }
    else {
      const tableState = this.PARTITIONED_TABLE_STATE
      tableState.startTime = partitionState.startTime < tableState.startTime ? partitionState.startTime : tableState.startTime
      tableState.endTime =   partitionState.endTime > tableState.endTime ? partitionState.endTime : tableState.endTime
	  tableState.read+=      partitionState.read
	  tableState.committed+= partitionState.committed
	  tableState.lost +=     partitionState.lost
	  tableState.skipped+=   partitionState.skipped
	  tableState.sqlTime+=   partitionState.sqlTime
      tableState.remaining--

      if (tableState.remaining === 0) {
		const summary = this.yadamu.recordMetrics(partitionState.partitionedTableName,tableState);  
	    const timings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(summary.elapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(tableState.sqlTime))}s. Throughput: ${summary.throughput} rows/s.`
	    this.LOGGER.info([`${partitionState.partitionedTableName}`,`${partitionState.insertMode}`],`Total Rows ${tableState.committed}. ${timings}`)  
      }
	}

  }
  
  getRootCause(pipelineState,err) {
	
	switch (pipelineState.errorSource) {
	  case DBIConstants.INPUT_STREAM_ID:
	    return pipelineState[DBIConstants.INPUT_STREAM_ID].error
	  case DBIConstants.PARSER_STREAM_ID:
	    return pipelineState[DBIConstants.PARSER_STREAM_ID].error
	  case DBIConstants.TRANSFORMATION_STREAM_ID:
	    return pipelineState[DBIConstants.TRANSFORMATION_STREAM_ID].error
	  case DBIConstants.OUTPUT_STREAM_ID:
	    return pipelineState[DBIConstants.OUTPUT_STREAM_ID].error
	  default:
	    return (pipelineState.readerState.LATEST_ERROR || pipelineState.readerState.WRITER_ERROR || err)
	}
  }
  
    
  reportPipelineStatus(pipelineState,err) {
 
	const cause = this.getRootCause(pipelineState,err)

	// Use parsed as proxy for read when the reader does not maintain the counter read
	pipelineState.read = pipelineState.read || pipelineState.parsed
	
    pipelineState.insertMode    = (!pipelineState.ddlComplete) ? 'DDL Error' : pipelineState.insertMode
	
	// console.log(pipelineState)
	
	const readElapsedTime = pipelineState[DBIConstants.PARSER_STREAM_ID].endTime - pipelineState[DBIConstants.INPUT_STREAM_ID].startTime;
    const writerElapsedTime = pipelineState[DBIConstants.OUTPUT_STREAM_ID].endTime - pipelineState[DBIConstants.TRANSFORMATION_STREAM_ID].startTime;        
    const pipeElapsedTime = pipelineState[DBIConstants.OUTPUT_STREAM_ID].endTime - pipelineState.startTime;
    const readThroughput = isNaN(readElapsedTime) ? 'N/A' : Math.round((pipelineState.read/readElapsedTime) * 1000)
    const writerThroughput = isNaN(writerElapsedTime) ? 'N/A' : Math.round((pipelineState.committed/writerElapsedTime) * 1000)
    
    let rowCountSummary = ''
    
    const readerTimings = `Reader Elapsed Time: ${YadamuLibrary.stringifyDuration(readElapsedTime)}s. Throughput ${Math.round(readThroughput)} rows/s.`
    const writerTimings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(writerElapsedTime)}s.  Idle Time: ${YadamuLibrary.stringifyDuration(pipelineState.idleTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(pipelineState.sqlTime))}s. Throughput: ${Math.round(writerThroughput)} rows/s.`

    // Any Pending Rows will not be processed. Pending will be zero if all batches have been written
    pipelineState.skipped += pipelineState.pending  
	pipelineState.pending = 0

    // Any Cached Rows will not be processed. Cached will be zero following AbortTable()
    pipelineState.skipped += pipelineState.cached   
	pipelineState.cached  = 0

    // Written rows that have not been committed will be lost. Written will be zero following a commit or rollback transaction or following AbortTable()
	pipelineState.lost    += pipelineState.written  
	pipelineState.written = 0

	if (err) {

      // console.log(pipelineState)
		  
	  // Account for any rows in-flight between the reader and the parter or the parser and the output manager. 
		
	  if (!isNaN(pipelineState[DBIConstants.INPUT_STREAM_ID].pending)) {
		// 'readable-stream' based Readble implementations (e.g. MySQL) may not maintain this value.
        pipelineState.read  += pipelineState[DBIConstants.INPUT_STREAM_ID].pending
	  }
	  	  	  
      pipelineState.skipped += (pipelineState.read - pipelineState.parsed) 
	  pipelineState.skipped += (pipelineState.parsed - pipelineState.received)
    }
    
	switch (true) {
	  case (!pipelineState.hasOwnProperty('ddlComplete')):
	    rowCountSummary = 'Operation Aborted. DDL Error.'
		break	  
	  case (pipelineState.tableNotFound === true) :
	    rowCountSummary = 'Operation Aborted. Table not found.'
		break
      case ((pipelineState.read === 0) && (pipelineState.readerState.LATEST_ERROR)):
        rowCountSummary = 'Operation Aborted. Read operation failed.'
	    break;
	  case ((pipelineState.read === pipelineState.committed) && (pipelineState.skipped === 0) && (pipelineState.lost === 0)):
        rowCountSummary = `Rows ${pipelineState.read}.`
	    break;
  	  default:
        rowCountSummary = `Read ${pipelineState.read}. Written ${pipelineState.committed}.`
        rowCountSummary = pipelineState.lost > 0 ? `${rowCountSummary} Lost ${pipelineState.lost}.` : rowCountSummary
        rowCountSummary = pipelineState.skipped > 0 ? `${rowCountSummary} Skipped ${pipelineState.skipped}.` : rowCountSummary
    }

    rowCountSummary = (this.yadamu.QA_TEST && pipelineState.receivedOoS > 0) ? `${rowCountSummary} [Out of Sequence ${pipelineState.receivedOoS}].` : rowCountSummary
   	
	if (cause) {
	  const tags = YadamuError.lostConnection(cause) ? ['LOST CONNECTION'] : []
      tags.push(pipelineState.readerState.LATEST_ERROR ? 'READABLE STREAM' : 'WRITABLE STREAM')
	  this.LOGGER.handleException(['PIPELINE',...tags,pipelineState.displayName,pipelineState[DBIConstants.INPUT_STREAM_ID].vendor,this.DATABASE_VENDOR,this.ON_ERROR,this.getWorkerNumber()],cause)
	  
	}
	
	if (pipelineState.failed) {
      this.LOGGER.error([`${pipelineState.displayName}`,`${pipelineState.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
    }
    else {
	  switch (true) {
		case (pipelineState.read == pipelineState.committed):
          this.LOGGER.info([`${pipelineState.displayName}`,`${pipelineState.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
		  break
	    case (pipelineState.read === (pipelineState.committed + pipelineState.skipped)):
          this.LOGGER.warning([`${pipelineState.displayName}`,`${pipelineState.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
		  break
		default:
          this.LOGGER.error([`${pipelineState.displayName}`,`${pipelineState.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
      }
    }     

    if (pipelineState.partitionedTableName) {
	  this.trackPartitionedTableState(pipelineState)
    }
    else {
      this.yadamu.recordMetrics(pipelineState.displayName,pipelineState)
	}
	
    return cause
  }
  
  getDriverState(userKey,description) {
	return {
      vendor             : this.DATABASE_VENDOR
	, version            : this.DATABASE_VERSION
   	, description        : description
 	, connection         : this.parameters.CONNECTION_NAME
    , schema             : this.parameters[userKey]
	, mode               : this.MODE
	, file               : this.FILE
	, ddlValid           : this.isValidDDL()
	}	 
  }
    
}

export { YadamuDBI as default}