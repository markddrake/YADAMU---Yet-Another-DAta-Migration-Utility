"use strict" 

const fs = require('fs');
const path = require('path');
const Readable = require('stream').Readable;
const util = require('util')
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const Yadamu = require('./yadamu.js');
const DBIConstants = require('./dbiConstants.js');
const YadamuConstants = require('./yadamuConstants.js');
const YadamuLibrary = require('./yadamuLibrary.js')
const {YadamuError, InternalError, CommandLineError, ConfigurationFileError, ConnectionError, DatabaseError, BatchInsertError, IterativeInsertError, InputStreamError} =  require('./yadamuException.js');
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

  get DATABASE_KEY()               { return 'yadamu' };
  get DATABASE_VENDOR()            { return 'YADAMU' };
  get SOFTWARE_VENDOR()            { return 'YABASC - Yet Another Bay Area Software Company'};
 
  get DATA_STAGING_SUPPORTED()     { return false } 
  get SQL_COPY_SUPPORTED()         { return false }
  
  get PASSWORD_KEY_NAME()          { return 'password' };
  get STATEMENT_TERMINATOR()       { return ';' }
  get STATEMENT_SEPERATOR()        { return '\n--\n' }

  get SPATIAL_FORMAT()             { return this.parameters.SPATIAL_FORMAT         || DBIConstants.SPATIAL_FORMAT };
  get TABLE_MAX_ERRORS()           { return this.parameters.TABLE_MAX_ERRORS       || DBIConstants.TABLE_MAX_ERRORS };
  get TOTAL_MAX_ERRORS()           { return this.parameters.TOTAL_MAX_ERRORS       || DBIConstants.TOTAL_MAX_ERRORS };
  get COMMIT_RATIO()               { return this.parameters.hasOwnProperty('COMMIT_RATIO') ?  this.parameters.COMMIT_RATIO : DBIConstants.COMMIT_RATIO };
  get MODE()                       { return this.parameters.MODE                   || DBIConstants.MODE }
  get ON_ERROR()                   { return this.parameters.ON_ERROR               || DBIConstants.ON_ERROR }
  get INFINITY_MANAGEMENT()        { return this.parameters.INFINITY_MANAGEMENT    || DBIConstants.INFINITY_MANAGEMENT };
  get LOCAL_STAGING_AREA()         { return YadamuLibrary.macroSubstitions((this.parameters.LOCAL_STAGING_AREA     || DBIConstants.LOCAL_STAGING_AREA || ''), this.yadamu.MACROS || '') }
  get REMOTE_STAGING_AREA()        { return YadamuLibrary.macroSubstitions((this.parameters.REMOTE_STAGING_AREA    || DBIConstants.REMOTE_STAGING_AREA || ''), this.yadamu.MACROS || '') }
  get STAGING_FILE_RETENTION()     { return this.parameters.STAGING_FILE_RETENTION || DBIConstants.STAGING_FILE_RETENTION }
  get TIMESTAMP_PRECISION()        { return this.parameters.TIMESTAMP_PRECISION    || DBIConstants.TIMESTAMP_PRECISION }
  get BYTE_TO_CHAR_RATIO()         { return this.parameters.BYTE_TO_CHAR_RATIO     || DBIConstants.BYTE_TO_CHAR_RATIO }
  get RETRY_COUNT()                { return 3 }

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
    this._COMMIT_COUNT = this._COMMIT_COUNT !== undefined ? this._COMMIT_COUNT : (() => {
      let commitCount = isNaN(this.COMMIT_RATIO) ? DBIConstants.COMMIT_RATIO : this.COMMIT_RATIO
      commitCount = Math.abs(Math.ceil(commitCount))
      commitCount = commitCount * this.BATCH_SIZE
      return commitCount
    })();
    return this._COMMIT_COUNT
  }
  
  // Override based on local parameters object ( which under the test harnesss may differ from the one obtained from yadamu in the constructor).
  
  get FILE()                          { return this.parameters.FILE     || this.yadamu.FILE }
  get PARALLEL()                      { return this.parameters.PARALLEL || this.yadamu.PARALLEL }
  
  get EXCEPTION_FOLDER()              { return this.parameters.FILE     || this.yadamu.EXCEPTION_FOLDER }
  get EXCEPTION_FILE_PREFIX()         { return this.parameters.FILE     || this.yadamu.EXCEPTION_FILE_PREFIX }
  get REJECTION_FOLDER()              { return this.parameters.FILE     || this.yadamu.REJECTION_FOLDER }
  get REJECTION_FILE_PREFIX()         { return this.parameters.FILE     || this.yadamu.REJECTION_FILE_PREFIX }
  get WARNING_FOLDER()                { return this.parameters.FILE     || this.yadamu.WARNING_FOLDER }
  get WARNING_FILE_PREFIX()           { return this.parameters.FILE     || this.yadamu.WARNING_FILE_PREFIX }

  get INPUT_METRICS()                 { return this._INPUT_METRICS }
  set INPUT_METRICS(v) {
	this._INPUT_METRICS =  Object.assign({},v);
  }
  
  get TRANSACTION_IN_PROGRESS()       { return this._TRANSACTION_IN_PROGRESS === true }
  set TRANSACTION_IN_PROGRESS(v)      { this._TRANSACTION_IN_PROGRESS = v }
  
  get RECONNECT_IN_PROGRESS()         { return this._RECONNECT_IN_PROGRESS === true }
  set RECONNECT_IN_PROGRESS(v)        { this._RECONNECT_IN_PROGRESS = v }
  
  get SAVE_POINT_SET()                { return this._SAVE_POINT_SET === true }
  set SAVE_POINT_SET(v)               { this._SAVE_POINT_SET = v }

  get ATTEMPT_RECONNECTION()          { return !this.RECONNECT_IN_PROGRESS }

  get SOURCE_DIRECTORY()              { return this.parameters.SOURCE_DIRECTORY || this.parameters.DIRECTORY }
  get TARGET_DIRECTORY()              { return this.parameters.TARGET_DIRECTORY || this.parameters.DIRECTORY }

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
    
  get TABLE_MATCHING()                { return this.parameters.TABLE_MATCHING }
  
  get DESCRIPTION()                   { return this._DESCRIPTION }
  set DESCRIPTION(v)                  { this._DESCRIPTION = v }

  constructor(yadamu,settings,parameters) {
    
    this.options = {
      recreateSchema : false
    }
    
    this._DB_VERSION = 'N/A'    
    this.yadamu = yadamu;
    
    this.sqlTraceTag = '';
    this.status = yadamu.STATUS
    this.yadamuLogger = yadamu.LOGGER;
	this.setConnectionProperties(settings || {})
	this.initializeParameters(parameters || {});
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
 
    this.tableName  = undefined;
    this.tableInfo  = undefined;
    this.insertMode = 'Empty';
    this.skipTable = true;

    this.sqlTraceTag = `/* Manager */`;	
    this.sqlCumlativeTime = 0
	this.firstError = undefined
	this.latestError = undefined    
    
	this.failedPrematureClose = false;
	
	// Used in testing
    this.killConfiguration = {}
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

  setOption(name,value) {
    this.options[name] = value;
  }
      
  initializeParameters(parameters){

	// Merge default parameters for this driver with parameters from configuration files and command line parameters.

    this.parameters = Object.assign({}, this.YADAMU_DBI_PARAMETERS, this.vendorParameters, parameters,this.yadamu.COMMAND_LINE_PARAMETERS);
    // console.log(this.YADAMU_DBI_PARAMETERS, this.vendorParameters, parameters,this.yadamu.COMMAND_LINE_PARAMETERS,this.parameters)									  									 
  }

  setParameters(parameters) {
	
	Object.assign(this.parameters, parameters || {})
	this._COMMIT_COUNT = undefined
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
	  this.yadamuLogger.error([this.DATABASE_VENDOR, status.operation, operation],`Server side operation resulted in ${summary.exceptions.length} errors.`)
  	  const err = new Error(`${this.DATABASE_VENDOR} ${operation} failed.`);
	  err.causes = summary.exceptions
      throw err
    }
	return summary;
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
    return (this.systemInformation.vendor === this.DATABASE_VENDOR)
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
   
  setMetrics(metrics) {
	// Share metrics with the Writer so adjustments can be made if the connection is lost.
    this.metrics = metrics
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
    switch (this.yadamu.IDENTIFIER_TRANSFORMATION) {
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
      case 'CUSTOM':
        throw new YadamuError([this.DATABSE_VENDOR],`IDENTIFIER_TRANSFORMATION="${this.yadamu.IDENTIFIER_TRANSFORMATION}": Unsupported Feature in YADAMU ${YadamuConstants.YADAMU_VERSION}.`)       
      default:
        throw new YadamuError([this.DATABSE_VENDOR],`Invalid IDENTIFIER_TRANSFORMATION specified (${this.yadamu.IDENTIFIER_TRANSFORMATION}). Valid Values are ${YadamuConstants.SUPPORTED_IDENTIFIER_TRANSFORMATION}.`)
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
	try {
      results = await Promise.all(ddl.map((ddlStatement) => {
        ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
		// this.status.sqlTrace.write(this.traceSQL(ddlStatement));
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
  
  async _getDatabaseConnection() {
    try {
      await this.createConnectionPool();
      this.connection = await this.getConnectionFromPool();
      await this.configureConnection();
    } catch (e) {
      const err = new ConnectionError(e,this.vendorProperties);
      throw err
    }

  }  

  waitForRestart(delayms) {
    return new Promise((resolve, reject) => {
        setTimeout(resolve, delayms);
    });
  }
    
  async _reconnect() {
    throw new Error(`Database Reconnection Not Implimented for ${this.DATABASE_VENDOR}`)
	
	// Default code for databases that support reconnection
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()

  }

  trackLostConnection() {
   
    /*
    **
    ** Invoked when the connection is lost. Assume a rollback took place. Any rows written but not committed are lost. 
    **
    */

  	if ((this.metrics !== undefined) && (this.metrics.lost  !== undefined) && (this.metrics.written  !== undefined)) {
      if (this.metrics.written > 0) {
        this.yadamuLogger.error([`RECONNECT`,this.DATABASE_VENDOR],`${this.metrics.written} uncommitted rows discarded when connection lost.`);
        this.metrics.lost += this.metrics.written;
	    this.metrics.written = 0;
      }
	}
  }	  
  
  async reconnect(cause,operation) {

    
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
	** Writers must invoke setMetrics() passing the writer's counter object to the database interface before consuming rows. 
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
          await this.waitForRestart(5000);
          retryCount++;
          continue;
        }
        else {
          // Reconnectiona attempt failed for some other reason. Throws the error
   	      this.RECONNECT_IN_PROGRESS = false;
          this.yadamuLogger.handleException([`RECONNECT`,this.DATABASE_VENDOR,operation],connectionFailure);
          throw cause;
        }
      }
      // Sucessfully reonnected. Throw the original error if rows were lost as a result of the lost connection
      this.RECONNECT_IN_PROGRESS = false;
      if ((this.metrics !== undefined) && (this.metrics.lost > 0)) { 
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
  
  async abort(e,options) {
	
	// this.yadamuLogger.trace([this.constructor.name,`abort(${poolOptions})`],'')

	// Log all errors other than lost connection errors. Do not throw otherwise underlying cause of the abort will be lost. 
				
	options = options === undefined ? {abort: true} : Object.assign(options,{abort:true})
    options.err = e
				
    try {
      await this.closeConnection(options);
	} catch (e) {
	  if (!YadamuError.lostConnection(e)) {
        this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,'ABORT','Connection'],e);
	  }
	}
	
    try {
	  // Force Termnination of All Current Connections.
	  await this.closePool(options);
	} catch (e) {
	  if (!YadamuError.lostConnection(e)) {
        this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,'ABORT','Pool'],e);
	  }
	}
	
	this.logDisconnect();
	
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
  
  getTypeMappings() {
	return {
      spatialFormat: this.SPATIAL_FORMAT
	}
  }
  
  getSystemInformation() {     
  
    return {
      date               : new Date().toISOString()
    , timeZoneOffset     : new Date().getTimezoneOffset()
    , typeMappings       : this.getTypeMappings()
	, tableFilter        : this.getTABLE_FILTER
    , schema             : this.parameters.FROM_USER ? this.parameters.FROM_USER : this.parameters.TO_USER
    , exportVersion      : YadamuConstants.YADAMU_VERSION
    , vendor             : this.DATABASE_VENDOR
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
	  	 
	  schemaInformation = this.TABLE_FILTER.map((tableName) => {
        return schemaInformation.filter((tableInformation) => {
		   return (tableInformation.TABLE_NAME === tableName)
		 })[0]
	  })
    }
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
  **
  The following methods are used by the YADAMU DBwriter class
  **
  */
  
  getSchemaIdentifer(key) {
	return this.parameters[key]
  }

  async initializeExport() {
	this.DESCRIPTION = this.getSchemaIdentifer('FROM_USER')
  }
  
  async finalizeExport() {
  }
  
  /*
  **
  ** The following methods are used by the YADAMU DBWriter class
  **
  */
  
  async initializeImport() {
	this.DESCRIPTION = this.getSchemaIdentifer('TO_USER')
  }
  
  async initializeData() {
  }
  
  async finalizeData() {
  }

  async finalizeImport() {
  }
    
  async generateStatementCache(StatementGenerator,schema) {
	const statementGenerator = new StatementGenerator(this,schema,this.metadata,this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache()
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

	tableInfo.MAPPED_TABLE_NAME = this.getMappedTableName(tableInfo.TABLE_NAME,this.identifierMappings) || tableInfo.TABLE_NAME
    return tableInfo
  }   

  createParser(tableInfo) {
    return new DefaultParser(tableInfo,this.yadamuLogger);      
  }
 
  inputStreamError(cause,sqlStatement) {
    return this.trackExceptions(new InputStreamError(cause,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(tableInfo) {
	throw new Error('Unimplemented Method')
	this.streamingStackTrace = new Error().stack;
	return inputStream;
  }      

  async getInputStreams(tableInfo) {
	const streams = []
	this.INPUT_METRICS = DBIConstants.NEW_TIMINGS
	this.INPUT_METRICS.DATABASE_VENDOR = this.DATABASE_VENDOR
	const inputStream = await this.getInputStream(tableInfo)
    inputStream.once('readable',() => {
	  this.INPUT_METRICS.readerStartTime = performance.now()
	}).on('error',(err) => { 
	  this.underlyingCause = YadamuError.prematureClose(err) ? null : this.inputStreamError(err,tableInfo.SQL_STATEMENT)
      this.INPUT_METRICS.readerEndTime = performance.now()
	  this.INPUT_METRICS.readerError = this.underlyingCause
	  this.INPUT_METRICS.failed = true;
	  this.failedPrematureClose = err.code === 'ERR_STREAM_PREMATURE_CLOSE'
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
      this.INPUT_METRICS.lost = parser.writableLength
	}).on('error',(err) => {
	  this.INPUT_METRICS.parserEndTime = performance.now()
	  this.INPUT_METRICS.rowsRead = parser.getRowCount()
      this.INPUT_METRICS.lost = parser.writableLength
	  this.INPUT_METRICS.parserError = YadamuError.prematureClose(err) ? null : err
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
    /*
    if (this.parameters.IDENTIFIER_MAPPING_FILE) {
      this.loadIdentifierMappings(this.parameters.IDENTIFIER_MAPPING_FILE);
    } 
    */    
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
	this.setIdentifierMappings(manager.getIdentifierMappings())

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
 
  async getConnectionID() {
	// ### Get a Unique ID for the connection
    throw new Error('Unimplemented Method')
  }

  configureTermination(configuration) {
    // Kill Configuration is added to the MasterDBI by YADAMU-QA
    this.killConfiguration = configuration
  }

  terminateConnection(idx) {
    return (!YadamuLibrary.isEmpty(this.killConfiguration) && ((this.isManager() && this.killConfiguration.worker === undefined) || (this.killConfiguration.worker === idx)))
  }
 
  /*
  **
  ** Copy-based Import Operations - Experimental
  **
  */
  
  getCredentials(key) {
    return ''
  }
	
  reportCopyOperationMode(copyEnabled,controlFilePath,contentType) {
    this.yadamuLogger.info([this.DATABASE_VENDOR,'COPY',`${contentType}`],`Processing ${controlFilePath}" using ${copyEnabled ? 'COPY' : 'PIPELINE' } mode.`)
	return copyEnabled
  } 
	
	
  validStagedDataSet(source,controlFilePath,controlFile) {   

    /*
	**
    ** Does the Target support copy operations from this source / control file ?
    **
	** Return true if, based on te contents of the control file, the data set can be consumed directly by the RDBMS using a COPY operation.
	** Return false if the data set cannot be consumed using a Copy operation
	** Do not throw errors if the data set cannot be used for a COPY operatio
	** Generate Info messages to explain why COPY cannot be used.
	**
  
    return true
	
	**
	*/
	
    throw new YadamuError(`Unsupported Feature for Driver "${this.DATABASE_KEY}".`)
	
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
  
  async generateCopyStatements(metadata) {
    const startTime = performance.now()
    await this.setMetadata(metadata)   
    const statementCache = await this.generateStatementCache(this.parameters.TO_USER)
	return statementCache
  }     
  
  async reportCopyErrors(tableName,stack,sqlStatement) {
  }
  
  async reportCopyResults(tableName,rowsRead,failed,elapsedTime,sqlStatement,stack) {
	const throughput = Math.round((rowsRead/elapsedTime) * 1000)
    const writerTimings = `Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s. Throughput: ${throughput} rows/s.`
   
    let rowCountSummary
    switch (failed) {
	  case 0:
	    rowCountSummary = `Rows ${rowsRead}.`
        this.yadamuLogger.info([`${tableName}`,`Copy`],`${rowCountSummary} ${writerTimings}`)  
        break
      default:
	    rowCountSummary = `Read ${rowsRead}. Written ${rowsRead - failed}.`
        this.yadamuLogger.error([`${tableName}`,`Copy`],`${rowCountSummary} ${writerTimings}`)  
        await this.reportCopyErrors(tableName,stack,sqlStatement)
	}
	
	const metrics = {
	  [tableName] : {
		rowCount    : rowsRead
	  , insertMode  : 'copy'
	  , elapsedTime : elapsedTime
	  , throughput  : `${throughput}/s`
	  }
    }
    
	this.yadamu.recordMetrics(metrics);  
  }
  
  async initializeCopy() {
  }
  
  async copyOperation(tableName,statement) {
	
    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
	
	let startTime 
	try {
	  startTime = performance.now();
	  let results = await this.beginTransaction()
	  results = await this.executeSQL(statement);
	  const elapsedTime = performance.now() - startTime;
	  results = await this.commitTransaction()
      const writerTimings = `Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.  rows/s.`
      this.yadamuLogger.info([tableName,'Copy'],`${writerTimings}`)  
	} catch(e) {
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'COPY',tableName],e)
	  let results = await this.rollbackTransaction()
	}
  }
  		  
  async finalizeCopy() {
  }

  async copyOperations(taskList,sourceVendor) {
	 
    this.activeWorkers = new Set()
    const taskCount = taskList.length
	
    const maxWorkerCount = parseInt(this.yadamu.PARALLEL)
    const workerCount = taskList.length < maxWorkerCount ? taskList.length : maxWorkerCount
  
    const workers = workerCount === 0 ? [this] : await Promise.all(new Array(workerCount).fill(0).map((x,idx) => { return this.workerDBI(idx) }))
	const concurrency = workerCount > 0 ? 'PARALLEL' : 'SERIAL'
	
	let operationAborted = false;
	let fatalError = undefined
    const copyOperations = workers.map((worker,idx) => { 
	  return new Promise(async (resolve,reject) => {
        // ### Await inside a Promise is an anti-pattern ???
        let result = undefined
        try {
     	  while (taskList.length > 0) {
	        const task = taskList.shift();
		    await worker.copyOperation(task.TABLE_NAME,task.copyStatement)
          }
		} catch (cause) {
		  result = cause
		  // this.yadamuLogger.trace(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,this.getWorkerNumber()],cause)
		  this.yadamuLogger.handleException(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,this.getWorkerNumber()],cause)
		  if ((this.ON_ERROR === 'ABORT') && !operationAborted) {
			fatalError = result
    	    operationAborted = true
			if (taskList.length > 0) {
		      this.yadamuLogger.error(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,worker.ON_ERROR],`Operation failed: Skipping ${taskList.length} Tables`);
			}
			else {
		      this.yadamuLogger.warning(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,worker.ON_ERROR],`Operation failed.`);
			}		
            taskList.length = 0;
          }
		}
        if (!worker.isManager()) {
		  await worker.releaseWorkerConnection()
	    }
		resolve(result) 
      })
    })
	  
    this.yadamuLogger.info(['PIPELINE','COPY',concurrency,workerCount,sourceVendor,this.DATABASE_VENDOR],`Processing ${taskCount} Tables`);
    const results = await Promise.allSettled(copyOperations)
	if (operationAborted) throw fatalError
    // this.yadamuLogger.trace(['PIPELINE','COPY',concurrency,workerCount,sourceVendor,this.DATABASE_VENDOR,taskList.length],`Processing Complete`);
	return results
  }  
  
  verifyStagingSource(validSources,source) {   
    if (!validSources.includes(source)) {
      throw new YadamuError(`COPY operations not supported between "${source}" and "${this.DATABASE_VENDOR}".`)
	}
  }
   
  async copyStagedData(vendor,controlFile,metadata,credentials) {

	this.DESCRIPTION = this.getSchemaIdentifer('TO_USER')
	this.setSystemInformation(controlFile.systemInformation)

    const startTime = performance.now()	
	const statementCache = await this.generateCopyStatements(metadata,credentials);
	const ddlStatements = this.analyzeStatementCache(statementCache,startTime)
	let results = await this.executeDDL(ddlStatements)
    if (results instanceof Error) {
	  this.yadamuLogger.ddl([this.DATABASE_VENDOR],`DDL Failure. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
	  return results
    }
	else {
  	  this.yadamuLogger.ddl([this.DATABASE_VENDOR],`Executed ${Array.isArray(results) ? results.length : undefined} DDL operations. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
	}

	if (this.MODE != 'DDL_ONLY') {
	  const taskList = Object.keys(statementCache).map((table) => {
	    return { 
  	      TABLE_NAME    : table
	    , copyStatement : statementCache[table].copy
        }
	  })
	  await this.initializeCopy()
      results = await this.copyOperations(taskList,vendor)
	  await this.finalizeCopy()
	  
	}
    return results
  }
  
}

module.exports = YadamuDBI
