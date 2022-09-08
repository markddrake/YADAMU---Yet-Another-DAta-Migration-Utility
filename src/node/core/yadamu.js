
import fs                     from 'fs';
import path                   from 'path';
import crypto                 from 'crypto';
import readline               from 'readline';
import assert                 from 'assert';

import {
  performance 
}                             from 'perf_hooks';

import {
  setTimeout 
}                             from "timers/promises"

import { 
  pipeline,
  finished 
}                             from 'stream/promises';

import YadamuConstants        from '../lib/yadamuConstants.js';
import YadamuLibrary          from '../lib/yadamuLibrary.js';

import FileDBI                from '../dbi/file/fileDBI.js';
import DBIConstants           from '../dbi/base/dbiConstants.js';
import YadamuDataTypes        from '../dbi/base/yadamuDataTypes.js';
import YadamuCopyManager      from '../dbi/base/yadamuCopyManager.js';
import NullWriter             from '../util/nullWriter.js';

import {
  FileNotFound, 
  FileError
}                             from '../dbi/file/fileException.js';

import DBReader               from './dbReader.js';
import DBWriter               from './dbWriter.js';
import DBReaderParallel       from './dbReaderParallel.js';
import DBReaderFile           from './dbReaderFile.js';
import YadamuLogger           from './yadamuLogger.js';
import YadamuRejectManager    from './yadamuRejectManager.js';

import {
  YadamuError, 
  UserError, 
  CommandLineError, 
  ConfigurationFileError, 
  DatabaseError, 
  ConnectionError
}                             from './yadamuException.js';


class Yadamu {

  static #_YADAMU_PARAMETERS
  static #_DBI_PARAMETERS

  static get YADAMU_VERSION()         { return YadamuConstants.YADAMU_VERSION }

  static get YADAMU_CONFIGURATION()   { return YadamuConstants.YADAMU_CONFIGURATION };    

  static get YADAMU_PARAMETERS()      { return YadamuConstants.YADAMU_PARAMETERS }  

  static get DBI_PARAMETERS()  { return DBIConstants.DBI_PARAMETERS }  

  get QA_TEST()                       { return false }

  get YADAMU_PARAMETERS()             { return Yadamu.YADAMU_PARAMETERS }

  get DBI_PARAMETERS()         { return Yadamu.DBI_PARAMETERS }
  
  get FILE()                          { return this.parameters.FILE                      || YadamuConstants.FILE }
  get CONFIG()                        { return this.parameters.CONFIG                    || YadamuConstants.CONFIG }
  get MODE()                          { return this.parameters.MODE                      || YadamuConstants.MODE }
  get ON_ERROR()                      { return this.parameters.ON_ERROR                  || YadamuConstants.ON_ERROR }
  get RDBMS()                         { return this.parameters.RDBMS                     || YadamuConstants.RDBMS }  
                                                                                        
  get EXCEPTION_FOLDER()              { return this.parameters.EXCEPTION_FOLDER          || YadamuConstants.EXCEPTION_FOLDER }
  get EXCEPTION_FILE_PREFIX()         { return this.parameters.EXCEPTION_FILE_PREFIX     || YadamuConstants.EXCEPTION_FILE_PREFIX }
  get REJECTION_FOLDER()              { return this.parameters.REJECTION_FOLDER          || YadamuConstants.REJECTION_FOLDER }
  get REJECTION_FILE_PREFIX()         { return this.parameters.REJECTION_FILE_PREFIX     || YadamuConstants.REJECTION_FILE_PREFIX }
  get WARNING_FOLDER()                { return this.parameters.WARNING_FOLDER            || YadamuConstants.WARNING_FOLDER }
  get WARNING_FILE_PREFIX()           { return this.parameters.WARNING_FILE_PREFIX       || YadamuConstants.WARNING_FILE_PREFIX }
 
  get IDENTIFIER_TRANSFORMATION()     { return this.parameters.IDENTIFIER_TRANSFORMATION || YadamuConstants.IDENTIFIER_TRANSFORMATION }
  get IDENTIFIER_MAPPING_FILE()       { return this.parameters.IDENTIFIER_MAPPING_FILE }
  get IDENTIFIER_MAPPINGS()           { 
    this._IDENTIFIER_MAPPINGS = this._IDENTIFIER_MAPPINGS || (() => {
      let identifierMappings = {}
      if (this.IDENTIFIER_MAPPING_FILE) {
        const mappingFile = path.resolve(this.IDENTIFIER_MAPPING_FILE)
        this.LOGGER.info(['IDENTIFIER MAPPING'],`Using identifier mappings file "${mappingFile}".`)
        identifierMappings = YadamuLibrary.loadJSON(mappingFile,this.LOGGER)
      }
      else {
        identifierMappings = {}
      }
      return identifierMappings
    })();
    return this._IDENTIFIER_MAPPINGS
  }
  
  get CIPHER_KEY_SIZE()               { return 32 }
  get CIPHER()                        { return this.parameters.CIPHER || YadamuConstants.CIPHER }
  get SALT()                          { return this.parameters.SALT || YadamuConstants.SALT }
  get ENCRYPTION()                    { return this.parameters.ENCRYPTION === undefined ? YadamuConstants.ENCRYPTION : this.parameters.ENCRYPTION }
  get ENCRYPTION_KEY()                { return this._ENCRYPTION_KEY }
  set ENCRYPTION_KEY(v)               { this._ENCRYPTION_KEY = v}
  
  get COMPRESSION()                   { return this.parameters.COMPRESSION || 'NONE' }
  get DATA_STAGING_ENABLED()          { return this.parameters.hasOwnProperty('DATA_STAGING_ENABLED') ? this.parameters.DATA_STAGING_ENABLED : true }
  
  get INTERACTIVE()                   { return this.STATUS.operation === 'YADAMUGUI' }
  
  get PARALLEL()                      { return this.parameters.PARALLEL === 0 ? 0 : (this.parameters.PARALLEL || YadamuConstants.PARALLEL) }
  get PARALLEL_ENABLED()              { return this._PARALLEL_ENABLED || false }
  set PARALLEL_ENABLED(v)             { this._PARALLEL_ENABLED = ((this.PARALLEL && (this.PARALLEL > 0)) && v )}

  get SOURCE_DIRECTORY()              { return this.parameters.SOURCE_DIRECTORY || this.parameters.DIRECTORY }
  get TARGET_DIRECTORY()              { return this.parameters.TARGET_DIRECTORY || this.parameters.DIRECTORY }

  get MACROS()                        { return YadamuConstants.MACROS }

  set OPERATION(value)                { this._OPERATION = value }
  get OPERATION()                     { return this._OPERATION }
  
  get YADAMU_QA()                     { return false }
  
  get LOG_FILE()                      { return this.parameters.LOG_FILE }
  get IDENTIFIER_MAPPING_FILE()       { return this.parameters.IDENTIFIER_MAPPING_FILE }

  get CONFIGURATION_FILE_PATH()       { return this.COMMAND_LINE_PARAMETERS.CONFIG }
  
  get COMMAND_LINE_PARAMETERS()       { 
    this._COMMAND_LINE_PARAMETERS = this._COMMAND_LINE_PARAMETERS || this.readCommandLineParameters();
	return this._COMMAND_LINE_PARAMETERS
  }
  
  get STATUS() {   
	this._STATUS = this._STATUS || {
      operation        : this.OPERATION
     ,errorRaised      : false
     ,warningRaised    : false
     ,statusMsg        : 'successfully'
     ,startTime        : performance.now()
    }
    return this._STATUS
  }
  
  get LOGGER() {
    this._LOGGER = this._LOGGER || (() => {
      const logger = this.LOG_FILE === undefined ? YadamuLogger.consoleLogger(this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX) : YadamuLogger.fileLogger(this.LOG_FILE,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      return logger
    })();
    return this._LOGGER
  }
  
  get REJECTION_MANAGER() {
    this._REJECTION_MANAGER = this._REJECTION_MANAGER || (() => {
      const rejectFile = path.resolve(`${this.REJECTION_FOLDER}${path.sep}${this.REJECTION_FILE_PREFIX}_${new Date().toISOString().replace(/:/g,'.')}.json`);
      return new YadamuRejectManager(this,'REJECTIONS',rejectFile);
    })();
    return this._REJECTION_MANAGER
  }
   
  get WARNING_MANAGER() {
    this._WARNING_MANAGER = this._WARNING_MANAGER || (() => {
      const rejectFile = path.resolve(`${this.WARNING_FOLDER}${path.sep}${this.WARNING_FILE_PREFIX}_${new Date().toISOString().replace(/:/g,'.')}.json`);
      return new YadamuRejectManager(this,'WARNINGS',rejectFile);
    })();
    return this._WARNING_MANAGER
  }
  
  constructor(operation,configParameters) {
	 
    this._OPERATION = operation
    this.activeConnections = new Set();
	this.mappedDataTypes = new Set();
	      
	if (process.listenerCount('unhandledRejection') === 0) { 
	  process.on('unhandledRejection', this.yadamuAbort.bind(this))
	  // process.on('unhandledRejection', this.yadamuAbort)
	}
	
    // Configure Paramters
    this.initializeParameters(configParameters || {})
    this.initializeLogging();    
	
	this.metrics = {}
	
	// Use an object to pass the prompt to ensure changes to prompt are picked up insde the writeToOutput function closure()

    this.cli = { 
	  "prompt" : null
	} 

    this.commandPrompt = readline.createInterface({input: process.stdin, output: process.stdout});
    this.commandPrompt._writeToOutput = (charsToWrite) => {
	  if (charsToWrite.startsWith(this.cli.prompt)) {
        this.commandPrompt.output.write(this.cli.prompt + '*'.repeat(charsToWrite.length-this.cli.prompt.length))
      } 
	  else {
	    this.commandPrompt.output.write(charsToWrite.length > 1 ? charsToWrite : "*")
      }
    };	

  }

  async reset(parameters) {
	
    this._IDENTIFIER_MAPPINGS = undefined
    this._REJECTION_MANAGER = undefined;
    this._WARNING_MANAGER = undefined;
	
    this.STATUS.startTime     = performance.now()
    this.STATUS.warningRaised = false;
    this.STATUS.errorRaised   = false;
    this.STATUS.statusMsg     = 'successfully'
	this.metrics = {}
    	
	this.initializeParameters(parameters)
	this.initializeLogging();
	
    if (parameters && (parameters.PASSPHRASE || (parameters.ENCRYPTION === true))) {		
	  await this.generateCryptoKey()
	}
  }
  
  clone() {
	
    this.REJECTION_MANAGER.close();
	this.WARNING_MANAGER.close();

	this.reset();
	this.LOGGER.resetMetrics()	
	return this;
  }
  
  yadamuAbort(err,promise) {
	 
    if (err.ignoreUnhandledRejection === true) {
	  // this.LOGGER.trace(['UNHANDLED REJECTION','YADAMU',this.STATUS.operation],'IGNORED'],err);
	  return;
	}

	this.LOGGER.error(['UNHANDLED REJECTION','YADAMU',this.STATUS.operation],err);
    this.LOGGER.handleException(['UNHANDLED REJECTION','YADAMU',this.STATUS.operation],err);
	this.STATUS.errorRaised = true;
    this.reportStatus(this.STATUS,this.LOGGER)
	this.close();
    if (!this.INTERACTIVE) {
  	  setTimeout(5000,null,{ref: false}).then(() => {
	    // this.LOGGER.trace(['UNHANDLED REJECTION','YADAMU',this.STATUS.operation],`Active Connections: ${this.activeConnections.size}`);
   	    this.LOGGER.error(['UNHANDLED REJECTION','YADAMU',this.STATUS.operation,'TIMEOUT'],'Closing connections and shutting down.')
  	    for (const conn of this.activeConnections) {
		  conn.destroy(err).then(() => {
		    this.LOGGER.info(['UNHANDLED REJECTION','YADAMU',this.STATUS.operation,conn.DATABASE_VENDOR],'Aborted Connection')
		  }).catch((e) => {
		    this.LOGGER.handleWarning(['UNHANDLED REJECTION','YADAMU',this.STATUS.operation,conn.DATABASE_VENDOR],e)
		  })
		}
        process.exit()
	  })
    }  
  }  
  
  reloadParameters(parameters) {
  
    this.initializeParameters(parameters || {})
    this.initializeLogging();    
  }
  
  appendSynonym(argument,value) {
	this._COMMAND_LINE_PARAMETERS[argument] = value
  }
  
  initializeDataTypes(DataTypes,dataTypeConfiguration) {
	  
	/*
	**
	** Avoid error: "Cannot set property CHAR_TYPE of class ... which has only a getter"
    ** by ensuring each Class object is only initialized once regardless of how many times it is used
	**
	*/
	
	if (!this.mappedDataTypes.has(DataTypes.name)) {
	  this.mappedDataTypes.add(DataTypes.name)     	 
      Object.assign(DataTypes,dataTypeConfiguration.mappings);
      Object.assign(DataTypes,dataTypeConfiguration.limits);
	  Object.assign(DataTypes.storageOptions,dataTypeConfiguration.storageOptions || {});	  
    }
	
  }	
  
 	  
  initializeParameters(configParameters) {

    // Start with Yadamu Defaults
    this.parameters = Object.assign({}, this.YADAMU_PARAMETERS);

    // Merge parameters from configuration files
    Object.assign(this.parameters, configParameters);

    // Merge parameters provided via command line arguments
    Object.assign(this.parameters,this.COMMAND_LINE_PARAMETERS)
   
  }
  
  initializeSQLTrace() {
	  
	const options = {
	  flags : (this.STATUS.sqlLogger && this.STATUS.sqlLogger.writableEnded) ? "a" : "w"
	}
	this.STATUS.sqlLogger = this.STATUS.sqlLogger || (this.parameters.SQL_TRACE ? fs.createWriteStream(this.parameters.SQL_TRACE,options) : NullWriter.NULL_WRITER )
  }
  
  initializeLogging() {

    // this.LOGGER = this.setYadamuLogger(this.parameters,this.STATUS);

    this.initializeSQLTrace() 
	
    if (this.parameters.LOG_FILE) {
      this.STATUS.logFileName = this.parameters.LOG_FILE;
    }

    if (this.parameters.DUMP_FILE) {
      this.STATUS.dumpFileName = this.parameters.DUMP_FILE
    }

    if (this.parameters.LOG_LEVEL) {
      this.STATUS.loglevel = this.parameters.LOG_LEVEL;
    }
    	
  }	  
  
  setDefaultParameter(parameters,defaultName,parameterName) {	 
    
     // Set default value is parameter is not defined and default is defined.	
 
	 if ((parameters.defaultName !== undefined) && (parameters.parameterName === undefined)) {
		paramteres[parameterName] = parameters[defaultName]
	 }
  } 

  async requestPassPhrase() {
	  
	 if (process.env.YADAMU_PASSPHRASE) {
	   this.LOGGER.info(['YADAMU'],'Passphrase used for Encryption and Decryption operations supplied using environemnt variable YADAMU_PASSPHRASE.')
	   return process.env.YADAMU_PASSPHRASE
	 }
	   
     const prompt = `Enter passphrase to be used when encrypting and decypting data files: `
     const pwQuery = this.createQuestion(prompt);
     return await pwQuery;
  
  }
  
  async generateCryptoKey() {

    let passphrase = this.parameters.PASSPHRASE || await this.requestPassPhrase()
  
    return await new Promise((resolve,reject) => {
	  crypto.scrypt(passphrase, this.SALT, this.CIPHER_KEY_SIZE, (err,key) => {
		if (err) reject(err);
		// console.log('Key',passphrase,this.SALT,this.CIPHER_KEY_SIZE,key)
	    passphrase = undefined
		this.ENCRYPTION_KEY = key
		resolve()
      })
	  this.parameters.PASSPHRASE = undefined
	})  
  }
  
  async initialize(parameters) {
	 if (!YadamuLibrary.isEmpty(parameters)) {
	   this.reloadParameters(parameters)
	 }
	 if (this.ENCRYPTION) {
	   await this.generateCryptoKey()
     }
  }
  
  recordPartitionMetrics(table,partitionMetrics) {
	if (this.metrics.hasOwnProperty(table)) {
	  const metrics = this.metrics[table]
      metrics.startTime = partitionMetrics.startTime < metrics.startTime ? partitionMetrics.startTime : metrics.startTime
	  metrics.endTime = partitionMetrics.endTime > metrics.endTime ? partitionMetrics.endTime : metrics.endTime
	  metrics.rowCount+= partitionMetrics.rowCount
	  metrics.rowsSkipped+= partitionMetrics.rowsSkipped
	  metrics.sqlExecutionTime+= partitionMetrics.sqlExecutionTime
      metrics.partitionCount--
	  if (metrics.partitionCount === 1) {
		delete metrics.partitionCount
        metrics.elapsedTime = metrics.endTime - metrics.startTime
		delete metrics.startTime
		delete metrics.endTime
		metrics.throughput = Math.round((metrics.rowCount/metrics.elapsedTime) * 1000)
	    const timings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(metrics.elapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(metrics.sqlExecutionTime))}s. Throughput: ${metrics.throughput} rows/s.`
	    this.LOGGER.info([`${table}`],`Total Rows ${metrics.rowCount}. ${timings}`)  
      }
	}
	else {
	 this.metrics[table] = partitionMetrics
	}
  }
  
  recordMetrics(tableName,metrics) {
	
	const elapsedTime =  Math.round(metrics.writerEndTime - metrics.pipeStartTime)
	
	const tableSummary = {
	  elapsedTime       : elapsedTime
	, throughput        : Math.round((metrics.committed/elapsedTime) * 1000)
    , rowCount          : metrics.committed
	, rowsSkipped       : metrics.skipped
	, sqlExecutionTime  : metrics.sqlTime
	}
	
	// console.log(metrics,tableSummary)

	Object.assign(this.metrics,{[tableName]:tableSummary})
	
	return tableSummary
  }
  
  reportStatus(status,yadamuLogger) {
     
    const endTime = performance.now();
      
	const metrics = yadamuLogger.getMetrics();
	status.statusMsg = status.warningRaised === true ? `with ${metrics.warnings} warnings` : status.statusMsg;
    status.statusMsg = status.errorRaised === true ? `with ${metrics.errors} errors and ${metrics.warnings} warnings`  : status.statusMsg;  
  
    const terminationArgs = [`YADAMU`,`${status.operation}`]
    const terminationMessage = `Operation completed ${status.statusMsg}. Elapsed time: ${YadamuLibrary.stringifyDuration(endTime - status.startTime)}.`

    switch (true) {
      case (status.errorRaised === true):
        yadamuLogger.error(terminationArgs,terminationMessage)
        break;
      case (status.warningRaised === true):
        yadamuLogger.warning(terminationArgs,terminationMessage)
        break
      default:
        yadamuLogger.info(terminationArgs,terminationMessage)
    }

    if (yadamuLogger.FILE_LOGGER) {
      console.log(`${new Date().toISOString()}[YADAMU][${status.operation}]: Operation completed ${status.statusMsg}. Elapsed time: ${YadamuLibrary.stringifyDuration(endTime - status.startTime)}. See "${status.logFileName}" for details.`);  
    }
  }
  
  reportError(e,parameters,status,yadamuLogger) {
    
	
    if (!(e instanceof ConnectionError)) {
	  yadamuLogger.handleException([`YADAMU`,`"${status.operation}"`],e);
	}
	
    if (yadamuLogger.FILE_LOGGER) {
      console.log(`${new Date().toISOString()} [ERROR][YADAMU][${status.operation}]: Operation failed: See "${parameters.LOG_FILE ? parameters.LOG_FILE  : 'above'}" for details.`);
    }
    else {
      console.log(`${new Date().toISOString()} [ERROR][YADAMU][${status.operation}]: Operation Failed:`);
      // console.dir(e,{depth:null});
	  YadamuLibrary.reportError(e)
    }
  }

  isSupportedValue(parameterName,parameterValue,validValues) {
	 const testValue = parameterValue.toUpperCase()
	 assert(validValues.includes(testValue),`Invalid value "${testValue}" specified for parameter "${parameterName}". Valid values are ${JSON.stringify(validValues)}.`)
	 return testValue;
  }

  isExistingFile(parameterName,parameterValue) {
  
    const resolvedPath = path.resolve(parameterValue);
	try {
	  if (!fs.statSync(resolvedPath).isFile()) {
	    const err = new CommandLineError(`Found Directory ["${resolvedPath}"]. The path specified for the ${parameterName} argument must not resolve to a directory.`)
	    throw err;
	  }
	}
    catch (e) {
	  if (e.code && e.code === 'ENOENT') {
        const err = new CommandLineError(`File not found ["${resolvedPath}"]. The path specified for the ${parameterName} argument must resolve to an existing file.`)
	    throw err
	  }
      throw e;
    } 
    return parameterValue
  }

  isExistingFolder(parameterName,parameterValue) {
  
    const resolvedPath = path.resolve(parameterValue);
	try {
	  if (fs.statSync(resolvedPath).isFile()) {
	    const err = new CommandLineError(`Found File ["${resolvedPath}"]. The path specified for the ${parameterValue} argument must resolve to a folder.`)
	    throw err;
	  }
	}
    catch (e) {
	  if (e.code && e.code === 'ENOENT') {
        const err = new CommandLineError(`Folder not found ["${resolvedPath}"]. The path specified for the ${parameterValue} argument must resolve to an existing folder.`)
	    throw err
	  }
      throw e;
    } 
    return parameterValue
  }

  processValue(parameterValue) {

    if ((parameterValue.startsWith('"') && parameterValue.endsWith('"')) && (parameterValue.indexOf('","') > 0 )) {
      // List of Values
	  let parameterValues = parameterValue.substring(1,parameterValue.length-1).split(',');
	  parameterValues = parameterValues.map((value) => {
        return YadamuLibrary.convertQutotedIdentifer(value);
	  })
	  return parameterValues
    }
    else {
      return YadamuLibrary.convertQuotedIdentifer(parameterValue);
    }
  }

  ensureNumeric(parameters,parameterName,parameterValue) {
     
     if (isNaN(parameterValue)) {
       console.log(`${new Date().toISOString()}[Yadamu]: Invalid Numeric value for parameter: "${parameterName}".`)         
     }
     else {
       parameters[parameterName] = parseInt(parameterValue);
     }

  }

  isTrue(value){
    if (typeof(value) === 'string'){
        value = value.trim().toLowerCase();
    }
    switch(value){
        case true:
        case "true":
        case 1:
        case "1":
        case "on":
        case "yes":
            return true;
        default: 
            return false;
    }
  }
  
  async finalize(status,yadamuLogger) {

    this.commandPrompt.close();
    await yadamuLogger.close();
    status.sqlLogger.end();
  }
  
  async close() {
    await this.finalize(this.STATUS,this.LOGGER);
  }
    
  createQuestion(prompt) {	
	this.cli.prompt = prompt;
    return new Promise((resolve,reject) => {
      this.commandPrompt.question(this.cli.prompt, (answer) => {
		resolve(answer);
	  })
	})
  }
  
  readCommandLineParameters() {
   
    const parameters = {}

    let allowAnyParameter = false

    process.argv.forEach((arg) => {
     
      if (arg.indexOf('=') > -1) {
        let parameterName = arg.substring(0,arg.indexOf('=')).toUpperCase();
        parameterName = parameterName.startsWith('--') ? parameterName.substring(2) : parameterName.startsWith('-') ? parameterName.substring(1) : parameterName
        const parameterValue = arg.substring(arg.indexOf('=')+1);
        switch (parameterName) {
		  case 'ALLOW_ANY_PARAMETER':
   	        allowAnyParameter = this.isSupportedValue(parameterName,parameterValue,YadamuConstants.TRUE_OR_FALSE) ? this.isTrue(parameterValue.toUpperCase()) : false
		    break;
	      case 'INIT':		  
	      case 'COPY':		  
	      case 'TEST':		  
   	        parameters.CONFIG = this.isExistingFile(parameterName,parameterValue);
			break;
          case 'IMPORT':
          case 'UPLOAD':		  
	      case 'EXPORT':
            parameters.FILE =  this.isExistingFile(parameterName,parameterValue);
            break;
          case 'RDBMS':
            parameters.RDBMS = parameterValue;
            break;
	      case 'OVERWRITE':		  
	        parameters.OVERWRITE = this.isSupportedValue(parameterName,parameterValue,YadamuConstants.TRUE_OR_FALSE) ? this.isTrue(parameterValue.toUpperCase()) : false
		    break;
          case 'CREDENTIALS':
            parameters.CREDENTIALS =  this.isExistingFile(parameterName,parameterValue);
          case 'CONFIG':
          case 'CONFIGURATION':
            parameters.CONFIG =  this.isExistingFile(parameterName,parameterValue);
            break;
	      case 'USERID':
  	        parameters.USERID = parameterValue;
			break;
          case 'USERNAME':
            parameters.USERNAME = parameterValue;
            break;
          case 'PASSWORD':
            console.log(`${new Date().toISOString()} [WARNING][${this.constructor.name}]: Suppling a password on the command line interface can be insecure`);
            parameters.PASSWORD = parameterValue;
            break;
          case 'DATABASE':
            parameters.DATABASE = parameterValue;
            break;
          case 'HOSTNAME':
            parameters.HOSTNAME = parameterValue;
            break;
          case 'ACCOUNT':
            parameters.ACCOUNT = parameterValue;
            break;
          case 'WAREHOUSE':
            parameters.WAREHOUSE = parameterValue;
            break;
          case 'HOSTNAME':
            parameters.HOSTNAME = parameterValue;
            break;
          case 'PORT':
            parameters.PORT = parameterValue;
            break;
		  case 'STAGING_PLATFORM':
            parameters.STAGING_PLATFORM = parameterValue;
            break;		    
          case 'FILE':
            if (parameters.IMPORT || parameters.EXPORT) {
			  throw new error(`Cannot combine legacy parameter FILE with IMPORT or EXPORT`);
			}
            parameters.FILE = parameterValue;
            break;
          case 'DIRECTORY':
            parameters.DIRECTORY = parameterValue;
            break;
		  case 'SOURCE':
          case 'SOURCE_DIR':
          case 'SOURCE_DIRECTORY':
            parameters.SOURCE_DIRECTORY = parameterValue;
            break;
		  case 'TARGET':
          case 'TARGET_DIR':
          case 'TARGET_DIRECTORY':
            parameters.TARGET_DIRECTORY = parameterValue;
            break;
          case 'LOCAL_STAGING_AREA':
            parameters.LOCAL_STAGING_AREA = parameterValue;
            break;
          case 'REMOTE_STAGING_AREA':
            parameters.REMOTE_STAGING_AREA = parameterValue;
            break;
          case 'BUCKET':
            parameters.BUCKET = parameterValue;
            break;
          case 'CONTAINER':
            parameters.CONTAINER = parameterValue;
            break;
          case 'OWNER':
            parameters.OWNER = this.processValue(parameterValue);
            break;
          case 'FROM_USER':
            parameters.FROM_USER = this.processValue(parameterValue);
            break;
          case 'TO_USER':
            parameters.TO_USER = this.processValue(parameterValue);
            break;
          case 'PARALLEL':
            parameters.PARALLEL = parameterValue;
            break;
          case 'LOG_FILE':
            parameters.LOG_FILE = parameterValue;
            break;
          case 'SQL_TRACE':
            parameters.SQL_TRACE = parameterValue;
            break;
          case 'PERF_TRACE':
          case 'PERFORMANCE_TRACE':
            parameters.PERFORMANCE_TRACE = parameterValue;
            break;
          case 'PARAMETER_TRACE':
            parameters.PARAMETER_TRACE = (parameterValue.toLowerCase() === 'true');
            break;
          case 'SPATIAL_FORMAT':
            parameters.SPATIAL_FORMAT = parameterValue.toUpperCase()
            break;
          case 'LOG_LEVEL':
            parameters.LOG_LEVEL = parameterValue;
            break;
          case 'DUMP_FILE':
            parameters.DUMP_FILE = parameterValue;
            break;
          case 'EXCEPTION_FOLDER':
            parameters.EXCEPTION_FOLDER = this.isExistingFolder(parameterName,parameterValue);
            break;
          case 'EXCEPTION_FILE_PREFIX':
            parameters.EXCEPTION_FILE_PREFIX = parameterValue;
            break;
          case 'REJECT_FOLDER':
            parameters.REJECT_FOLDER = this.isExistingFolder(parameterName,parameterValue);
            break;
          case 'REJECT_FILE_PREFIX':
            parameters.REJECT_FILE_PREFIX = parameterValue;
            break;
          case 'FEEDBACK':
            parameters.FEEDBACK = parameterValue.toUpperCase();
            break;
          case 'MODE':
            parameters.MODE = this.isSupportedValue(parameterName,parameterValue,YadamuConstants.MODES)
            break
          case 'BATCH_SIZE':
            this.ensureNumeric(parameters,parameterName.toUpperCase(),parameterValue)
            break;
          case 'COMMIT_RATIO':
            this.ensureNumeric(parameters,parameterName.toUpperCase(),parameterValue)
            break;
          case 'BATCH_LOB_COUNT':
            this.ensureNumeric(parameters,parameterName.toUpperCase(),parameterValue)
            break;
          case 'TABLES':
            if ((parameterValue.indexOf('[') === 0) && (parameterValue.indexOf(']') === (parameterValue.length -1))) {
			  // Assume we have a comma seperated list of tables - Convert to JSON Array
			  try {
				parameters.TABLES = JSON.parse(parameterValue) 
			  } catch (e) {
			    // Assume the " were not back quoted and were swallowed by OS command intepreter
				try {
				  parameters.TABLES = parameterValue.substring(1,parameterValue.length-1).split(',')
				}
				catch (e) {
				  throw new CommandLineError(`Parameter TABLES: Expected a comma seperated, case sensitive list of table names enclosed in Square Brackets  eg TABLE=[Table1,Table2] or a path to a file. Received "TABLES=${parameterValue}".`)
				}
			  }
		    }    
			else {
              parameters.TABLES = parameterValue
			}
            break;
          case 'IDENTIFIER_MAPPING_FILE':
            parameters.IDENTIFIER_MAPPING_FILE = isExistingFile(parameterName,parameterValue);
            break;
          case 'IDENTIFIER_TRANSFORMATION':
            parameters.IDENTIFIER_TRANSFORMATION = this.isSupportedValue(parameterName,parameterValue,YadamuConstants.SUPPORTED_IDENTIFIER_TRANSFORMATION);
            break;
          case 'OUTPUT_FORMAT':
            parameters.OUTPUT_FORMAT = this.isSupportedValue(parameterName,parameterValue,YadamuConstants.OUTPUT_FORMATS);
            break;
          case 'COMPRESSION':
            parameters.COMPRESSION = this.isSupportedValue(parameterName,parameterValue,YadamuConstants.SUPPORTED_COMPRESSION)
            break
          case 'ENCRYPTION':
            const encryption = YadamuConstants.TRUE_OR_FALSE.includes(parameterValue.toUpperCase()) ? parameterValue.toUpperCase() === 'TRUE' : this.isSupportedValue(parameterName,parameterValue,YadamuConstants.SUPPORTED_CIPHER) 
			if (typeof encryption === 'string') {
			   parameters.ENCRYPTION = true 
			   parameters.CIPHER = encryption
			}
			else {
			  parameters.ENCRYPTION = encryption
	    	}
			break
          case 'CIPHER':
            parameters.CIPHER = this.isSupportedValue(parameterName,parameterValue,YadamuConstants.SUPPORTED_CIPHER)
            break
          case 'PASSPHRASE':
            console.log(`${new Date().toISOString()} [WARNING][${this.constructor.name}]: Suppling a password on the command line interface can be insecure`);
            parameters.PASSPHRASE = parameterValue;
            break;
          case 'SALT':
            parameters.SALT = parameterValue;
            break;
          default:
		    if (allowAnyParameter) {
              try {
				parameters[parameterName] = JSON.parse(parameterValue.toLowerCase());
				if (typeof parameters[parameterName] === 'string') {
				  parameters[parameterName]	= parameterValue
				}
			  } catch (e) {
                parameters[parameterName] = parameterValue;
			  }
			  console.log(`${new Date().toISOString()}[WARNING][YADAMU][PARAMETERS]: Adding parameter: "${parameterName}" with value ${parameters[parameterName]} to parameter list.`) 
			}
			else {
              console.log(`${new Date().toISOString()}[WARNING][YADAMU][PARAMETERS]: Unknown parameter: "${parameterName}". See yadamu --help for supported command line switches and arguments` )          
		    }
        }
      }
    })

	return parameters;
  }
  
  closeFile(outputStream) {
	          
    return new Promise((resolve,reject) => {
      outputStream.on('finish',() => { resolve() });
      outputStream.close();
    })

  }

  async getDBReader(dbi,isDatabase) {
	const dbReader = isDatabase ? this.PARALLEL_ENABLED ? new DBReaderParallel(dbi, this.LOGGER) : new DBReader(dbi, this.LOGGER) : new DBReaderFile(dbi, this.LOGGER)
	await dbReader.initialize();
    return dbReader;
  }
  
  async getDBWriter(dbi) {
	const dbWriter = new DBWriter(dbi, this.LOGGER);
    await dbWriter.initialize();
    return dbWriter;
  }
    
  async doCopyOperation(source,target) {
	
    /*
	**
	** Load data the has been previously staged to a file system that is directly accessable by the target database.
	**
	*/
	
   	// Remap the data file locations in the control file to a path that is accessible by the target database
	
    const controlFile = source.controlFile
    this.LOGGER.info(['COPY',source.DATABASE_VENDOR,target.DATABASE_VENDOR],`Using Control File "${source.CONTROL_FILE_PATH}".`)
	      
	if (target.REMOTE_STAGING_AREA) {
	  Object.keys(controlFile.data).forEach((tableName,idx) => {
		if ((source.TABLE_FILTER.length === 0) || source.TABLE_FILTER.includes(tableName)) {
    	  source.DIRECTORY = source.TARGET_DIRECTORY
		  switch (Array.isArray(controlFile.data[tableName].files)) {
			case true:			
		       const remotePaths = controlFile.data[tableName].files.map((filename) => {return path.join(target.REMOTE_STAGING_AREA,path.basename(source.CONTROL_FILE_FOLDER),filename)})
	           controlFile.data[tableName].files = remotePaths
			  break
			case false:
		       const remotePath = path.join(target.REMOTE_STAGING_AREA,path.basename(source.CONTROL_FILE_FOLDER),controlFile.data[tableName].file)
	           controlFile.data[tableName].file = remotePath
			  break
	      }
	    }
		else {
		  delete controlFile.metadata[tableName]
		  delete controlFile.data[tableName]
		}
	  })
	}

	const metadata = await source.loadMetadataFiles(true)
	

    const copyManager = new YadamuCopyManager(target,source.getCredentials(target.DATABASE_KEY),this.LOGGER);
	try {
	  const results = await copyManager.copyStagedData(source.DATABASE_KEY,controlFile,metadata)
      this.reportStatus(this.STATUS,this.LOGGER)
	  await source.final()
	  await target.final()
	  return results;
    } catch (e) {
	  await source.destroy(e)
	  await target.destroy(e)
	  throw (e)
	}
  }

  async doPipelineOperation(source,target) {
	  
    // this.LOGGER.trace([this.constructor.name,`PIPELINE`,source.DATABASE_VENDOR,target.DATABASE_VENDOR,process.arch,process.platform,process.version],'Pipeline')

    /*
	**
	** Copy data using a Node Pipeline.
	**
	*/

    /*
	**
	** Enabled Parallel Processing if parallel operations are support by the soure and target
	**
	*/ 
	
	this.PARALLEL_ENABLED = source.PARALLEL_OPERATIONS && target.PARALLEL_OPERATIONS
	
    const dbReader = await this.getDBReader(source,target.isDatabase())
    const dbWriter = await this.getDBWriter(target) 

    const yadamuPipeline = []
    const activeStreams = []
    try {
      // dbReader.getInputStream() returns itself (this) for databases...	  
	  yadamuPipeline.push(...dbReader.getInputStreams())
	  yadamuPipeline.push(dbWriter)

      
      // The components that make up the pipeline may not have finished _final and destroy when the pipeline completes. Need to wait for all components to Finish before closing connections
	  // activeStreams.push(...yadamuPipeline.map((s) => { return finished(s) }))

      // this.LOGGER.trace([this.constructor.name,'PIPELINE'],`${yadamuPipeline.map((s) => { return `${s.constructor.name}`}).join(' => ')}`)
 	  // this.LOGGER.trace([this.constructor.name,`PIPELINE`,dbReader.dbi.DATABASE_VENDOR,dbWriter.dbi.DATABASE_VENDOR,process.arch,process.platform,process.version],'Starting Pipeline')
	  
	  await pipeline(...yadamuPipeline)
      // await Promise.allSettled(activeStreams)
	  this.STATUS.operationSuccessful = true;

      // this.LOGGER.trace([this.constructor.name,'PIPELINE'],'Success')
      this.reportStatus(this.STATUS,this.LOGGER)
    } catch (e) {
	  this.LOGGER.handleException(['YADAMU','PIPELINE'],e)
	  // If the pipeline operation throws 'ERR_STREAM_PREMATURE_CLOSE' get the underlying cause from the dbReader;
	  if (e.code === 'ERR_STREAM_PREMATURE_CLOSE') {
	    e = dbReader.underlyingError instanceof Error ? dbReader.underlyingError : (dbWriter.underlyingError instanceof Error ? dbWriter.underlyingError : e)
	  }
  	  // this.LOGGER.trace([this.constructor.name,'PIPELINE','FAILED'],e)
      // this.LOGGER.trace([this.constructor.name,'doPipelineOperation()'],`Waiting for Streams to finish. [${yadamuPipeline.map((s) => { return `${s.constructor.name}`}).join(' => ')}]`);
	  // await Promise.allSettled(activeStreams)
	  // this.LOGGER.trace([this.constructor.name,'doPipelineOperation()'],`Streams Finished. [${yadamuPipeline.map((s) => { return `${s.constructor.name}`}).join(' => ')}]`);
	  
	  throw e;
	}
  }
  
  async doPumpOperation(source,target) {
	     
	let results;
      
	try {
	  let failed = false;
	  let cause = undefined;

      await source.initialize();
      await target.initialize();
		
      if (this.DATA_STAGING_ENABLED && source.DATA_STAGING_SUPPORTED && target.SQL_COPY_OPERATIONS) {
		await source.loadControlFile()
		if (target.validStagedDataSet(source.DATABASE_KEY,source.CONTROL_FILE_PATH,source.controlFile)) {
		  // TODO: If all copy operations fail due to issues accessing file fallback to Pipeline.
          await this.doCopyOperation(source,target)
		}
		else {
		  await this.doPipelineOperation(source,target)
	    }
	  }	
	  else {
	    await this.doPipelineOperation(source,target)
      }		  
	 
	  results = this.metrics
	  
	} catch (e) {		
  	  if (!source.DESTROYED) {
		await source.destroy(e)
	  }
  	  if (!target.DESTROYED) {
	    await target.destroy(e)
	  }
	  this.STATUS.operationSuccessful = false;
	  this.STATUS.err = e;
      results = e;
	  if (!((e instanceof UserError) && (e instanceof FileNotFound))) {
        this.reportError(e,this.parameters,this.STATUS,this.LOGGER);
	  }
    } finally { 
      await this.REJECTION_MANAGER.close();
      await this.WARNING_MANAGER.close();
	}

    return results;
  }
    
  async pumpData(source,target) {
     
    if ((source.isDatabase() === true) && (source.parameters.FROM_USER === undefined)) {
      throw new Error('Missing mandatory parameter FROM_USER');
    }

    if ((target.isDatabase() === true) && (target.parameters.TO_USER === undefined)) {
      throw new Error('Missing mandatory parameter TO_USER');
    }
	
    return await this.doPumpOperation(source,target)    
  }
  
  async convertFile(fileDBI,encrypt) {
	const options = {
	  encryptedInput   : !encrypt
	, compressedInput  : false
	, encryptedOutput  : encrypt
	, compressedOutput : false
	, filename         : `${this.FILE}.${encrypt ? 'secure' : 'plain'}`
    }
	
	let streamsCompleted
    try {
	  const pipelineComponents = await fileDBI.createCloneStream(options)
	  await pipeline(...pipelineComponents)
    } catch (e) {
	  this.LOGGER.handleException(['YADAMU','PIPELINE'],e)
 	  await Promise.allSettled(streamsCompleted)
      throw e;
    }
  }
  
  async doImport(dbi) {
    const fileReader = new FileDBI(this,null,{},{FROM_USER: 'YADAMU'})
    const metrics = await this.pumpData(fileReader,dbi);
    await this.close();
    return metrics
  }  
 
  async doExport(dbi) {
    const fileWriter = new FileDBI(this,null,{},{TO_USER: 'YADAMU'})
    const metrics = await this.pumpData(dbi,fileWriter);
    await this.close();
    return metrics
  }  
  
  async doEncrypt() {
    const fileDBI = new FileDBI(this,null,{},{FROM_USER: 'YADAMU'})
    await this.convertFile(fileDBI,true);
    await this.close();
    return
  }  

  async doDecrypt() {
    const fileDBI = new FileDBI(this,null,{},{TO_USER: 'YADAMU'})
    await this.convertFile(fileDBI,false);
    await this.close();
    return
  }  

  async doCopy(source,target) {
    const metrics = await this.pumpData(source,target);
    await this.close();
    return metrics
  }  
  
  async cloneFile(pathToFile) {

    const fileReader = new FileDBI(this,null,{},{FROM_USER: 'YADAMU'})
    const fileWriter = new FileDBI(this,null,{},{TO_USER: 'YADAMU'})
    const metrics = await this.pumpData(fileReader,fileWriter);
    await this.close();
    return metrics
  }
   
  getMetrics(log) {
      
     const metrics = {}
     log.forEach((entry) => {
       switch (Object.keys(entry)[0]) {
         case 'dml' :
           metrics[entry.dml.tableName] = {rowCount : entry.dml.rowCount, rowsSkipped: 0, insertMode : "SQL", elapsedTime : entry.dml.elapsedTime + "ms", throughput: Math.round((entry.dml.rowCount/Math.round(entry.dml.elapsedTime)) * 1000).toString() + "/s"}
           break;
         case 'error' :
           metrics[entry.error.tableName] = {rowCount : -1, rowsSkipped: 0, insertMode : "SQL", elapsedTime : "NaN", throughput: "NaN"}
           break;
         default:
       }
     })
     return metrics
  }
  
  async doUploadOperation(dbi) {

    try {
      await dbi.initialize();
      this.STATUS.operationSuccessful = false;
	  const log = await dbi.upload()
      await dbi.final();
	  const metrics = this.getMetrics(log);  
	  this.STATUS.operationSuccessful = true;
      return metrics
    } catch (e) {
	  this.STATUS.operationSuccessful = false;
	  this.STATUS.err = e;
      await dbi.destroy(e)
    }
    return {};
  }

  async uploadData(dbi) {
	  
    if ((dbi.isDatabase() === true) && (dbi.parameters.TO_USER === undefined)) {
      throw new Error('Missing mandatory parameter TO_USER');
    }

    if (dbi.parameters.FILE === undefined) {
      throw new Error('Missing mandatory parameter FILE');
    }
   
    let metrics = await this.doUploadOperation(dbi)
	
	switch (this.STATUS.operationSuccessful) {
      case true:
        this.reportStatus(this.STATUS,this.LOGGER)
		break;
	  case false:
        metrics = this.STATUS.err
        this.reportError(this.STATUS.err,this.parameters,this.STATUS,this.LOGGER);
		break;
	  default:
	}
	
    return metrics;
  } 
  
  async doUpload(dbi) {
    const metrics = await this.uploadData(dbi);
    await this.close();
    return metrics
  }  
  
  reportIncorrectMessageSequence() {
  }
  
}  
     
export { Yadamu as default}
