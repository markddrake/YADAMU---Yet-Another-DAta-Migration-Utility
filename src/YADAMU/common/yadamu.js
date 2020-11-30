"use strict"

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { performance } = require('perf_hooks');
const assert = require('assert');

const util = require('util')
const stream = require('stream')
const pipeline = util.promisify(stream.pipeline);

const FileDBI = require('../file/node/fileDBI.js');
const DBReader = require('./dbReader.js');
const DBWriter = require('./dbWriter.js');
const DBReaderParallel = require('./dbReaderParallel.js');

const YadamuConstants = require('./yadamuConstants.js');
const NullWriter = require('./nullWriter.js');
const YadamuLogger = require('./yadamuLogger.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const {YadamuError, UserError, CommandLineError, ConfigurationFileError, DatabaseError} = require('./yadamuError.js');
const {FileNotFound, FileError} = require('../file/node/fileError.js');
const YadamuRejectManager = require('./yadamuRejectManager.js');

class Yadamu {

  static get YADAMU_VERSION()         { return YadamuConstants.YADAMU_VERSION }
  
  get FILE()                          { return this.parameters.FILE     || YadamuConstants.FILE }
  get CONFIG()                        { return this.parameters.CONFIG   || YadamuConstants.CONFIG }
  get MODE()                          { return this.parameters.MODE     || YadamuConstants.MODE }
  get ON_ERROR()                      { return this.parameters.ON_ERROR || YadamuConstants.ON_ERROR }
  get PARALLEL()                      { return this.parameters.PARALLEL === 0 ? 0 : (this.parameters.PARALLEL || YadamuConstants.PARALLEL) }
  get PARALLEL_PROCESSING()           { return this.PARALLEL > 0 } // Parellel 1 is Parallel processing logic with a single worker.
  get RDBMS()                         { return this.parameters.RDBMS    || YadamuConstants.RDBMS }  
  get COMPRESSION()                   { return this.parameters.COMPRESSION || 'NONE' }

  get EXCEPTION_FOLDER()              { return this.parameters.EXCEPTION_FOLDER       || YadamuConstants.EXCEPTION_FOLDER }
  get EXCEPTION_FILE_PREFIX()         { return this.parameters.EXCEPTION_FILE_PREFIX  || YadamuConstants.EXCEPTION_FILE_PREFIX }
  get REJECTION_FOLDER()              { return this.parameters.REJECTION_FOLDER       || YadamuConstants.REJECTION_FOLDER }
  get REJECTION_FILE_PREFIX()         { return this.parameters.REJECTION_FILE_PREFIX  || YadamuConstants.REJECTION_FILE_PREFIX }
  get WARNING_FOLDER()                { return this.parameters.WARNING_FOLDER         || YadamuConstants.WARNING_FOLDER }
  get WARNING_FILE_PREFIX()           { return this.parameters.WARNING_FILE_PREFIX    || YadamuConstants.WARNING_FILE_PREFIX }

  get MACROS()                        { return YadamuConstants.MACROS }

  set OPERATION(value)                { this._OPERATION = value }
  get OPERATION()                     { return this._OPERATION }
  
  get LOG_FILE()                      { return this.parameters.LOG_FILE }

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

  constructor(operation,parameters) {
	  
    this._OPERATION = operation
    
	if (process.listenerCount('unhandledRejection') === 0) {
	  process.on('unhandledRejection', (err, p) => {
		
  	    if (err.ignoreUnhandledRejection === true) {
	       // this.LOGGER.trace(['YADAMU',this.STATUS.operation,'UHANDLED REJECTION','IGNORED'],err);
		   return;
	    }
	  
	    this.LOGGER.handleException(['YADAMU',this.STATUS.operation,'UHANDLED REJECTION'],err);
        this.LOGGER.error(['YADAMU',this.STATUS.operation,'UHANDLED REJECTION'],err.message);
        console.log(err)
        this.STATUS.errorRaised = true;
        this.reportStatus(this.STATUS,this.LOGGER)
        process.exit()
      })
	}

    // Read Command Line Parameters
    this.loadParameters(parameters)
    this.processParameters();    
	
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
  
  recordMetrics(metrics) {
	Object.assign(this.metrics,metrics)
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
    
    if (yadamuLogger.FILE_LOGGER) {
      yadamuLogger.handleException([`${this.constructor.name}`,`"${status.operation}"`],e);
      console.log(`${new Date().toISOString()} [ERROR][YADAMU][${status.operation}]: Operation failed: See "${parameters.LOG_FILE ? parameters.LOG_FILE  : 'above'}" for details.`);
    }
    else {
      console.log(`${new Date().toISOString()} [ERROR][YADAMU][${status.operation}]: Operation Failed:`);
      console.dir(e,{depth:null});
    }
  }

  isSupportedValue(parameterName,parameterValue,validValues) {
	 const testValue = parameterValue.toUpperCase()
	 assert(validValues.includes(testValue),`Invalid value "${testValue}" specified for parameter "${parameterName}". Valid values are ${JSON.stringify(validValues)}.`)
	 return testValue;
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
    status.sqlTrace.end();
  }
  
  async close() {
    await this.finalize(this.STATUS,this.LOGGER);
  }
  
  reloadParameters(parameters) {
	 
	this.loadParameters(parameters)
    this.processParameters();    
  }
  
  loadParameters(suppliedParameters) {

    // Start with Yadamu Defaults
    this.parameters = Object.assign({}, YadamuConstants.YADAMU_DEFAULTS.yadamu);

    // Merge parameters read from configuration files
    Object.assign(this.parameters, suppliedParameters ? suppliedParameters : {});

    // Merge parameters provided via command line arguments
    Object.assign(this.parameters,this.COMMAND_LINE_PARAMETERS)

  }

  initializeSQLTrace() {
	const options = {
	  flags : (this.STATUS.sqlTrace && this.STATUS.sqlTrace.writableEnded) ? "a" : "w"
	}
	this.STATUS.sqlTrace = this.STATUS.sqlTrace || (this.parameters.SQL_TRACE ? fs.createWriteStream(this.parameters.SQL_TRACE,options) : NullWriter.NULL_WRITER )
  }
  
  processParameters() {

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
  
  createQuestion(prompt) {	
	this.cli.prompt = prompt;
    return new Promise((resolve,reject) => {
      this.commandPrompt.question(this.cli.prompt, (answer) => {
		resolve(answer);
	  })
	})
  }
  
  cloneDefaultParameters() {
     const parameters = Object.assign({},YadamuConstants.EXTERNAL_DEFAULTS.yadamu)
     Object.assign(parameters, YadamuConstants.EXTERNAL_DEFAULTS.yadamuDBI)
     return parameters
  }

  readCommandLineParameters() {
   
    const parameters = {}

    const allowAnyParameter = process.argv.some((currentValue) => {return ((typeof  currentValue === 'string') && (currentValue.toUpperCase() === 'ALLOW_ANY_PARAMETER=TRUE'))})

    process.argv.forEach((arg) => {
     
      if (arg.indexOf('=') > -1) {
        const parameterName = arg.substring(0,arg.indexOf('=')).toUpperCase();
        const parameterValue = arg.substring(arg.indexOf('=')+1);
        switch (parameterName) {
	      case 'INIT':		  
	      case '--INIT':
	      case 'COPY':		  
	      case '--COPY':
	      case 'TEST':		  
	      case '--TEST':
			parameters.CONFIG = parameterValue;
			break;
          case 'IMPORT':
          case '--IMPORT':
          case 'UPLOAD':		  
	      case '--UPLOAD':
  	      case 'EXPORT':
          case '--EXPORT':
			parameters.FILE = parameterValue;
            break;
          case 'RDBMS':
          case '--RDBMS':
            parameters.RDBMS = parameterValue;
            break;
	      case 'OVERWRITE':		  
	      case '--OVERWRITE':
  	        parameters.OVERWRITE = this.isTrue(parameterValue.toUpperCase());
		    break;
          case 'CONFIG':
          case '--CONFIG':
          case 'CONFIGURATION':
          case '--CONFIGURATION':
            parameters.CONFIG = parameterValue;
			parameters.FILE = parameterValue;
            break;
	      case 'USERID':
  	        parameters.USERID = parameterValue;
			break;
          case 'USERNAME':
          case '--USERNAME':
            parameters.USERNAME = parameterValue;
            break;
          case 'PASSWORD':
          case '--PASSWORD':
		    console.log(`${new Date().toISOString()} [WARNING][${this.constructor.name}]: Suppling a password on the command line interface can be insecure`);
            parameters.PASSWORD = parameterValue;
            break;
          case 'DATABASE':
          case '--DATABASE':
            parameters.DATABASE = parameterValue;
            break;
          case 'HOSTNAME':
          case '--HOSTNAME':
            parameters.HOSTNAME = parameterValue;
            break;
          case 'ACCOUNT':
          case '--ACCOUNT':
            parameters.ACCOUNT = parameterValue;
            break;
          case 'WAREHOUSE':
          case '--WAREHOUSE':
            parameters.WAREHOUSE = parameterValue;
            break;
          case 'HOSTNAME':
          case '--HOSTNAME':
            parameters.HOSTNAME = parameterValue;
            break;
          case 'PORT':
          case '--PORT':
            parameters.PORT = parameterValue;
            break;
          case 'FILE':
          case '--FILE':
		    if (parameters.IMPORT || parameters.EXPORT) {
			  throw new error(`Cannot combine legacy parameter FILE with IMPORT or EXPORT`);
			}
            parameters.FILE = parameterValue;
            break;
          case 'ROOT_FOLDER':
          case '--ROOT_FOLDER':
            parameters.FILE = parameterValue;
            break;
          case 'BUCKET':
          case '--BUCKET':
            parameters.BUCKET = parameterValue;
            break;
          case 'CONTAINER':
          case '--CONTAINER':
            parameters.CONTAINER = parameterValue;
            break;
          case 'OWNER':
          case '--OWNER':
          case 'FROM_USER':
          case '--FROM_USER':
            parameters.FROM_USER = this.processValue(parameterValue);
            break;
          case 'TOUSER':
          case '--TOUSER':
          case 'TO_USER':
          case '--TO_USER':
            parameters.TO_USER = this.processValue(parameterValue);
            break;
          case 'PARALLEL':
          case '--PARALLEL':
            parameters.PARALLEL = parameterValue;
            break;
          case 'LOG_FILE':
          case '--LOG_FILE':
            parameters.LOG_FILE = parameterValue;
            break;
          case 'SQL_TRACE':
          case '--SQL_TRACE':
            parameters.SQL_TRACE = parameterValue;
            break;
          case 'PERF_TRACE':
          case '--PERF_TRACE':
		  case 'PERFORMANCE_TRACE':
          case '--PERFORMANCE_TRACE':
            parameters.PERFORMANCE_TRACE = parameterValue;
            break;
                    case '--SQL_TRACE':
            parameters.SQL_TRACE = parameterValue;
            break;
          case 'PARAMETER_TRACE':
          case '--PARAMETER_TRACE':
            parameters.PARAMETER_TRACE = (parameterValue.toLowerCase() === 'true');
            break;
          case 'SPATIAL_FORMAT':
          case '--SPATIAL_FORMAT':
            parameters.SPATIAL_FORMAT = parameterValue.toUpperCase()
            break;
          case 'LOG_LEVEL':
          case '--LOG_LEVEL':
            parameters.LOG_LEVEL = parameterValue;
            break;
          case 'DUMP_FILE':
          case '--DUMP_FILE':
            parameters.DUMP_FILE = parameterValue;
            break;
          case 'EXCEPTION_FOLDER':
          case '--EXCEPTION_FOLDER':
            parameters.EXCEPTION_FOLDER = parameterValue;
            break;
          case 'EXCEPTION_FILE_PREFIX':
          case '--EXCEPTION_FILE_PREFIX':
            parameters.EXCEPTION_FILE_PREFIX = parameterValue;
            break;
          case 'REJECT_FOLDER':
          case '--REJECT_FOLDER':
            parameters.REJECT_FOLDER = parameterValue;
            break;
          case 'REJECT_FILE_PREFIX':
          case '--REJECT_FILE_PREFIX':
            parameters.REJECT_FILE_PREFIX = parameterValue;
            break;
          case 'FEEDBACK':
          case '--FEEDBACK':
            parameters.FEEDBACK = parameterValue.toUpperCase();
            break;
          case 'MODE':
          case '--MODE':
            parameters.MODE = parameterValue.toUpperCase();
            break
          case 'BATCH_SIZE':
          case '--BATCH_SIZE':
            this.ensureNumeric(parameters,parameterName.toUpperCase(),parameterValue)
            break;
          case 'COMMIT_RATIO':
          case '--COMMIT_RATIO':
            this.ensureNumeric(parameters,parameterName.toUpperCase(),parameterValue)
            break;
          case 'BATCH_LOB_COUNT':
          case '--BATCH_LOB_COUNT':
            this.ensureNumeric(parameters,parameterName.toUpperCase(),parameterValue)
            break;
          case 'TABLES':
          case '--TABLES':
            if (typeof parameterValue === "string") {
              parameters.TABLES = parameterValue.split(',')
            }
            else {
              throw new CommandLineError(`Parameter TABLES: Expected a comma seperated list of table names. Received "TABLES=${parameterValue}".`)
            }
            break;
          case 'OUTPUT_FORMAT':
          case '--OUTPUT_FORMAT':
            parameters.OUTPUT_FORMAT = this.isSupportedValue('OUTPUT_FORMAT',parameterValue,YadamuConstants.SUPPORTED_OUTPUT_FORMAT);
            break;
          case 'COMPRESSION':
          case '--COMPRESSION':
            parameters.COMPRESSION = this.isSupportedValue('COMPRESSION',parameterValue,YadamuConstants.SUPPORTED_COMPRESSION)
            break
          case 'ENCRYPT':
          case '--ENCRYPT':
            parameters.ENCRYPT = null;
            break
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

  async getDBReader(dbi,parallel) {
	const dbReader = parallel ? new DBReaderParallel(dbi, this.LOGGER) : new DBReader(dbi, this.LOGGER);
	await dbReader.initialize();
    return dbReader;
  }
  
  async getDBWriter(dbi,parallel) {
	const dbWriter = new DBWriter(dbi, this.LOGGER);
    await dbWriter.initialize();
    return dbWriter;
  }
  
  async doPumpOperation(source,target) {
	     
	let results;
    let dbReader
	let dbWriter

	const parallel = (this.PARALLEL_PROCESSING && source.isDatabase() && target.isDatabase());
    
	try {
	  let failed = false;
	  let cause = undefined;

      await source.initialize();
      await target.initialize();
		
      this.STATUS.operationSuccessful = false;
      try {
        dbReader = await this.getDBReader(source,parallel)
        dbWriter = await this.getDBWriter(target,parallel) 

        const yadamuPipeline = []
	    // dbReader.getInputStream() returns itself (this) for databases...	  
	    yadamuPipeline.push(...dbReader.getInputStreams())
	    yadamuPipeline.push(dbWriter)

	    // this.LOGGER.trace([this.constructor.name,'PIPELINE'],`${yadamuPipeline.map((proc) => { return `${proc.constructor.name}`}).join(' => ')}`)
	    await pipeline(yadamuPipeline)
        this.STATUS.operationSuccessful = true;
        // this.LOGGER.trace([this.constructor.name,'PIPELINE'],'Success')
   	    /*
	    if (source.isDatabase()) {
		  this.LOGGER.trace([this.constructor.name,'PIPELINE',performance.now()],'Waiting on Data Complete')
	      await dbReader.dataComplete
		  this.LOGGER.trace([this.constructor.name,'PIPELINE',performance.now()],'DataComplete')
	    }
	    */
        await source.finalize();
   	    await target.finalize();
        this.reportStatus(this.STATUS,this.LOGGER)
	    results = this.metrics
      } catch (e) {
	    // If the pipeline operation throws 'ERR_STREAM_PREMATURE_CLOSE' get the underlying cause from the dbReader;
	    if (e.code === 'ERR_STREAM_PREMATURE_CLOSE') {
		  e = dbReader.underlyingError instanceof Error ? dbReader.underlyingError : (dbWriter.underlyingError instanceof Error ? dbWriter.underlyingError : e)
	    }
	    this.LOGGER.handleException(['YADAMU','PIPELINE'],e)
  	    // this.LOGGER.trace([this.constructor.name,'PIPELINE','FAILED'],e)

        if (source.isDatabase() && ((e instanceof DatabaseError) && !e.lostConnection())) {
		  if (dbReader.copyInProgress()) {
			const startTime = performance.now()
	        this.LOGGER.info(['YADAMU','PIPELINE'],`Copy operation failed. Clean-up in progress ...`)
            try {
  	          await dbReader.dataComplete
			} catch(e) {
              // this.yadamuLogger.trace([this.constructor.name],'DATA_COMPLETE'],e)
            }
	        this.LOGGER.info(['YADAMU','PIPELINE'],`Copy operation failed. Clean-up completed. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`)
		  }
	    }
		throw e;
	  }
	} catch (e) {		  
	  this.STATUS.operationSuccessful = false;
	  this.STATUS.err = e;
      results = e;
   
      await source.abort(e);
      await target.abort(e);
    
	  if (!((e instanceof UserError) && (e instanceof FileNotFound))) {
        this.reportError(e,this.parameters,this.STATUS,this.LOGGER);
	  }
    }

    await this.REJECTION_MANAGER.close();
    await this.WARNING_MANAGER.close();
    return results;
  }
    
  async pumpData(source,target) {
     
	this.setDefaultParameter(source.parameters,'YADAMU_USER','FROM_USER')
    if ((source.isDatabase() === true) && (source.parameters.FROM_USER === undefined)) {
      throw new Error('Missing mandatory parameter FROM_USER');
    }

	this.setDefaultParameter(source.parameters,'YADAMU_USER','TO_USER')
    if ((target.isDatabase() === true) && (target.parameters.TO_USER === undefined)) {
      throw new Error('Missing mandatory parameter TO_USER');
    }
	
    return await this.doPumpOperation(source,target)    
  }
  
  async doImport(dbi) {
    const fileReader = new FileDBI(this)
    const metrics = await this.pumpData(fileReader,dbi);
    await this.close();
    return metrics
  }  
 
  async doExport(dbi) {
    const fileWriter = new FileDBI(this)
    const metrics = await this.pumpData(dbi,fileWriter);
    await this.close();
    return metrics
  }  
  
  async doCopy(source,target) {
    const metrics = await this.pumpData(source,target);
    await this.close();
    return metrics
  }  
  
  async cloneFile(pathToFile) {

    const fileReader = new FileDBI(this)
    const fileWriter = new fileDBI(this)
    const metrics = await this.pumpData(fileReader,fileWriter);
    await this.close();
    return metrics
  }
  
  async uploadFile(dbi,importFilePath) {
	let stack 
	try {
      stack = new Error().stack		
      const stats = fs.statSync(importFilePath)
      const fileSizeInBytes = stats.size    

      const startTime = performance.now();
      const json = await dbi.uploadFile(importFilePath);
      const elapsedTime = performance.now() - startTime;
      this.LOGGER.info([`${dbi.DATABASE_VENDOR}`,`UPLOAD`],`File "${importFilePath}". Size ${fileSizeInBytes}. Elapsed time ${YadamuLibrary.stringifyDuration(elapsedTime)}s.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.`)
      return json;
    } catch (err) {
      throw err.code === 'ENOENT' ? new FileNotFound(err,stack,importFilePath) : new FileError(err,stack,importFilePath)
	}
  }
   
  getMetrics(log) {
      
     const metrics = {}
     log.forEach((entry) => {
       switch (Object.keys(entry)[0]) {
         case 'dml' :
           metrics[entry.dml.tableName] = {rowCount : entry.dml.rowCount, insertMode : "SQL", elapsedTime : entry.dml.elapsedTime + "ms", throughput: Math.round((entry.dml.rowCount/Math.round(entry.dml.elapsedTime)) * 1000).toString() + "/s"}
           break;
         case 'error' :
           metrics[entry.error.tableName] = {rowCount : -1, insertMode : "SQL", elapsedTime : "NaN", throughput: "NaN"}
           break;
         default:
       }
     })
     return metrics
  }

  
  async doUploadOperation(dbi) {

    const metrics = {}

    try {
      await dbi.initialize();
      this.STATUS.operationSuccessful = false;
      const pathToFile = dbi.parameters.FILE;
      const hndl = await this.uploadFile(dbi,pathToFile);
      const log = await dbi.processFile(hndl)
	  await dbi.releasePrimaryConnection()
      await dbi.finalize();
	  const metrics = this.getMetrics(log);  
	  this.STATUS.operationSuccessful = true;
      return metrics
    } catch (e) {
	  this.STATUS.operationSuccessful = false;
	  this.STATUS.err = e;
      await dbi.abort(e)
    }
    return metrics;
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

}  
     
module.exports = Yadamu;
