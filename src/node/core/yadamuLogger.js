
import fs            from 'fs';
import path          from 'path';

import {Readable}    from 'stream'
import {pipeline}    from 'stream/promises'

import YadamuLibrary from '../lib/yadamuLibrary.js';
import StringWriter  from '../util//stringWriter.js'
import NullWriter    from '../util//nullWriter.js';
import {OracleError} from '../dbi/oracle/oracleException.js';
import ErrorDBI      from '../dbi/file/errorDBI.js';
import {FileError}   from '../dbi/file/fileException.js';

import DBWriter      from './dbWriter.js';
import {InternalError, DatabaseError, IterativeInsertError, BatchInsertError}  from './yadamuException.js';


class YadamuLogger {
  
  /*
  **
  **  Create from a path. 
  **
  **  If path is undefined, null or || or not valid use process.out
  ** 
  **  Exception Folder can be absolute or relative. If not supplied use default. If relative assume relative to supplied path. 
  **  
  **
  */
  
  static get LOGGER_CLASS() { return YadamuLogger }

  static get LOGGER_DEFAULTS() {
    this._LOGGER_DEFAULTS = this._LOGGER_DEFAULTS || Object.freeze({
      "EXCEPTION_FOLDER"          : 'exception'
    , "EXCEPTION_FILE_PREFIX"     : 'exception'
    })
    return this._LOGGER_DEFAULTS;
  }
  
  static  get EXCEPTION_FOLDER() { return this.LOGGER_DEFAULTS.EXCEPTION_FOLDER }
  static  get EXCEPTION_FILE_PREFIX() { return this.LOGGER_DEFAULTS.EXCEPTION_FILE_PREFIX }
  
  static get NULL_LOGGER() {
    this._NULL_LOGGGER = this._NULL_LOGGGER || new this.LOGGER_CLASS(NullWriter.NULL_WRITER,{})
    return this._NULL_LOGGGER;
  }

  /*
  **
  ** _EXCEPTION_FOLDER_PATH contains the path passed to createYadamuLogger or the default value for the location of the exception folder.
  **
  ** _EXCEPTION_FOLDER contains a validated path to an existing folder.
  **
  */

  set EXCEPTION_FOLDER_PATH(value) {
    /*
    **
    ** Calculate the absolure path to the folder used when logging an exception.
    **
    ** If the supplied value is null or undefined start with the system defined default
    ** 
    ** If the value if not absolute convert it to an absolute path. 
    ** If logging to a file treat the location as relative to the location of the log file
    ** If logging to stdout treat the location relative as relative to the current working directory.
    **
    **
    */
    let exceptionFolderPath = value
    let exceptionFolderRoot = this.FILE_BASED_LOGWRITER ? path.dirname(this.os.path) : process.cwd() 
    
    exceptionFolderPath = ((exceptionFolderPath === null) || (exceptionFolderPath === undefined)) ? YadamuLogger.EXCEPTION_FOLDER : exceptionFolderPath
    exceptionFolderPath = YadamuLibrary.macroSubstitions(exceptionFolderPath,[])
    exceptionFolderPath = exceptionFolderPath === '' ? exceptionFolderRoot : exceptionFolderPath
    exceptionFolderPath = (!path.isAbsolute(exceptionFolderPath)) ? path.join(exceptionFolderRoot,exceptionFolderPath) : exceptionFolderPath
    this._EXCEPTION_FOLDER_PATH = exceptionFolderPath;
  }
  
  get EXCEPTION_FOLDER() {
    this._EXCEPTION_FOLDER = this._EXCEPTION_FOLDER || (() => {
      // If EXCEPTION_FOLDER_PATH has not been explictly set, invoke the setter method passing the empty string. 
      // This will set it to the directory containing the current log file or the current working directory when logging to the console;
      this._EXCEPTION_FOLDER_PATH = this._EXCEPTION_FOLDER_PATH || (() => { this.EXCEPTION_FOLDER_PATH = ''; return this.EXCEPTION_FOLDER})()
      fs.mkdirSync(this._EXCEPTION_FOLDER_PATH,{recursive: true});
      return this._EXCEPTION_FOLDER_PATH
    })();
    return this._EXCEPTION_FOLDER
  }

  set EXCEPTION_FILE_PREFIX(value) {
    let exceptionFilePrefix = value
    exceptionFilePrefix = ((exceptionFilePrefix === null) || (exceptionFilePrefix === undefined)) ? YadamuLogger.EXCEPTION_FILE_PREFIX : exceptionFilePrefix
    exceptionFilePrefix = YadamuLibrary.macroSubstitions(exceptionFilePrefix,[])
    this._EXCEPTION_FILE_PREFIX = exceptionFilePrefix
  }
  
  get EXCEPTION_FILE_PREFIX() { return this._EXCEPTION_FILE_PREFIX || YadamuLibrary.macroSubstitions(YadamuLogger.EXCEPTION_FILE_PREFIX,[]) }
  
  get YADAMU_STACK_TRACE() {
     return process.env.YADAMU_STACK_TRACE && process.env.YADAMU_STACK_TRACE.trim().toUpperCase() === 'TRUE' 
  }
  
  get FILE_BASED_LOGGER() {
    switch (true) {
      case (this.os === NullWriter.NULL_WRITER):
      case (this.os === process.stdout):
         return false
      default:
        return true
	}
  }
  
  get CONSOLE_BASED_LOGGER() {
	 return (this.os === process.stdout)
  }
  
  static fileLogger(logFilePath,state,exceptionFolder,exceptionFilePrefix) {
	if ((logFilePath === undefined) || (logFilePath === '') || (logFilePath === null)) {
	  console.log(`${new Date().toISOString()}[YADAMU][LOGGER]: Invalid value supplied for parameter LOG_FILE (${logFilePath}). Logging to console`); 
      return this.consoleLogger(state,exceptionFolder,exceptionFilePrefix)
	} 
    const absolutePath = path.resolve(logFilePath);	
    try {
      const os = fs.createWriteStream(absolutePath,{flags : "a"})
      return new this.LOGGER_CLASS(os,state,exceptionFolder,exceptionFilePrefix)
    } catch (e) {
	  console.log(`${new Date().toISOString()}[YADAMU][LOGGER][FILE]: Unable to create log file "${absolutePath}". Logging to console`); 
      return this.console.Logger(state,exceptionFolder,exceptionFilePrefix)
    }
  }
  
  static consoleLogger(state,exceptionFolder,exceptionFilePrefix) {
    return new this.LOGGER_CLASS(process.stdout,state,exceptionFolder,exceptionFilePrefix)
  }
  
  static streamLogger(os,state,exceptionFolder,exceptionFilePrefix) {
    try {
      return new this.LOGGER_CLASS(os,state,exceptionFolder,exceptionFilePrefix)
    } catch (e) {
	  console.log(`${new Date().toISOString()}[YADAMU][LOGGER][STREAM]: Unable to create log file from supplied stream". Logging to console`); 
      return this.console.Logger(state,exceptionFolder,exceptionFilePrefix)
    }
  }

  constructor(outputStream,state,exceptionFolder,exceptionFilePrefix) {
   
    this.os = outputStream !== undefined ? outputStream : process.out
    this.state = state;
    this.EXCEPTION_FOLDER_PATH = exceptionFolder
    this.EXCEPTION_FILE_PREFIX = exceptionFilePrefix
    this.resetMetrics();
  }
  
  switchOutputStream(os) {
	if (this.os !== undefined) {
      this.os = os;
	}
  }
  
  write(msg) {
    this.os.write('**'+msg);
  }

  writeDirect(msg) {
    this.os.write(msg);
  }

  logNoTimestamp(args,msg) {
	try {
      this.os.write(`${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${msg}\n`)
	} catch (e) {
	  if (e.message === `TypeError: Cannot read property 'write' of undefined`) {
        this.os = process.out
        this.os.write(`${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${msg}\n`)
	  }
	}
  }
  
  log(args,msg) {

    const ts = new Date().toISOString()
    try {
      this.os.write(`${ts} ${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${msg}\n`)
	} catch (e) {
	  if (e.message === `TypeError: Cannot read property 'write' of undefined`) {
        this.os = process.out
	    this.os.write(`${ts} ${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${msg}\n`)
	  }
	}    
    return ts
  }
  
  info(args,msg) {
    args.unshift('INFO')
    return this.log(args,msg)
  }
  
  dml(args,msg) {
    args.unshift('DML')
    return this.log(args,msg)
  }

  ddl(args,msg) {
    args.unshift('DDL')
    return this.log(args,msg)
  }

  sql(args,msg) {
    args.unshift('SQL')
    this.log(args,msg)
  }

  warning(args,msg) {
    this.state.warningRaised = true;
    this.metrics.warnings++
    args.unshift('WARNING')
    return this.log(args,msg)
  }

  error(args,msg) {
    this.state.errorRaised = true;
    this.metrics.errors++
    args.unshift('ERROR')
    return this.log(args,msg)
  }

  logDatabaseError(e) {
    this.os.write(`${e.message}\n`);
    this.os.write(`${e.stack}\n`)
    this.os.write(e instanceof FileError ?  `PATH: "${e.path}"\n` : `SQL: ${e.sql}\n`);
    if (e instanceof OracleError) {
      this.logOracleError(e)
    }
    if (e.cause instanceof Error) {
      this.os.write(`cause: ${YadamuLibrary.serializeException(e.cause)}\n`)
    }
  }
  
  logOracleError(e) {
    this.os.write(`Args/Binds: ${JSON.stringify(e.args)}\n`);
    this.os.write(`OutputFormat/Rows: ${JSON.stringify(e.outputFormat)}\n`);
  } 
  
  logException(args,e) {

    if (e.yadamuAlreadyReported) {
      this.info(args,`Processed exception: "${e.message}"`);
      return
    }

    this.error(args,`Caught exception`);
    if (e instanceof DatabaseError) {
      this.logDatabaseError(e);
    }
    else {
      this.os.write(`${YadamuLibrary.serializeException(e)}\n`)
    }
    e.yadamuAlreadyReported = true;
  }

  createLogFile(filename) {
    const ws = fs.openSync(filename,'w');
    return ws;
  }
  
  writeLogToFile(args,log) {
    const ts = new Date().toISOString()
    const logFile = path.resolve(`${this.EXCEPTION_FOLDER_PATH}${path.sep}${this.LOG_FILE_PREFIX}_${ts.replace(/:/g,'.')}.json`);
    const errorLog = this.createLogFile(logFile)
    fs.writeSync(errorLog,JSON.stringify(log));
    fs.closeSync(errorLog)  
    this.info(args,`Server Log written to "${logFile}".`)
  }
  
  writeExceptionToFile(exceptionFile,ts,args,e) {
    if (this.YADAMU_STACK_TRACE === true) {
      console.dir(e,{depth: null})
    }

    const errorLog = this.createLogFile(exceptionFile)
    fs.writeSync(errorLog,`${ts} ${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${e.message}\n`)
    fs.writeSync(errorLog,YadamuLibrary.serializeException(e));
    fs.closeSync(errorLog)
  }
  
  reportLoggerError(loggerError) {
    try {
	  this.handleException(['LOGGER','DATA_FILE'],loggerError);
	} catch (e) {
	  console.log(loggerError)
	}
  }	  
  
  async writeMetadata(exceptionFile,yadamu,systemInformation,metadata) {
	const errorPipeline = []
 	const metadataFile = exceptionFile.replace(`${yadamu.EXCEPTION_FILE_PREFIX}_`,`metadata_`).replace('.trace','.json')
    try {
	 
      const dbi = new ErrorDBI(yadamu,metadataFile)
      await dbi.initialize()
      await dbi.initializeImport();
      dbi.setSystemInformation(systemInformation)
      dbi.setMetadata(metadata)
	  await dbi.initializeData()
	  await dbi.finalizeData();
	  await dbi.finalizeImport();
    } catch (err) {    
	  const loggerError = new Error(`Error creating data file "${metadataFile}".`)
	  loggerError.cause = err
	  loggerError.systemInformation = systemInformation
      loggerError.metadata = metadata
      if (err.code === 'ERR_STREAM_PREMATURE_CLOSE') {
        errorPipeline.forEach((stream) => {
  		  if (stream.underlyingError instanceof Error) {
		    loggerError.cause[stream.constructor.name] = stream.underlyingError
		   }
	    })
      }
	  this.reportLoggerError(loggerError)
    }
  }
	  
  async writeDataFile(dataFilePath,tableName,currentSettings,data) {
	const errorPipeline = []
    try {
      const dbi = new ErrorDBI(currentSettings.yadamu,dataFilePath)
      await dbi.initialize()
      await dbi.initializeImport();
      dbi.setSystemInformation(currentSettings.systemInformation)
      dbi.setMetadata(currentSettings.metadata)
	  await dbi.initializeData()
	  const is = Readable.from([{table:tableName},...data.map((d) => { return {data:d}})])
	  const outputStreams = dbi.getOutputStreams(tableName,{})
	  errorPipeline.push(is)
      errorPipeline.push(...outputStreams)
      await pipeline(errorPipeline,{end:false})
      await dbi.finalizeData();
	  await dbi.finalizeImport();
    } catch (err) {    
	  const loggerError = new Error(`Error creating data file "${dataFilePath}".`)
	  loggerError.cause = err
	  loggerError.systemInformation = currentSettings.systemInformation
      loggerError.metadata = currentSettings.metadata
	  loggerError.data = currentSettings.data
      if (err.code === 'ERR_STREAM_PREMATURE_CLOSE') {
        errorPipeline.forEach((stream) => {
  		  if (stream.underlyingError instanceof Error) {
		    loggerError.cause[stream.constructor.name] = stream.underlyingError
		   }
	    })
      }
	  this.reportLoggerError(loggerError)
    }
  }

  // async writeDataFile(dataFilePath,tableName,currentSettings,data) {}
  
  generateDataFile(exceptionFile,e) {
    if (e instanceof IterativeInsertError) {
      // Write the row information a seperate file and replace the row tag with a reference to the file
      e.dataFilePath = path.resolve(`${path.dirname(exceptionFile)}${path.sep}${path.basename(exceptionFile,'.trace')}.data`);
      this.writeDataFile(e.dataFilePath,e.tableName,e.currentSettings,[e.row])
      delete e.currentSettings
      delete e.row
    }
    if (e instanceof BatchInsertError) {
      // Write the row information a seperate file and replace the row tag with a reference to the file
      e.dataFilePath = path.resolve(`${path.dirname(exceptionFile)}${path.sep}${path.basename(exceptionFile,'.trace')}.data`);
      this.writeDataFile(e.dataFilePath,e.tableName,e.currentSettings,e.rows)
      delete e.currentSettings
      delete e.rows
    }
  }

  logInternalError(args,msg,info) {
    args.unshift('INTERNAL')
    const internalError = new InternalError(msg,args,info)
    this.handleException(args,internalError)
    throw internalError
  }

  handleException(args,e) {
    // Handle Exception does not produce any output if the exception has already been processed by handleException or logException
    if (e.yadamuAlreadyReported === true) {
      if (this.YADAMU_STACK_TRACE === true) {
        this.log(['YADAMU_STACK_TRACE',...args],e)
      }
    }
    else {
      const largs = [...args]
      const ts = this.error(args,e.message);
      const exceptionFile = path.resolve(`${this.EXCEPTION_FOLDER}${path.sep}${this.EXCEPTION_FILE_PREFIX}_${ts.replace(/:/g,'.')}.trace`);
      this.generateDataFile(exceptionFile,e);
      this.writeExceptionToFile(exceptionFile,ts,args,e)
      this.info(largs,`Exception logged to "${exceptionFile}".`)
      e.yadamuAlreadyReported = true;
	  return exceptionFile
    }
  }
  
  handleWarning(args,e) {
     
    // Handle Exception does not produce any output if the exception has already been processed by handleException ot logException

    if (e.yadamuAlreadyReported === true) {
      if (this.YADAMU_STACK_TRACE === true) {
        this.trace(args,e)
      }
    }
    else {
      const largs = [...args]
      const ts = this.warning(args,e.message);
      const exceptionFile = path.resolve(`${this.EXCEPTION_FOLDER}${path.sep}${this.EXCEPTION_FILE_PREFIX}_${ts.replace(/:/g,'.')}.trace`);
      this.generateDataFile(exceptionFile,e);
      this.writeExceptionToFile(exceptionFile,ts,args,e)
      this.info(largs,`Exception logged to "${exceptionFile}".`)
      e.yadamuAlreadyReported = true;
	  return exceptionFile
    }
  }

  logRejected(args,e) {
    args.unshift('REJECTED')
    this.handleException(args,e);
  }

  logRejectedAsWarning(args,e) {
    args.unshift('REJECTED')
    this.handleWarning(args,e);
  }
  
  trace(args,msg) {
    args.unshift('TRACE')
    this.log(args,msg instanceof Error ? msg.message : msg)
  }
  
  resetMetrics() {
    this.metrics = {
      errors   : 0
    , warnings : 0
    , failed   : 0
    }
  }
  
  getMetrics(reset) {
    const metrics = {
		...this.metrics
    }
    if (reset) this.resetMetrics();
    return metrics
  }
  
  closeStream() {
  
    if (this.os.close) { 
      return new Promise((resolve,reject) => {
        this.os.on('finish',() => { resolve() });
        this.os.close();
      })
    }
    
  }
  
  async close() {

    if (this.FILE_BASED_LOGWRITER) {
      await this.closeStream()
      this.os = process.stdout
    }    
  }
   
  qa(args,msg) {}
  
  qaInfo(args,msg) {}
  
  qaWarning(args,msg) {}

  qaError(args,msg) {}
  
}

export { YadamuLogger as default}