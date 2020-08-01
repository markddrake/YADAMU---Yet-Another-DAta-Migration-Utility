"use strict"

const fs = require('fs');
const path = require('path');
const util = require('util');

const DBWriter = require('./dbWriter.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const StringWriter = require('./stringWriter.js')
const {DatabaseError, IterativeInsertError, BatchInsertError}  = require('./yadamuError.js');
const OracleError  = require('../oracle/node/oracleError.js');
const FileDBI = require('../file/node/fileDBI.js');

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

  static get LOGGER_DEFAULTS() {
    this._LOGGER_DEFAULTS = this._LOGGER_DEFAULTS || Object.freeze({
      "EXCEPTION_FOLDER"          : 'exception'
    , "EXCEPTION_FILE_PREFIX"     : 'exception'
    })
    return this._LOGGER_DEFAULTS;
  }
  
  static  get EXCEPTION_FOLDER() { return this.LOGGER_DEFAULTS.EXCEPTION_FOLDER }
  static  get EXCEPTION_FILE_PREFIX() { return this.LOGGER_DEFAULTS.EXCEPTION_FILE_PREFIX }
  
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
    let exceptionFolderRoot = this.IS_CONSOLE_LOGGER ? process.cwd() : path.dirname(this.os.path);
    
    exceptionFolderPath = ((exceptionFolderPath === null) || (exceptionFolderPath === undefined)) ? YadamuLogger.EXCEPTION_FOLDER : exceptionFolderPath
    exceptionFolderPath = YadamuLibrary.pathSubstitutions(exceptionFolderPath)
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
    exceptionFilePrefix = YadamuLibrary.pathSubstitutions(exceptionFilePrefix)
    this._EXCEPTION_FILE_PREFIX = exceptionFilePrefix
  }
  
  get EXCEPTION_FILE_PREFIX() { return this._EXCEPTION_FILE_PREFIX || YadamuLibrary.pathSubstitutions(YadamuLogger.EXCEPTION_FILE_PREFIX) }
  
  get LOG_STACK_TRACE_TO_CONSOLE() {
     return process.env.YADAMU_PRINT_STACK && process.env.YADAMU_PRINT_STACK.toUpperCase() === 'TRUE' 
  }
 
  get IS_CONSOLE_LOGGER() {
    return this.os === process.stdout;
  }

  static fileLogger(logFilePath,state,exceptionFolder,exceptionFilePrefix) {
    const os = ((logFilePath === undefined) || (logFilePath === '') || (logFilePath === null)) ? process.stdout : (() => {
    try {
      const absolutePath = path.resolve(logFilePath);
      return fs.createWriteStream(this.parameters.LOG_FILE,{flags : "a"})
    } catch (e) {
      return process.out;
    }
    })();   
    return new YadamuLogger(os,state,exceptionFolder,exceptionFilePrefix)
  }
    
  static nulLogger() {
    // TODO : Create a subclass that actually does not write stuff to NUL
    const nul = fs.createWriteStream("\\\\.\\NUL",{flags : "a"})
    return new YadamuLogger(nul,{})
  }
  
  static consoleLogger(state,exceptionFolder,exceptionFilePrefix) {
    return new YadamuLogger(process.stdout,state,exceptionFolder,exceptionFilePrefix)
  }
  
  constructor(outputStream,state,exceptionFolder,exceptionFilePrefix) {
   
    this.os = outputStream;
    this.state = state;
	this.EXCEPTION_FOLDER_PATH = exceptionFolder
    this.EXCEPTION_FILE_PREFIX = exceptionFilePrefix
    this.resetCounters();
  }

  switchOutputStream(os) {
	this.os = os;
  }

  write(msg) {
    this.os.write('**'+msg);
  }

  writeDirect(msg) {
    this.os.write(msg);
  }

  logNoTimestamp(args,msg) {
    this.os.write(`${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${msg}\n`)
  }
  
  log(args,msg) {
	const ts = new Date().toISOString()
    this.os.write(`${ts} ${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${msg}\n`)
	return ts
  }
  
  qa(args,msg) {
    args.unshift('QA')
    return this.log(args,msg)
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
	this.counters.warnings++
    args.unshift('WARNING')
	return this.log(args,msg)
  }

  error(args,msg) {
    this.state.errorRaised = true;
	this.counters.errors++
    args.unshift('ERROR')
    return this.log(args,msg)
  }

  serializeException(e) {
    return util.inspect(e,{depth:null})
  }

  serializeException1(e) {
 	try {
	  const out = new StringWriter();
	  const err = new StringWriter();
	  const con = new console.Console(out,err);
	  con.dir(e, { depth: null });
	  const serialization = out.toString();
	  return serialization
	  this.os.write(`cause: ${strCause}\n`)
	} catch (e) {
      console.log(e);
	}
  }

  logDatabaseError(e) {
    this.os.write(`${e.message}\n`);
	this.os.write(`${e.stack}\n`)
	this.os.write(`SQL: ${e.sql}\n`);
    if (e instanceof OracleError) {
	  this.logOracleError(e)
	}
	if (e.cause instanceof Error) {
      this.os.write(`cause: ${this.serializeException(e.cause)}\n`)
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
	  this.os.write(`${this.serializeException(e)}\n`)
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
	const errorLog = this.createLogFile(exceptionFile)
	fs.writeSync(errorLog,`${ts} ${args.map((arg) => { return '[' + arg + ']'}).join('')}: ${e.message}\n`)
	fs.writeSync(errorLog,this.serializeException(e));
	fs.closeSync(errorLog)
  }

  async writeDataFile(dataFilePath,tableName,currentSettings,data) {
	try {
	  const dbi = new FileDBI(currentSettings.yadamu,dataFilePath)
	  await dbi.initialize()
	  // const logger = this
      const logger = YadamuLogger.nulLogger();
	  const dbWriter = new DBWriter(dbi,logger);  
	  const ddlComplete = new Promise((resolve,reject) => {
	    dbWriter.once('ddlComplete',() => {
 	      resolve(true);
	    })
	  })
	  await dbWriter.initialize()
      dbWriter.write({systemInformation: currentSettings.systemInformation})
	  dbWriter.write({metadata: currentSettings.metadata})
	  dbWriter.write({pause:true})
	  await ddlComplete
	  const tableWriter = dbi.getOutputStream(tableName)
	  // Disable the columnCountCheck when writing an error report
	  tableWriter.checkColumnCount = () => {}
	  tableWriter.initialize();
	  for (const d of data) {
        tableWriter.write({data:d})
	  }
	  await new Promise((resolve,reject) => {
		tableWriter.end(null,null,() => {
          resolve()
        })
      })		
	  dbWriter.deferredCallback();
	  await new Promise((resolve,reject) => {
		dbWriter.end(null,null,() => {
          resolve()
        })
      })		
	  await dbi.finalize()
      await logger.close();
	} catch (e) {
	  console.log(e)
	}
  }
  
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

  handleException(args,e) {
    // Handle Exception does not produce any output if the exception has already been processed by handleException ot logException

	if (e.yadamuAlreadyReported !== true) {
	  if (this.LOG_STACK_TRACE_TO_CONSOLE === true) {
		this.logException(args,e)
	  }
	  else {
		const largs = [...args]
		const ts = this.error(args,e.message);
        const exceptionFile = path.resolve(`${this.EXCEPTION_FOLDER}${path.sep}${this.EXCEPTION_FILE_PREFIX}_${ts.replace(/:/g,'.')}.trace`);
		this.generateDataFile(exceptionFile,e);
		this.writeExceptionToFile(exceptionFile,ts,args,e)
	    this.info(largs,`Exception logged to "${exceptionFile}".`)
		e.yadamuAlreadyReported = true;
	  }
	}
  }
  
  handleWarning(args,e) {
	 
    // Handle Exception does not produce any output if the exception has already been processed by handleException ot logException

	if (e.yadamuAlreadyReported !== true) {
	  if (this.LOG_STACK_TRACE_TO_CONSOLE === true) {
		this.logException(args,e)
	  }
	  else {
		const largs = [...args]
		const ts = this.warning(args,e.message);
        const exceptionFile = path.resolve(`${this.EXCEPTION_FOLDER}${path.sep}${this.EXCEPTION_FILE_PREFIX}_${ts.replace(/:/g,'.')}.trace`);
		this.generateDataFile(exceptionFile,e);
		this.writeExceptionToFile(exceptionFile,ts,args,e)
	    this.info(largs,`Exception logged to "${exceptionFile}".`)
		e.yadamuAlreadyReported = true;
	  }
	}
  }

  logRejected(args,e) {
	args.unshift('REJECTED')
	this.handleException(args,e);
    /*
	const largs = [...args]
	const ts = this.warning(args,e.message);
    const exceptionFile = path.resolve(`${this.EXCEPTION_FOLDER}${path.sep}${this.EXCEPTION_FILE_PREFIX}_${ts.replace(/:/g,'.')}.trace`);
    this.writeExceptionToFile(exceptionFile,ts,args,e)
	this.warning(largs,`Details logged to "${exceptionFile}".`)
	*/
  }

  trace(args,msg) {
    args.unshift('TRACE')
    this.log(args,msg)
  }
  
  resetCounters() {
	this.counters = {
	  errors   : 0
    , warnings : 0
	, failed   : 0
	}
  }
  
  getCounters(reset) {
	const counters = Object.assign({},this.counters)
    if (reset) this.resetCounters();
	return counters
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

    if (!this.IS_CONSOLE_LOGGER) {
      await this.closeStream()
      this.os = process.stdout
    }    
  }
  
}

module.exports = YadamuLogger