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
  
  constructor(outputStream,state) {
   
    this.resetCounters();
	
    this.os = outputStream;
    this.state = state;
    this.exceptionFolderPath = this.state.exceptionFolder === undefined ? 'exception' : YadamuLibrary.pathSubstitutions(this.state.exceptionFolder) 
    this.exceptionFilePrefix = this.state.exceptionFilePrefix === undefined ? 'exception' : YadamuLibrary.pathSubstitutions(this.state.EXCEPTION_FILE_PREFIX);
    this.exceptionFolderLocation = undefined
	this.inlineStackTrace = process.env.YADAMU_INLINE_STACK && process.env.YADAMU_INLINE_STACK.toUpperCase() === 'TRUE'
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
    if (this.exceptionFolderLocation == undefined) {
      this.exceptionFolderLocation = path.dirname(filename);
      fs.mkdirSync(this.exceptionFolderLocation, { recursive: true });
    }
    const ws = fs.openSync(filename,'w');
    return ws;
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
      const logger = new YadamuLogger(fs.createWriteStream("\\\\.\\NUL"),{});  
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
	} catch (e) {
	  console.log(e)
	}
  }
  
  generateDataFile(exceptionFile,e) {
    if (e instanceof IterativeInsertError) {
	  // Write the row information a seperate file and replace the row tag with a reference to the file
	  e.dataFilePath = `${path.dirname(exceptionFile)}${path.sep}${path.basename(exceptionFile,'.trace')}.data`
	  this.writeDataFile(e.dataFilePath,e.tableName,e.currentSettings,[e.row])
	  delete e.currentSettings
	  delete e.row
	}
	if (e instanceof BatchInsertError) {
	  // Write the row information a seperate file and replace the row tag with a reference to the file
	  e.dataFilePath = `${path.dirname(exceptionFile)}${path.sep}${path.basename(exceptionFile,'.trace')}.data`
	  this.writeDataFile(e.dataFilePath,e.tableName,e.currentSettings,e.rows)
	  delete e.currentSettings
	  delete e.rows
	}
  }

  handleException(args,e) {
	 
    // Handle Exception does not produce any output if the exception has already been processed by handleException ot logException

	if (e.yadamuAlreadyReported !== true) {
	  if (this.inlineStackTrace === true) {
		this.logException(args,e)
	  }
	  else {
		const largs = [...args]
		const ts = this.error(args,e.message);
        const exceptionFile = path.resolve(`${this.exceptionFolderPath}${path.sep}${this.exceptionFilePrefix}_${ts.replace(/:/g,'.')}.trace`);
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
	  if (this.inlineStackTrace === true) {
		this.logException(args,e)
	  }
	  else {
		const largs = [...args]
		const ts = this.warning(args,e.message);
        const exceptionFile = path.resolve(`${this.exceptionFolderPath}${path.sep}${this.exceptionFilePrefix}_${ts.replace(/:/g,'.')}.trace`);
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
    const exceptionFile = path.resolve(`${this.exceptionFolderPath}${path.sep}${this.exceptionFilePrefix}_${ts.replace(/:/g,'.')}.trace`);
	this.writeExceptionToFile(exceptionFile,ts,args,e)
	this.warning(largs,`Details logged to "${exceptionFile}".`)
	*/
  }

  trace(args,msg) {
    args.unshift('TRACE')
    this.log(args,msg)
  }
  
  loggingToConsole() {
    return this.os === process.stdout;
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

    if (this.os !== process.stdout) {
      const closer = this.closeStream()
      await closer;
      this.os = process.stdout
    }    
  }
  
}

module.exports = YadamuLogger