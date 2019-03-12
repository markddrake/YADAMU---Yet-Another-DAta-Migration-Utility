"use strict"

const fs = require('fs');
const path = require('path');
  
const FileParser = require('../file/node/fileParser.js');
const FileWriter = require('../file/node/fileExport.js');
const DBReader = require('./dbReader.js');
const DBWriter = require('./dbWriter.js');
const ImportErrorManager = require('./importErrorManager.js');

class Yadamu {

  get EXPORT_VERSION() { return '1.0' };
  
  static nameMatch(source,target,rule) {
      
    switch (rule)  {
    
      case 'EXACT':
        if (source === target) {
          return true;
        }
        break;
      case 'UPPER':
        if (source.toUpperCase() === target) {
           return true;
        }
        break;
      case 'LOWER': 
        if (source.toLowerCase() === target) {
           return true;
        }
        break;
      case 'INSENSITIVE':
        if (source.toLowerCase() === target.toLowerCase()) {
           return true;
        }
      default:
    }
  }
 
  static convertIdentifierCase(identifierCase, metadata) {
            
    switch (identifierCase) {
      case 'UPPER':
        for (let table of Object.keys(metadata)) {
          metadata[table].columns = metadata[table].columns.toUpperCase();
          if (table !== table.toUpperCase()){
            metadata[table].tableName = metadata[table].tableName.toUpperCase();
            Object.assign(metadata, {[table.toUpperCase()]: metadata[table]});
            delete metadata[table];
          }
        }           
        break;
      case 'LOWER':
        for (let table of Object.keys(metadata)) {
          metadata[table].columns = metadata[table].columns.toLowerCase();
          if (table !== table.toLowerCase()) {
            metadata[table].tableName = metadata[table].tableName.toLowerCase();
            Object.assign(metadata, {[table.toLowerCase()]: metadata[table]});
            delete metadata[table];
          } 
        }     
        break;         
      default: 
    }             
    return metadata
  }
  
  static decomposeDataType(targetDataType) {
    
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
  
  static convertQuotedIdentifer(parameterValue) {

    if (parameterValue.startsWith('"') && parameterValue.endsWith('"')) {
      return parameterValue.slice(1,-1);
    }
    else {
      return parameterValue.toUpperCase()
    }	
  }

  static processValue(parameterValue) {

    if ((parameterValue.startsWith('"') && parameterValue.endsWith('"')) && (parameterValue.indexOf('","') > 0 )) {
      // List of Values
	  let parameterValues = parameterValue.substring(1,parameterValue.length-1).split(',');
	  parameterValues = parameterValues.map(function(value) {
        return Yadamu.convertQutotedIdentifer(value);
	  })
	  return parameterValues
    }
    else {
      return Yadamu.convertQuotedIdentifer(parameterValue);
    }
  }
    
  static processLog(log,status,logWriter) {

    const logDML         = (status.loglevel && (status.loglevel > 0));
    const logDDL         = (status.loglevel && (status.loglevel > 1));
    const logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    const logTrace       = (status.loglevel && (status.loglevel > 3));
   
    if (status.dumpFileName) {
      fs.writeFileSync(status.dumpFileName,JSON.stringify(log));
    }
    
    log.forEach(function(result) {
                  const logEntryType = Object.keys(result)[0];
                  const logEntry = result[logEntryType];
                  switch (true) {
                    case (logEntryType === "message") : 
                      logWriter.write(`${new Date().toISOString()}: ${logEntry}.\n`)
                      break;
                    case (logEntryType === "dml") : 
                      logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elaspsed Time ${Math.round(logEntry.elapsedTime)}ms. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.\n`)
                      break;
                    case (logEntryType === "info") :
                      logWriter.write(`${new Date().toISOString()}[INFO]: "${JSON.stringify(logEntry)}".\n`);
                      break;
                    case (logDML && (logEntryType === "dml")) :
                      logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.\n`)
                      break;
                    case (logDDL && (logEntryType === "ddl")) :
                      logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.\n`) 
                      break;
                    case (logTrace && (logEntryType === "trace")) :
                      logWriter.write(`${new Date().toISOString()} [TRACE]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".\n' : '\n'}${logEntry.sqlStatement}.\n`)
                      break;
                    case (logEntryType === "error"):
   	                switch (true) {
   		              case (logEntry.severity === 'FATAL') :
                          status.errorRaised = true;
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}\n${logEntry.sqlStatement}\n`)
   				        break
   					  case (logEntry.severity === 'WARNING') :
                          status.warningRaised = true;
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}\n`)
                          break;
   					  case (logEntry.severity === 'CONTENT_TOO_LARGE') :
                          status.errorRaised = true;
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: Cannot import columns larger than 32k in 12c.\n`)
                          break;
                        case (logDDLIssues) :
                          logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName  + '".' : ''} Details: ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}\n`)
                      } 	
                  } 
   				if ((status.sqlTrace) && (logEntry.sqlStatement)) {
   				  status.sqlTrace.write(`${logEntry.sqlStatement}\n\/\n`)
   		        }
    }) 
  }    
    

  static finalize(status,logWriter) {

    if (logWriter !== process.stdout) {
      logWriter.close();
      logWriter = process.stdout
    }    
  
    if (status.sqlTrace) {
      status.sqlTrace.close();
    }
  }

  static stringifyDuration(duration) {
        
    let milliseconds = parseInt(duration%1000)
    let seconds = parseInt((duration/1000)%60)
    let minutes = parseInt((duration/(1000*60))%60)
    let hours = parseInt((duration/(1000*60*60))%24);

    hours = (hours < 10) ? "0" + hours : hours;
    minutes = (minutes < 10) ? "0" + minutes : minutes;
    seconds = (seconds < 10) ? "0" + seconds : seconds;

    return `${hours}:${minutes}:${seconds}.${(milliseconds + '').padStart(3,'0')}`;
  }
  
  static reportStatus(status,logWriter) {

    const endTime = new Date().getTime();
      
    status.statusMsg = status.warningRaised ? 'with warnings' : status.statusMsg;
    status.statusMsg = status.errorRaised ? 'with errors'  : status.statusMsg;  
  
    logWriter.write(`${status.operation} operation completed ${status.statusMsg}. Elapsed time: ${Yadamu.stringifyDuration(endTime - status.startTime)}.\n`);
    if (logWriter !== process.stdout) {
      console.log(`${status.operation} operation completed ${status.statusMsg}. Elapsed time: ${Yadamu.stringifyDuration(endTime - status.startTime)}. See "${status.logFileName}" for details.`);  
    }
  }

  static reportError(e,parameters,status,logWriter) {
    
    if (logWriter !== process.stdout) {
      console.log(`${status.operation} operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write(`${status.operation} operation failed.\n`);
      logWriter.write(`${e}\n`);
    }
    else {
      console.log(`${status.operation} operation Failed:`);
      console.log(e);
    }
  }
  
  constructor(operation) {
    this.logWriter = process.stdout;
    this.parameters = this.processArguments();
    this.logWriter = this.setLogWriter();
    
    this.status = {
      operation     : operation
     ,errorRaised   : false
     ,warningRaised : false
     ,statusMsg     : 'successfully'
     ,startTime     : new Date().getTime()
    }
  
    process.on('unhandledRejection', (err, p) => {
      this.logWriter.write(`${new Date().toISOString()}: ${this.status.operation} operation failed with unhandled rejection ${err.stack}\n`);
      process.exit()
    })
      
    if (this.parameters.SQLTRACE) {
	  this.status.sqlTrace = fs.createWriteStream(this.parameters.SQLTRACE);
    }

    if (operation === 'Import') {
       this.status.importErrorMgr = new ImportErrorManager(this.parameters.FILE,1000);
    }
  
    if (this.parameters.LOGFILE) {
      this.status.logFileName = this.parameters.LOGFILE;
    }

    if (this.parameters.LOGLEVEL) {
      this.status.loglevel = this.parameters.LOGLEVEL;
    }
    	
    if (this.parameters.DUMPFILE) {
      this.status.dumpFileName = this.parameters.DUMPFILE
    }
  
  }
  
  getParameters() {
    return this.parameters
  }
  
  getStatus() {
    return this.status
  }

  getLogWriter() {
    return this.logWriter
  }
    
  processArguments() {
   
    const parameters = {
      FILE : "export.json"
     ,MODE : "DDL_AND_DATA"
    }
 
    process.argv.forEach(function (arg) {
     
      if (arg.indexOf('=') > -1) {
        const parameterName = arg.substring(0,arg.indexOf('='));
        const parameterValue = arg.substring(arg.indexOf('=')+1);
        switch (parameterName.toUpperCase()) {
	      case 'USERID':
  	        parameters.USERID = parameterValue;
			break;
          case 'USERNAME':
          case '--USERNAME':
            parameters.USERNAME = parameterValue;
            break;
          case 'PASSWORD':
          case '--PASSWORD':
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
            parameters.FILE = parameterValue;
            break;
          case 'IMPORT':
          case '--IMPORT':
            parameters.IMPORT = parameterValue;
            break;
          case 'EXPORT':
          case '--EXPORT':
            parameters.EXPORT = parameterValue;
            break;
          case 'OWNER':
          case '--OWNER':
            parameters.OWNER = Yadamu.processValue(parameterValue);
            break;
          case 'FROMUSER':
          case '--FROMUSER':
            parameters.FROMUSER = Yadamu.processValue(parameterValue);
            break;
          case 'TOUSER':
          case '--TOUSER':
            parameters.TOUSER = Yadamu.processValue(parameterValue);
            break;
          case 'LOGFILE':
          case '--LOGFILE':
            parameters.LOGFILE = parameterValue;
            break;
          case 'SQLTRACE':
          case '--SQLTRACE':
            parameters.SQLTRACE = parameterValue;
            break;
          case 'LOGLEVEL':
          case '--LOGLEVEL':
            parameters.LOGLEVEL = parameterValue;
            break;
          case 'DUMPFILE':
          case '--DUMPFILE':
            parameters.DUMPFILE = parameterValue.toUpperCase();
            break;
          case 'MODE':
          case '--MODE':
            parameters.MODE = parameterValue.toUpperCase();
            break;
          case 'CONFIG':
          case '--CONFIG':
            parameters.CONFIG = parameterValue;
            break;
          default:
            console.log(`${new Date().toISOString()}[Yadamu]: Unknown parameter: "${parameterName}".`)          
        }
      }
    })
    
    return parameters;
  }
  
  mergeParameters(newValues) {
    Object.keys(newValues).forEach(function(key) {
      if (!this.parameters.hasOwnProperty(key)) {
        this.parameters[key] = newValues[key]
      }
    },this)
    return this.parameters
  }
  
  overwriteParameters(newValues) {
    Object.keys(newValues).forEach(function(key) {
      this.parameters[key] = newValues[key]
    },this)
    return this.parameters
  }

  setLogWriter(parameters) {

    if (this.parameters.LOGFILE) {
      return fs.createWriteStream(this.parameters.LOGFILE,{flags : "a"});
    }
    return this.logWriter;
  
  }
  
  closeFile(outputStream) {
        
    return new Promise(function(resolve,reject) {
      outputStream.on('finish',function() { resolve() });
      outputStream.close();
    })

  }

  getDBReader(dbi) {
    const dbReader = new DBReader(dbi, this.parameters.OWNER, this.parameters.MODE, this.status, this.logWriter);
    return dbReader;
  }
  
  getDBWriter(dbi) {
    const dbWriter = new DBWriter(dbi, this.parameters.TOUSER, this.parameters.MODE, this.status, this.logWriter);
    return dbWriter;
  }

  async doImport(dbi,pathToFile) {

    const logWriter = this.logWriter;
  
    try {
      await dbi.initialize();
      const importFilePath = path.resolve(pathToFile);
      const stats = fs.statSync(importFilePath)
      const fileSizeInBytes = stats.size
      this.logWriter.write(`${new Date().toISOString()}[Yadamu.doImport()]: Processing file "${importFilePath}". Size ${fileSizeInBytes} bytes.\n`)

      const dbWriter = await this.getDBWriter(dbi)
      const readStream = fs.createReadStream(importFilePath);    
      const parser = new FileParser(this.logWriter);
 
      const importOperation = new Promise(function (resolve,reject) {
        try {
          dbWriter.on('finish', function(){resolve(parser.checkState())});
          dbWriter.on('error',function(err){dbi.logWriter.write(`${new Date().toISOString()}[DBWriter.error()]}: ${err.stacl}\n`);reject(err)})      
          readStream.pipe(parser).pipe(dbWriter);
        } catch (err) {
          logWriter.write(`${new Date().toISOString()}[Yadamu.doImport()]}: ${err.stack}\n`);
          reject(err);
        }
      })
    
      await importOperation;
      await dbi.finalize();
      Yadamu.reportStatus(this.status,this.logWriter)
    } catch (e) {
      Yadamu.reportError(e,this.parameters,this.status,this.logWriter);
      await dbi.abort()
    }
    Yadamu.finalize(this.status,this.logWriter);
  }  
  
  async cloneFile(pathToFile) {

  const logWriter = this.logWriter;
  
    try {
      const importFilePath = path.resolve(pathToFile);
      const stats = fs.statSync(importFilePath)
      const fileSizeInBytes = stats.size
      this.logWriter.write(`${new Date().toISOString()}[Yadamu.doImport()]: Processing file "${importFilePath}". Size ${fileSizeInBytes} bytes.\n`)
      
      const fi = new FileWriter(this)
      const readStream = fs.createReadStream(importFilePath);    
      const parser = new FileParser(this.logWriter);
      
      const cloneOperation = new Promise(function (resolve,reject) {
        try {
          fi.on('finish', function(){resolve(parser.checkState())});
          fi.on('error',function(err){dbi.logWriter.write(`${new Date().toISOString()}[DBWriter.error()]}: ${err.stacl}\n`);reject(err)})      
          readStream.pipe(parser).pipe(fi);
        } catch (err) {
          logWriter.write(`${new Date().toISOString()}[Yadamu.doImport()]}: ${err.stack}\n`);
          reject(err);
        }
      })
      await cloneOperation;
      Yadamu.reportStatus(this.status,this.logWriter)
    } catch (e) {
      Yadamu.reportError(e,this.parameters,this.status,this.logWriter);
    }
    Yadamu.finalize(this.status,this.logWriter);    
  }
  
  async doExport(dbi) {
    const fi = new FileWriter(this)
    await this.pumpData(dbi,fi);
    Yadamu.finalize(this.status,this.logWriter);
  }  
  
  async uploadFile(dbi,pathToFile) {
    const importFilePath = path.resolve(pathToFile);
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size

    const startTime = new Date().getTime();
    const json = await dbi.uploadFile(importFilePath);
    const elapsedTime = new Date().getTime() - startTime;
    this.logWriter.write(`${new Date().toISOString()}[Yadamu.uploadFile()]: Processing file "${importFilePath}". Size ${fileSizeInBytes}. File Upload elapsed time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)
    return json;
  }
    
  async doServerImport(dbi,pathToFile) {
    try {
      await dbi.initialize();
      const hndl = await this.uploadFile(dbi,pathToFile);
      const log = await dbi.processFile(this.parameters.MODE,this.parameters.TOUSER,hndl)
      Yadamu.processLog(log,this.status,this.logWriter);
      await dbi.finalize();
      Yadamu.reportStatus(this.status,this.logWriter)
    } catch (e) {
      Yadamu.reportError(e,this.parameters,this.status,this.logWriter);
      await dbi.abort()
    }
    Yadamu.finalize(this.status,this.logWriter);
  }  

  async pumpData(source,target) {

    const logWriter = this.logWriter;

    try {
      await source.initialize();
      await target.initialize();
      const dbReader = await this.getDBReader(source)
      const dbWriter = await this.getDBWriter(target)  
      dbReader.setOutputStream(dbWriter);
      const copyOperation = new Promise(function (resolve,reject) {
        try {
          dbWriter.on('finish', function(){resolve()});
          dbWriter.on('error',function(err){logWriter.write(`${new Date().toISOString()}[DBWriter.error()]}: ${err.stack}\n`);reject(err)})
          dbReader.on('error',function(err){logWriter.write(`${new Date().toISOString()}[DBReader.error()]}: ${err.stack}\n`);reject(err)})
          dbReader.pipe(dbWriter);
        } catch (err) {
          logWriter.write(`${new Date().toISOString()}[Yadamu.doExport()]}: ${err}\n`);
          reject(err);
        }
      })
      
      await copyOperation;
      await source.finalize();
      await target.finalize();
      Yadamu.reportStatus(this.status,this.logWriter)
    } catch (e) {
      Yadamu.reportError(e,this.parameters,this.status,this.logWriter);
      await source.abort();
      await target.abort();
    }
    // Yadamu.finalize(this.status,this.logWriter);
  }
    
   close() {
     Yadamu.finalize(this.status,this.logWriter);
   }
}

  
module.exports.Yadamu = Yadamu;
module.exports.processLog             = Yadamu.processLog
module.exports.decomposeDataType      = Yadamu.decomposeDataType
module.exports.convertIdentifierCase  = Yadamu.convertIdentifierCase
module.exports.convertQuotedIdentifer = Yadamu.convertQuotedIdentifer
