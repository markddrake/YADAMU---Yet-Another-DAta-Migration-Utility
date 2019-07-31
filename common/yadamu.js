"use strict"

const fs = require('fs');
const path = require('path');
  
const FileReader = require('../file/node/fileReader.js');
const FileWriter = require('../file/node/fileWriter.js');
const DBReader = require('./dbReader.js');
const DBWriter = require('./dbWriter.js');
const YadamuLogger = require('./yadamuLogger.js');

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

  static async finalize(status,yadamuLogger) {

    await yadamuLogger.close();

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
  
  static reportStatus(status,yadamuLogger) {

    const endTime = new Date().getTime();
      
    status.statusMsg = status.warningRaised === true ? 'with warnings' : status.statusMsg;
    status.statusMsg = status.errorRaised === true ? 'with errors'  : status.statusMsg;  
  
    yadamuLogger.log([`${this.name}`,`"${status.operation}"`],`Operation completed ${status.statusMsg}. Elapsed time: ${Yadamu.stringifyDuration(endTime - status.startTime)}.`);
    if (!yadamuLogger.loggingToConsole()) {
      console.log(`[${this.name}][${status.operation}]: Operation completed ${status.statusMsg}. Elapsed time: ${Yadamu.stringifyDuration(endTime - status.startTime)}. See "${status.logFileName}" for details.`);  
    }
  }

  static reportError(e,parameters,status,yadamuLogger) {
    
    if (!yadamuLogger.loggingToConsole()) {
      yadamuLogger.logException([`${this.name}`,`"${status.operation}"`],e);
      console.log(`[ERROR][${this.name}][${status.operation}]: Operation failed: See "${parameters.LOGFILE ? parameters.LOGFILE  : 'above'}" for details.`);
    }
    else {
      console.log(`[ERROR][${this.name}][${status.operation}]: Operation Failed:`);
      console.log(e);
    }
  }
  
  constructor(operation) {
    this.yadamuLogger = process.stdout;
    this.parameters = this.processArguments();
    
    this.status = {
      operation     : operation
     ,errorRaised   : false
     ,warningRaised : false
     ,statusMsg     : 'successfully'
     ,startTime     : new Date().getTime()
    }
  
    this.yadamuLogger = this.setYadamuLogger(this.parameters,this.status);

    process.on('unhandledRejection', (err, p) => {
      this.yadamuLogger.logException([`${this.constructor.name}.onUnhandledRejection()`,`"${this.status.operation}"`],err);
      Yadamu.reportStatus(this.status,this.yadamuLogger)
      process.exit()
    })
      
    if (this.parameters.SQLTRACE) {
	  this.status.sqlTrace = fs.createWriteStream(this.parameters.SQLTRACE);
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
    
    this.status.showInfoMsgs = (this.status.loglevel && (this.status.loglevel > 2));  
  }
    
  getParameters() {
    return Object.assign({},this.parameters)
  }

  getStatus() {
    return this.status
  }

  getYadamuLogger() {
    return this.yadamuLogger
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

  setYadamuLogger(parameters) {

    if (this.parameters.LOGFILE) {
      return new YadamuLogger(fs.createWriteStream(this.parameters.LOGFILE,{flags : "a"}),this.status);
    }
    return new YadamuLogger(process.stdout,this.status);
  
  }
  
  closeFile(outputStream) {
        
    return new Promise(function(resolve,reject) {
      outputStream.on('finish',function() { resolve() });
      outputStream.close();
    })

  }

  getDBReader(dbi) {
    const dbReader = new DBReader(dbi, dbi.parameters.MODE, this.status, this.yadamuLogger);
    return dbReader;
  }
  
  getDBWriter(dbi) {
    const dbWriter = new DBWriter(dbi, dbi.parameters.MODE, this.status, this.yadamuLogger);
    return dbWriter;
  }

  async pumpData(source,target) {
      
    if ((source.isDatabase() === true) && (source.parameters.OWNER === undefined)) {
      throw new Error('Missing mandatory parameter OWNER');
    }

    if ((target.isDatabase() === true) && (target.parameters.TOUSER === undefined)) {
      throw new Error('Missing mandatory parameter TOUSER');
    }
    

    let timings = {}
    const self = this
 
    try {
      await source.initialize();
      await target.initialize();
      const dbReader = await this.getDBReader(source)
      const dbWriter = await this.getDBWriter(target)  
      dbReader.setOutputStream(dbWriter);
      const copyOperation = new Promise(function (resolve,reject) {
        try {
          const reader = dbReader.getReader();
          dbWriter.on('finish', function(){resolve()})
          dbWriter.on('error',function(err){self.yadamuLogger.logException([`${dbWriter.constructor.name}.onError()`],err);reject(err)})
          reader.on('error',function(err){self.yadamuLogger.logException([`${reader.constructor.name}.onError()`],err);reject(err)})
          reader.pipe(dbWriter);
        } catch (err) {
          self.yadamuLogger.logException([`${self.constructor.name}.onError()`],err)
          reject(err);
        }
      })
      
      await copyOperation;
      timings = dbWriter.getTimings();
      await source.finalize();
      await target.finalize();
      Yadamu.reportStatus(this.status,this.yadamuLogger)
      return timings
    } catch (e) {
      Yadamu.reportError(e,this.parameters,this.status,this.yadamuLogger);
      await source.abort();
      await target.abort();
    }
    // Yadamu.finalize(this.status,this.yadamuLogger);
  }
  
  async doImport(dbi) {
    const fileReader = new FileReader(this)
    const timings = await this.pumpData(fileReader,dbi);
    Yadamu.finalize(this.status,this.yadamuLogger);
    return timings
  }  
 
  async doExport(dbi) {
    const fileWriter = new FileWriter(this)
    const timings = await this.pumpData(dbi,fileWriter);
    Yadamu.finalize(this.status,this.yadamuLogger);
    return timings
  }  
  
  async cloneFile(pathToFile) {

    const fileReader = new FileReader(this)
    const fileWriter = new FileReader(this)
    const timings = await this.pumpData(fileReader,FileWriter);
    Yadamu.finalize(this.status,this.yadamuLogger);
    return timings
  }
  
  async uploadFile(dbi,importFilePath) {
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size

    const startTime = new Date().getTime();
    const json = await dbi.uploadFile(importFilePath);
    const elapsedTime = new Date().getTime() - startTime;
    this.yadamuLogger.log([`${this.constructor.name}.uploadFile()`],`Processing file "${importFilePath}". Size ${fileSizeInBytes}. File Upload elapsed time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.`)
    return json;
  }
    
  getTimings(log) {
      
     const timings = {}
     log.forEach(function(entry) {
       switch (Object.keys(entry)[0]) {
         case 'dml' :
           timings[entry.dml.tableName] = {rowCount : entry.dml.rowCount, insertMode : "SQL", elapsedTime : entry.dml.elapsedTime + "ms", throughput: Math.round((entry.dml.rowCount/Math.round(entry.dml.elapsedTime)) * 1000).toString() + "/s"}
           break;
         case 'error' :
           timings[entry.error.tableName] = {rowCount : -1, insertMode : "SQL", elapsedTime : "NaN", throughput: "NaN"}
           break;
         default:
       }
     },this)
     return timings
  }

  async doServerImport(dbi) {
    let timings = {}
    const pathToFile = dbi.parameters.FILE;
    try {
      await dbi.initialize();
      const hndl = await this.uploadFile(dbi,pathToFile);
      const log = await dbi.processFile(hndl)
      timings = this.getTimings(log);
      await dbi.finalize();
      Yadamu.reportStatus(this.status,this.yadamuLogger)
    } catch (e) {
      Yadamu.reportError(e,this.parameters,this.status,this.yadamuLogger);
      await dbi.abort()
    }
    Yadamu.finalize(this.status,this.yadamuLogger);
    return timings;
  } 

   close() {
     Yadamu.finalize(this.status,this.yadamuLogger);
   }
}  
     
module.exports.Yadamu = Yadamu;
module.exports.convertIdentifierCase  = Yadamu.convertIdentifierCase
