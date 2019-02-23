"use strict" 

const fs = require('fs');
const path = require('path');

const ImportErrorManager = require('./importErrorManager.js');
const RowParser = require('./rowParser.js');
const FileWriter = require('./FileWriter.js');

function processLog(log,status,logWriter) {

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

function decomposeDataType(targetDataType) {
    
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

function convertQuotedIdentifer(parameterValue) {

  if (parameterValue.startsWith('"') && parameterValue.endsWith('"')) {
    return parameterValue.slice(1,-1);
  }
  else {
    return parameterValue.toUpperCase()
  }	
}

function processValue(parameterValue) {

  if ((parameterValue.startsWith('"') && parameterValue.endsWith('"')) && (parameterValue.indexOf('","') > 0 )) {
	let parameterValues = parameterValue.substring(1,parameterValue.length-1).split(',');
	parameterValues = parameterValues.map(function(value) {
      return convertQutotedIdentifer(value);
	})
	return parameterValues
  }
  else {
    return convertQuotedIdentifer(parameterValue);
  }
}

function stringifyDuration(duration) {
        
  let milliseconds = parseInt(duration%1000)
  let seconds = parseInt((duration/1000)%60)
  let minutes = parseInt((duration/(1000*60))%60)
  let hours = parseInt((duration/(1000*60*60))%24);

  hours = (hours < 10) ? "0" + hours : hours;
  minutes = (minutes < 10) ? "0" + minutes : minutes;
  seconds = (seconds < 10) ? "0" + seconds : seconds;

  return `${hours}:${minutes}:${seconds}.${(milliseconds + '').padStart(3,'0')}`;
}


function initialize(parameters,operation,logWriter) {
 
  const status = {
    operation     : operation
   ,errorRaised   : false
   ,warningRaised : false
   ,statusMsg     : 'successfully'
   ,startTime     : new Date().getTime()
  }
  
  process.on('unhandledRejection', function (err, p) {
    logWriter.write(`${new Date().toISOString()}: ${status.operation} operation failed with unhandled rejection ${err.stack}\n`);
    process.exit()
  })
      
  if (parameters.SQLTRACE) {
	status.sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
  }

  if (operation === 'Import') {
     status.importErrorMgr = new ImportErrorManager(parameters.FILE,1000);
  }
  
  if (parameters.LOGFILE) {
    status.logFileName = parameters.LOGFILE;
  }

  if (parameters.LOGLEVEL) {
    status.loglevel = parameters.LOGLEVEL;
  }
    	
  if (parameters.DUMPFILE) {
     status.dumpFileName = parameters.DUMPFILE
  }

  return status;

}

function finalize(status,logWriter) {

  if (logWriter !== process.stdout) {
    logWriter.close();
  }    
  
  if (status.sqlTrace) {
    status.sqlTrace.close();
  }

}

function reportStatus(status,logWriter) {

  const endTime = new Date().getTime();
      
  status.statusMsg = status.warningRaised ? 'with warnings' : status.statusMsg;
  status.statusMsg = status.errorRaised ? 'with errors'  : status.statusMsg;  
  
  logWriter.write(`${status.operation} operation completed ${status.statusMsg}. Elapsed time: ${stringifyDuration(endTime - status.startTime)}.\n`);
  if (logWriter !== process.stdout) {
    console.log(`${status.operation} operation completed ${status.statusMsg}. Elapsed time: ${stringifyDuration(endTime - status.startTime)}. See "${status.logFileName}" for details.`);  }

}

function reportError(e,parameters,status,logWriter) {
    
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

function getLogWriter(parameters) {

  if (parameters.LOGFILE) {
    return fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
  }
  return process.stdout;
  
}


function closeOutputStream(outputStream) {
        
  return new Promise(function(resolve,reject) {
    outputStream.on('finish',function() { resolve() });
    outputStream.close();
  })

}

function convertIdentifierCase(identifierCase, metadata) {
            
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
  
function mergeMetadata(targetMetadata, sourceMetadata) {
            
  for (let table of Object.keys(sourceMetadata)) {
    if (!targetMetadata.hasOwnProperty(table)) {
      Object.assign(targetMetadata, {[table] : sourceMetadata[table]})
    }     
  }             
  return targetMetadata
}

async function importFile(pathToFile, dbWriter, logWriter){

  const importFilePath = path.resolve(pathToFile);
  const stats = fs.statSync(importFilePath)
  const fileSizeInBytes = stats.size
  logWriter.write(`${new Date().toISOString()}[FileReader]: Processing file "${importFilePath}". Size ${fileSizeInBytes} bytes.\n`)

  return new Promise(function (resolve,reject) {
    try {
      dbWriter.on('finish', function(){resolve(parser.checkState())});
      dbWriter.on('error',function(err){logWriter.write(`${new Date().toISOString()}[DBWriter.error()]}: ${err}\n`);reject(err)})
      
      const parser = new RowParser(logWriter);
      const readStream = fs.createReadStream(importFilePath);    
      readStream.pipe(parser).pipe(dbWriter);
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      reject(e);
    }
  })
}

async function exportFile(pathToFile, dbReader, status, logWriter){

  const exportFilePath = path.resolve(pathToFile);
  const exportFile = fs.createWriteStream(exportFilePath);
  logWriter.write(`${new Date().toISOString()}[FileWriter]: Writing file "${exportFilePath}".\n`)
  
  return new Promise(function (resolve,reject) {
    try {
      const fileWriter = new FileWriter(exportFile,status,logWriter);
      fileWriter.on('finish', function(){resolve(exportFile)});
      fileWriter.on('error',function(err){logWriter.write(`${new Date().toISOString()}[FileWriter.error()]}: ${err}\n`);reject(err)})
      dbReader.setOutputStream(fileWriter);
      dbReader.on('error',function(err){logWriter.write(`${new Date().toISOString()}[DBReader.error()]}: ${err}\n`);reject(err)})
      dbReader.pipe(fileWriter);
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      reject(e);
    }
  })
}
  
module.exports.processLog             = processLog
module.exports.decomposeDataType      = decomposeDataType
module.exports.convertQuotedIdentifer = convertQuotedIdentifer
module.exports.processValue           = processValue
module.exports.initialize             = initialize
module.exports.getLogWriter           = getLogWriter
module.exports.importFile             = importFile
module.exports.exportFile             = exportFile
module.exports.reportStatus           = reportStatus
module.exports.reportError            = reportError
module.exports.closeOutputStream      = closeOutputStream
module.exports.finalize               = finalize
module.exports.convertIdentifierCase  = convertIdentifierCase
module.exports.mergeMetadata          = mergeMetadata
