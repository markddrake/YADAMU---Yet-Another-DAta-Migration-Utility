"use strict" 

const fs = require('fs');
const path = require('path');

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
                        logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details:  ${logEntry.msg}\n${logEntry.details}${logEntry.sqlStatement}\n`)
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


function getStatus(parameters,operation) {
 
  const status = {
    operation     : operation
   ,errorRaised   : false
   ,warningRaised : false
   ,statusMsg     : 'successfully'
   ,startTime     : new Date().getTime()
  }
  
  if (parameters.SQLTRACE) {
	status.sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
  }

  if (parameters.LOGFILE) {
    status.logFileName = parameters.LOGFILE;
  }

  if (parameters.LOGLEVEL) {
    status.loglevel = parameters.LOGLEVEL;
  }
    	
  if ((parameters.DUMPLOG) && (parameters.DUMPLOG == 'TRUE')) {
     status.dumpFileName = path.basename(parameters.FILE,'.json') + '.log.json';
  }

  return status;

}

function reportStatus(status,logWriter) {

  const endTime = new Date().getTime();
      
  status.statusMsg = status.warningRaised ? 'with warnings' : status.statusMsg;
  status.statusMsg = status.errorRaised ? 'with errors'  : status.statusMsg;  
  
  logWriter.write(`${status.operation} operation completed ${status.statusMsg}. Elapsed time: ${stringifyDuration(endTime - status.startTime)}.\n`);
  if (logWriter !== process.stdout) {
    console.log(`${status.operation} operation completed ${status.statusMsg}. Elapsed time: ${stringifyDuration(endTime - status.startTime)}. See "${status.logFileName}" for details.`);  }

}
module.exports.processLog             = processLog
module.exports.decomposeDataType      = decomposeDataType
module.exports.convertQuotedIdentifer = convertQuotedIdentifer
module.exports.processValue           = processValue
module.exports.getStatus              = getStatus
module.exports.reportStatus           = reportStatus
