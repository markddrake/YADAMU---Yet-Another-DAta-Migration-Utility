"use strict" 
const fs = require('fs');
const path = require('path');

const Yadamu = require('../common/yadamuCore.js');
const RowParser = require('../common/rowParser.js');
const FileWriter = require('../common/fileWriter.js');

function processFile(outputStream,importFilePath,status,logWriter) {
  
  return new Promise(function (resolve,reject) {
    try {
      const fileWriter = new FileWriter(outputStream,status,logWriter);
      fileWriter.on('error',function(err){logWriter.write(`${err}\n${err.stack}\n`);})
      fileWriter.on('finish', function(){resolve(parser.checkState())});
      const parser = new RowParser(logWriter);
      parser.on('error',function(err) {logWriter.write(`${err}\n${err.stack}\n`);})
      const readStream = fs.createReadStream(importFilePath);    
      readStream.pipe(parser).pipe(fileWriter);
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      reject(e);
    }
  })
}

function processArguments(args) {


   const parameters = {}
   
   process.argv.forEach(function (arg) {
	   
	 if (arg.indexOf('=') > -1) {
       const parameterName = arg.substring(0,arg.indexOf('='));
	   const parameterValue = arg.substring(arg.indexOf('=')+1);
	    switch (parameterName.toUpperCase()) {
	      case 'SOURCE':
	        parameters.SOURCE = parameterValue;
			break;
	      case 'TARGET':
	        parameters.TARGET = parameterValue;
			break;
	      case 'LOGFILE':
		    parameters.LOGFILE = parameterValue;
			break;
	      case 'LOGLEVEL':
		    parameters.LOGLEVEL = parameterValue;
			break;
	      case 'DUMPLOG':
		    parameters.DUMPLOG = parameterValue.toUpperCase();
			break;
		  default:
		    console.log(`Unknown parameter: "${parameterName}".`)			
	   }
	 }
   })
   
   return parameters;
}  
async function main() {

  let parameters;
  let logWriter = process.stdout;
    
  let results;
  let status;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`Unhandled Rejection:\Error:`);
      logWriter.write(`${err}\n${err.stack}\n`);
    })

    parameters = processArguments(process.argv);
    status = Yadamu.getStatus(parameters,'Clone');
    
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

	const stats = fs.statSync(parameters.SOURCE)
    const fileSizeInBytes = stats.size
    logWriter.write(`${new Date().toISOString()}[Clarinet]: Processing file "${path.resolve(parameters.SOURCE)}". Size ${fileSizeInBytes} bytes.\n`)

    const exportFilePath = path.resolve(parameters.TARGET);
    const exportFile = fs.createWriteStream(exportFilePath);
    // exportFile.on('error',function(err) {console.log(err)})
    logWriter.write(`${new Date().toISOString()}[Clone]: Generating file "${exportFilePath}".\n`)

    
    status.warningsRaised = await processFile(exportFile,parameters.SOURCE, status, logWriter);
    Yadamu.reportStatus(status,logWriter)    
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Import operation failed.\n');
      logWriter.write(`${e}\n`);
    }
    else {
      console.log(`Import operation Failed:`);
      console.log(e);
    }
  }
  
  // status.importErrorMgr.close();

  if (logWriter !== process.stdout) {
    logWriter.close();
  }


}
    
main()