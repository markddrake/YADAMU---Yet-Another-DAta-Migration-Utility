"use strict";
const fs = require('fs');
const {Client} = require('pg')
const path = require('path');

const Yadamu = require('../../common/yadamuCore.js');
const RowParser = require('../../common/rowParser.js');
const DBWriter = require('./dbWriter.js');
const PostgresCore = require('./postgresCore.js');

function processFile(conn, schema, importFilePath, batchSize, commitSize, mode, status, logWriter) {
  
  return new Promise(function (resolve,reject) {
    try {
      const dbWriter = new DBWriter(conn,schema,batchSize,commitSize,mode,status,logWriter);
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
    
async function main() {

  let pgClient;
  let parameters;
  let logWriter = process.stdout;
    
  let results;
  let status;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`${new Date().toISOString()}: Unhandled Rejection\n`);
      logWriter.write(`${err}\n${err.stack}\n`);
    })
    
    parameters = PostgresCore.processArguments(process.argv);
    status = Yadamu.getStatus(parameters,'Import');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

	pgClient = await PostgresCore.getClient(parameters,logWriter,status);

	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    logWriter.write(`${new Date().toISOString()}[Clarinet]: Processing file "${path.resolve(parameters.FILE)}". Size ${fileSizeInBytes} bytes.\n`)
	
    status.warningsRaised = await processFile(pgClient, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    
	await pgClient.end();

    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Import operation failed.\n');
      logWriter.write(`${e}\n${e.stack}\n`);
    }
    else {
      console.log(`Import operation Failed:`);
      console.log(e);
    }
    if (pgClient !== undefined) {
	  await pgClient.end();
	}
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

  if (status.sqlTrace) {
    status.sqlTrace.close();
  }
}
    
main()