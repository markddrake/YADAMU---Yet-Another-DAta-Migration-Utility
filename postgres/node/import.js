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
    const dbWriter = new DBWriter(conn,schema,batchSize,commitSize,mode,status,logWriter);
    const rowGenerator = new RowParser(logWriter);
    const readStream = fs.createReadStream(importFilePath);    
    dbWriter.on('finish', function() { resolve()});
    readStream.pipe(rowGenerator).pipe(dbWriter);
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
    
    parameters = PostgresCore.processArguments(process.argv,'export');
    status = Yadamu.getStatus(parameters);

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

	pgClient = await PostgresCore.getClient(parameters,logWriter);

	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    logWriter.write(`${new Date().toISOString()}[Clarinet]: Processing file "${path.resolve(parameters.FILE)}". Size ${fileSizeInBytes} bytes.\n`)
	
    await processFile(pgClient, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    
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