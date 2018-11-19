"use strict";
const fs = require('fs');
const sql = require('mssql');
const Readable = require('stream').Readable;

const Yadamu = require('../../common/yadamuCore.js');
const RowParser = require('../../common/rowParser.js');
const DBWriter = require('./dbWriter.js');
const MsSQLCore = require('./mssqlCore.js');

function processFile(conn, database, schema, importFilePath, batchSize, commitSize, mode, status, logWriter) {
  
  return new Promise(function (resolve,reject) {
    const dbWriter = new DBWriter(conn,database,schema,batchSize,commitSize,mode,status,logWriter);
    const rowGenerator = new RowParser(logWriter);
    const readStream = fs.createReadStream(importFilePath);    
    dbWriter.on('finish', function() { resolve()});
    readStream.pipe(rowGenerator).pipe(dbWriter);
  })
}
    
async function main() {

  let pool;	
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;
    
  let results;
  let status;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`Unhandled Rejection:\Error:`);
      logWriter.write(`${err}\n${err.stack}\n`);
      setTimeout((function() { console.log('Forced Exit'); return process.exit(); }), 5000);
    })
    
    parameters = MsSQLCore.processArguments(process.argv,'export');
    status = Yadamu.getStatus(parameters);

	if (parameters.LOGFILE) {
 	  logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

    const connectionDetails = {
      server    : parameters.HOSTNAME
     ,user      : parameters.USERNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
	 ,options: {
        encrypt: false // Use this if you're on Windows Azure

      }
    }
    
    pool = await new sql.ConnectionPool(connectionDetails).connect()
    const request = pool.request();
	results = await request.query(`SET QUOTED_IDENTIFIER ON`);
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
	
    await processFile(pool, parameters.DATABASE, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    
    await pool.close();
    
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
    if (pool !== undefined) {
	  await pool.close();
	}
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

  if (parameters.SQLTRACE) {
    status.sqlTrace.close();
  }
  
  // setTimeout((function() { console.log('Forced Exit'); return process.exit(); }), 5000);

}
    
main()