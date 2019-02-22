"use strict" 
const fs = require('fs');
const path = require('path');

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamuCore.js');
const RowParser = require('../../common/rowParser.js');
const DBWriter = require('./dbWriter.js');
const OracleCore = require('./oracleCore.js');

function processFile(conn, schema, importFilePath,batchSize,commitSize,lobCacheSize,mode,status,logWriter) {
  
  return new Promise(function (resolve,reject) {
    try {
      const dbWriter = new DBWriter(conn,schema,batchSize,commitSize,lobCacheSize,mode,status,logWriter);
      dbWriter.on('finish', function(){resolve(parser.checkState())});
      dbWriter.on('error',function(err){logWriter.write(`${new Date().toISOString()}[DBWriter.error()]}: ${err}\n`);reject(err)})
      const parser = new RowParser(logWriter);
      parser.on('error',function(err){logWriter.write(`${new Date().toISOString()}[Parser.error()]}: ${err}\n`);reject(err)})
      const readStream = fs.createReadStream(importFilePath);    
      readStream.pipe(parser).pipe(dbWriter);
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      reject(e);
    }
  })
}
    
async function main() {

  let pool;	
  let conn;
  let parameters;
  let logWriter = process.stdout;
    
  let results;
  let status;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`Unhandled Rejection:\Error:`);
      logWriter.write(`${err}\n${err.stack}\n`);
    })

    parameters = OracleCore.processArguments(process.argv);
    status = Yadamu.getStatus(parameters,'Import');
    
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

    conn = await OracleCore.getConnection(parameters.USERID,status);
    await OracleCore.setCurrentSchema(conn, parameters.TOUSER, status, logWriter);
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    logWriter.write(`${new Date().toISOString()}[Clarinet]: Processing file "${path.resolve(parameters.FILE)}". Size ${fileSizeInBytes} bytes.\n`)

    status.warningsRaised = await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.LOBCACHESIZE, parameters.MODE, status, logWriter);
    const currentUser = Yadamu.convertQuotedIdentifer(parameters.USERID.split('/')[0])
    await OracleCore.setCurrentSchema(conn, currentUser, status, logWriter);
    await OracleCore.releaseConnection(conn);					   

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
    if (conn !== undefined) {
      await OracleCore.releaseConnection(conn);
    }
  }
  
  if (status.sqlTrace) {
    status.sqlTrace.close();
  }
  
  status.importErrorMgr.close();

  if (logWriter !== process.stdout) {
    logWriter.close();
  }


}
    
main()