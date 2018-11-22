"use strict" 
const fs = require('fs');
const oracledb = require('oracledb');
const path = require('path');

const Yadamu = require('../../common/yadamuCore.js');
const RowParser = require('../../common/rowParser.js');
const DBWriter = require('./dbWriter.js');
const OracleCore = require('./oracleCore.js');
 
async function setCurrentSchema(conn, schema, status, logWriter) {

  const sqlStatement = `begin :log := JSON_IMPORT.SET_CURRENT_SCHEMA(:schema); end;`;
     
  try {
    const results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 1024} , schema:schema});
    const log = JSON.parse(results.outBinds.log);
    if (log !== null) {
      Yadamu.processLog(log, status, logWriter)
    }
  } catch (e) {
    logWriter.write(`${e}\n${e.stack}\n`);
  }    
}

function processFile(conn, schema, importFilePath,batchSize,commitSize,lobCacheSize,mode,status,logWriter) {
  
  return new Promise(function (resolve,reject) {
    try {
      const dbWriter = new DBWriter(conn,schema,batchSize,commitSize,lobCacheSize,mode,status,logWriter);
      dbWriter.on('error',function(err) {logWriter.write(`${err}\n${err.stack}\n`);})
      dbWriter.on('finish', function() { resolve()});
      const rowGenerator = new RowParser(logWriter);
      rowGenerator.on('error',function(err) {logWriter.write(`${err}\n${err.stack}\n`);})
      const readStream = fs.createReadStream(importFilePath);    
      readStream.pipe(rowGenerator).pipe(dbWriter);
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

    conn = await OracleCore.doConnect(parameters.USERID,status);
    await setCurrentSchema(conn, parameters.TOUSER, status, logWriter);
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    logWriter.write(`${new Date().toISOString()}[Clarinet]: Processing file "${path.resolve(parameters.FILE)}". Size ${fileSizeInBytes} bytes.\n`)

    await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.LOBCACHESIZE, parameters.MODE, status, logWriter);
    const currentUser = Yadamu.convertQuotedIdentifer(parameters.USERID.split('/')[0])
    await setCurrentSchema(conn, currentUser, status, logWriter);
    
    OracleCore.doRelease(conn);						   

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
      OracleCore.doRelease(conn);
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