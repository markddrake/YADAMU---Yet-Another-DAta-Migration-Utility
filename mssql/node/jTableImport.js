"use strict";
 
const fs = require('fs');
const sql = require('mssql');
const Writable = require('stream').Writable

const MsSQLCore = require('./mssqlCore.js');
const StagingTable = require('./stagingTable');

async function verifyDataLoad(dbConn,stagingTable) {    
  const statement = `select ISJSON("${stagingTable.column_name}") "VALID_JSON" from "${stagingTable.table_name}"`;
  const startTime = new Date().getTime();
  const results = await dbConn.query(statement);
  console.log(`${new Date().toISOString()}: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${new Date().getTime() - startTime}ms.`);
  return results;
}

async function processStagingTable(dbConn,stagingTable,schema) {    

  const request = new sql.Request(dbConn);    
  let results;

  try {
    results = await request.input('TARGET_DATABASE',sql.VARCHAR,schema).execute('IMPORT_JSON');
  } catch (e) {
    if (e.code == 'ETIMEOUT') {
      results = await untilFinished(request,stagingTable);
    }
    else {
      throw(e);
    }
  }
  return results.recordset;
}

async function main(){
    
  let dbConn;
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;
  let status;
    
  try {

    let results;
    parameters = common.processArguments(process.argv,'import');
    status = Yadamu.getStatus(parameters);

    if (parameters.LOGFILE) {
     logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }
        
    const config = {
           server    : parameters.HOSTNAME
          ,user      : parameters.USERNAME
          ,database  : parameters.DATABASE
          ,password  : parameters.PASSWORD
          ,port: parameters.PORT
          ,options   : {
             encrypt: false // Use this if you're on Windows Azure
          }
          ,pool      : {
             requestTimeout : 2 * 60 * 60 * 1000
          }
        }

    dbConn = new sql.ConnectionPool(config);
    await dbConn.connect()
    await dbConn.query(`SET QUOTED_IDENTIFIER ON`);
    
    const schema = parameters.TOUSER;
    
    const importFilePath = parameters.FILE; 
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size;
 
    const startTime = new Date().getTime();
    const stagingTable = new StagingTable(dbconn, { table_name : 'JSON_STAGING', column_name : 'DATA'}, importFilePath,status); 
    results = await statingTable.uploadFile()
    const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}: Import Data file "${importFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms. Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

    results = await verifyDataLoad(dbConn,stagingTable);
    results = await processStagingTable(dbConn,stagingTable,schema);
    Yadamu.processLog(results, status, logWriter) 
    await dbConn.close();
    Yadamu.reportStatus(status,logWriter) 
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Import operation failed.\n');
      logWriter.write(e.stack);
    }
    else {
      console.log('Import operation Failed.');
      console.log(e);
    }
    if (sql !== undefined) {
      await sql.close();
    }
  } 
 
  if (logWriter !== process.stdout) {
    logWriter.close();
  } 

}

main()