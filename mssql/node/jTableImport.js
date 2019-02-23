"use strict";
 
const fs = require('fs');
const sql = require('mssql');
const Writable = require('stream').Writable
const path = require('path')

const MsSQLCore = require('./mssqlCore.js');
const StagingTable = require('./stagingTable.js');
const Yadamu = require('../../common/yadamuCore.js');

async function verifyDataLoad(request,tableSpec,status,logWriter) {    
  const statement = `select ISJSON("${tableSpec.columnName}") "VALID_JSON" from "${tableSpec.tableName}"`;
  const startTime = new Date().getTime();
  if (status.sqlTrace) {
    status.sqlTrace.write(`${statement}\n\/\n`)
  }  
  const results = await request.query(statement);
  logWriter.write(`${new Date().toISOString()}: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${new Date().getTime() - startTime}ms.\n`);
  return results;
}

async function processStagingTable(request,tableSpec,schema) {    

  const results = await request.input('TARGET_DATABASE',sql.VARCHAR,schema).execute('IMPORT_JSON');
  return results.recordset;
}

async function main(){
    
  let pool;
  let parameters;
  let logWriter = process.stdout;
  let status;
    
  try {

    let results;
    parameters = MsSQLCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status = Yadamu.initialize(parameters,'Import');
    const pool = await MsSQLCore.getConnectionPool(parameters,status);

    const importFilePath = path.resolve(parameters.FILE); 
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size;
 
    const startTime = new Date().getTime();
    const tableSpec =  { tableName : '#JSON_STAGING', columnName : 'DATA'}
    const stagingTable = new StagingTable(pool,tableSpec,importFilePath,status); 
    results = await stagingTable.uploadFile()
    
    const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}[JSON_TABLE()]: Import Data file "${importFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms. Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

    results = await verifyDataLoad(pool.request(),tableSpec,status,logWriter);
    results = await processStagingTable(pool.request(),tableSpec,parameters.TOUSERexit);
    results = results[0][Object.keys(results[0])[0]]
    results = JSON.parse(results)
    Yadamu.processLog(results, status, logWriter) 
    await pool.close();
    Yadamu.reportStatus(status,logWriter) 
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (pool !== undefined) {
      await pool.close();
    }
  } 
  Yadamu.finalize(status,logWriter);
}

main()