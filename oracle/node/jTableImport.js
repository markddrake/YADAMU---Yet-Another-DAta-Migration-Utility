"use strict";
const fs = require('fs');
const path = require('path');

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamuCore.js');
const OracleCore = require('./oracleCore.js');

async function importJSON (conn, parameters, json) {

  let sqlStatement = "BEGIN" + "\n";
  switch (parameters.MODE) {
	 case 'DDL_AND_DATA':
       sqlStatement = `${sqlStatement}  JSON_IMPORT.DATA_ONLY_MODE(FALSE);\n  JSON_IMPORT.DDL_ONLY_MODE(FALSE);\n`;
	   break;	   break
	 case 'DATA_ONLY':
       sqlStatement = `${sqlStatement}  JSON_IMPORT.DATA_ONLY_MODE(TRUE);\n  JSON_IMPORT.DDL_ONLY_MODE(FALSE);\n`;
       break;
	 case 'DDL_ONLY':
       sqlStatement = `${sqlStatement}  JSON_IMPORT.DDL_ONLY_MODE(TRUE);\n  JSON_IMPORT.DATA_ONLY_MODE(FALSE);\n`;
	   break;
  }	 
	 
  sqlStatement = `${sqlStatement}    :log := JSON_IMPORT.IMPORT_JSON(:json, :schema);\nEND;`;

  const results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024}, json:json, schema:parameters.TOUSER});
  return results.outBinds.log;
};

async function main() {
    
  let conn = undefined
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;   
  let status;
  
  try {
    
    parameters = OracleCore.processArguments(process.argv);
    status = Yadamu.getStatus(parameters,'Import');

    if (parameters.LOGFILE) {
      logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

    let conn = await OracleCore.doConnect(parameters.USERID,status);

    const importFilePath = path.resolve(parameters.FILE);
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size

    const startTime = new Date().getTime();
    const json = await OracleCore.lobFromFile(conn,importFilePath);
    const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}[JSON_TABLE()]: Processing Import Data file "${importFilePath}". Size ${fileSizeInBytes}. File Upload elapsed time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

    const log = await importJSON(conn,parameters,json);
    const results = JSON.parse(log);
    Yadamu.processLog(results, status, logWriter)    
    OracleCore.doRelease(conn);						   
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
    if (conn !== undefined) {
      OracleCore.doRelease(conn);
    }
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

  if (parameters.SQLTRACE) {
    sqlTrace.close();
  }
}

main()