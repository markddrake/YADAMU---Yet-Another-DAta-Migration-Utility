"use strict" 
const fs = require('fs');
const path = require('path');

const Yadamu = require('../common/yadamuCore.js');
const PostgresCore = require('../postgres/node/postgresCore.js')
const DBWriter = require('../postgres/node/dbWriter.js');
const OracleCore = require('../oracle/node/oracleCore.js')
const DBReader = require('../oracle/node/dbReader.js');

const oracleConnection = { 
  user           : "system"
, password       : "oracle"
, connectString  : "ORCL18C"
}

const postgresConnection = {
  USERNAME : "postgres"
, HOSTNAME : "192.168.1.250"
, DATABASE : "postgres"
, PASSWORD : null
, PORT     : 5432
}

function cloneSchema(sourceConnection,targetConnection,schema,batchSize,commitSize,mode,status,logWriter) {
  
  return new Promise(function (resolve,reject) {
    try {
      const dbWriter = new DBWriter(targetConnection,schema,batchSize,commitSize,mode,status,logWriter);
      dbWriter.on('finish', function(){resolve(status)});
      dbWriter.on('error',function(err){logWriter.write(`${err}\n${err.stack}\n`);})

      const dbReader = new DBReader(sourceConnection,schema,dbWriter,mode,status,logWriter);
      dbReader.on('error',function(err){logWriter.write(`${err}\n${err.stack}\n`);})
      
      dbReader.pipe(dbWriter);
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      reject(e);
    }
  })
}
  
async function main() {

  let logWriter = process.stdout;
  const parameters = {LOGFILE : 'c:\\temp\\cloneSchema.log'};
    
  let results;
  let status;
  
  let oracleConn;
  let pgClient;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`Unhandled Rejection:\Error:`);
      logWriter.write(`${err}\n${err.stack}\n`);
    })

    status = Yadamu.getStatus(parameters,'CloneSchema')
    logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    
	pgClient = await PostgresCore.getClient(postgresConnection,logWriter,status);
    oracleConn = await OracleCore.getConnection(oracleConnection,status)
    
	status = await cloneSchema(oracleConn, pgClient, 'HR', 1000, 1000, 'DATA_ONLY', status, logWriter);
    await OracleCore.releaseConnection(oracleConn);
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
    if (oracleConn !== undefined) {
      await OracleCore.releaseConnection(oracleConn);
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