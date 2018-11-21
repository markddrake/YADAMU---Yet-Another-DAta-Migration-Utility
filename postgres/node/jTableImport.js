"use strict";
const fs = require('fs');
const {Client} = require('pg')
const copyFrom = require('pg-copy-streams').from;
const path = require('path');

const Yadamu = require('../../common/yadamuCore.js');
const PostgresCore = require('./postgresCore.js');
	
async function createStagingTable(pgClient,useBinaryJSON,status) {
	let sqlStatement = `drop table if exists "JSON_STAGING"`;		
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlStatement}\n\/\n`)
    }    
	await pgClient.query(sqlStatement);
	sqlStatement = `create temporary table if not exists "JSON_STAGING" (data ${useBinaryJSON ? 'jsonb' : 'json'}) on commit preserve rows`;					   
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlStatement}\n\/\n`)
    }    
	await pgClient.query(sqlStatement);
}

async function loadStagingTable(pgClient,importFileStream,status) {

  return new Promise(async function(resolve,reject) {  
    let startTime;
    const copyStatement = `copy "JSON_STAGING" from STDIN csv quote e'\x01' delimiter e'\x02'`;
    if (status.sqlTrace) {
      status.sqlTrace.write(`${copyStatement}\n\/\n`)
    }    
    const stream = pgClient.query(copyFrom(copyStatement));
    stream.on('end',function() {resolve(new Date().getTime() - startTime)})
	stream.on('error',function(err){reject(err)});
	startTime = new Date().getTime();
    importFileStream.pipe(stream);
  })  
}

async function processStagingTable(pgClient,schema,useBinaryJSON,status) {  	
	const sqlStatement = `select ${useBinaryJSON ? 'import_jsonb' : 'import_json'}(data,$1) from "JSON_STAGING"`;
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlStatement}\n\/\n`)
    }    
	var results = await pgClient.query(sqlStatement,[schema]);
    if (useBinaryJSON) {
	  return results.rows[0].import_jsonb;  
    }
    else {
	  return results.rows[0].import_json;  
    }
}

async function main(){
  
  let pgClient = undefined
  let parameters;
  let logWriter = process.stdout;   
  let sqlTrace;
  let useBinaryJSON = true;
  let status; 
  
  try {
    parameters = PostgresCore.processArguments(process.argv,'export');
    status = Yadamu.getStatus(parameters);
	
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }
	
    const importFilePath = parameters.FILE;   
    const stats = fs.statSync(importFilePath)
    const fileSizeInBytes = stats.size
    
	pgClient = await PostgresCore.getClient(parameters,logWriter);

    let importFile = fs.createReadStream(importFilePath);
    importFile.on('error',function(err) {console.log(err)})
	
	const schema = parameters.TOUSER;
    await createStagingTable(pgClient,useBinaryJSON,status);
    let elapsedTime;
    
    try {
      elapsedTime = await loadStagingTable(pgClient,importFile,status)
    }
    catch (e) {
      if (e.code && (e.code === '54000')) {
        // Switch to Character JSON
        logWriter.write(`${new Dring()}[JSON_TABLE()]: Processing Import Data file "${importFilePath}". Size ${fileSizeInBytes}. File Upload elapsed time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)
        importFile.close();
        useBinaryJSON = false;
        await createStagingTable(pgClient,useBinaryJSON,status);
        importFile = fs.createReadStream(importFilePath,status);
        importFile.on('error',function(err) {console.log(err)})
        elapsedTime = await loadStagingTable(pgClient,useBinaryJSON,status);	
      }      
    }
    importFile.close();
    logWriter.write(`${new Date().toISOString()}: Import Data file "${importFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

	const results = await processStagingTable(pgClient,schema,useBinaryJSON,status);	
    Yadamu.processLog(results, status, logWriter)          
	await pgClient.end();
    Yadamu.reportStatus(status,logWriter)    
  } catch (e) {
    if (logWriter !== process.stdout) {
	  console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
  	  logWriter.write('Import operation failed.');
	  logWriter.write(e.stack);
    }
	else {
    	console.log('Import operation Failed.');
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
    sqlTrace.close();
  }
  
}

main()