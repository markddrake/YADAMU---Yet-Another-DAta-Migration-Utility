"use strict";
 
const fs = require('fs');
const {Client} = require('pg')
const copyFrom = require('pg-copy-streams').from;
const common = require('./common.js');
const PgError = require("pg-error")

// pgClient.connection.parseE = PgError.parse
// pgClient.connection.parseN = PgError.parse
	
async function createStagingTable(pgClient) {    	
	const sqlStatement = `create temporary table if not exists "JSON_STAGING" (data jsonb) on commit preserve rows`;					   
	await pgClient.query(sqlStatement);
}

async function loadStagingTable(pgClient,dumpFileStream) {

  return new Promise(async function(resolve,reject) {  
    let startTime = undefined;
    const copyStatement = `copy "JSON_STAGING" from STDIN csv quote e'\x01' delimiter e'\x02'`;
    const stream = pgClient.query(copyFrom(copyStatement));
    stream.on('end',function() {resolve(new Date().getTime() - startTime)})
	stream.on('error',function(err){reject(err)});
	startTime = new Date().getTime();
    dumpFileStream.pipe(stream);
  })  
}

async function processStagingTable(pgClient,schema) {    	
	const sqlStatement = `select import_json(data,$1) from "JSON_STAGING"`;
	var results = await pgClient.query(sqlStatement,[schema]);
	return results.rows[0].import_json;
}

async function main(){
  
  let pgClient = undefined
  let parameters = undefined
  let logWriter = process.stdout;
  
  try {
    parameters = common.processArguments(process.argv,'export');
	
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
    const connectionDetails = {
      user      : parameters.USERNAME
     ,host      : parameters.HOSTNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
    }

    const pgClient = new Client(connectionDetails);
	await pgClient.connect();
	pgClient.on('notice',function(msg){ console.log(msg)});
	
    const dumpFilePath = parameters.FILE;	
	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size
    const dumpFile = fs.createReadStream(dumpFilePath);
    dumpFile.on('error',function(err) {console.log(err)})
	
	const schema = parameters.TOUSER;
    await createStagingTable(pgClient);
    const elapsedTime = await loadStagingTable(pgClient,dumpFile);	
	dumpFile.close();
	
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

	const results = await processStagingTable(pgClient,schema);	
	results.forEach( function(result) {
  	                  logWriter.write(`${new Date().toISOString()}: Table "${result.tableName}". Rows ${result.rowCount}. Elaspsed Time${Math.round(result.elapsedTime)}ms. Throughput ${Math.round((result.rowCount/Math.round(result.elapsedTime)) * 1000)} rows/s.\n`)
	})

	await pgClient.end();
	logWriter.write('Import operation successful.');
    if (logWriter !== process.stdout) {
	  console.log(`Import operation successful: See "${parameters.LOGFILE}" for details.`);
    }
  } catch (e) {
    if (logWriter !== process.stdout) {
	  console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
  	  logWriter.write('Import operation failed.');
	  logWriter.log(e);
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
  
}

main();





