
"use strict";
 
const connectionDetails = {
  user: 'postgres',
  host: '192.168.1.250',
  database: 'postgres',
  password: null,
  port: 5432,
}

const fs = require('fs');
const {Client} = require('pg')
const copyFrom = require('pg-copy-streams').from;
const common = require('./common.js');
const PgError = require("pg-error")

const pgClient = new Client(connectionDetails);
// pgClient.connection.parseE = PgError.parse
// pgClient.connection.parseN = PgError.parse
	
async function createStagingTable(pgClient) {    	
	const sqlStatement = `create temporary table if not exists "JSON_STAGING" (data jsonb) on commit preserve rows`;					   
	await pgClient.query(sqlStatement);
}

async function loadStagingTable(pgClient,dumpFileStream) {

  return new Promise(async function(resolve,reject) {  
    const copyStatement = `copy "JSON_STAGING" from STDIN csv quote e'\x01' delimiter e'\x02'`;
    const stream = pgClient.query(copyFrom(copyStatement));
    stream.on('end',function() {resolve()})
	stream.on('error',function(err){console.log('Error'),reject(err)});
    dumpFileStream.pipe(stream);
  })  
}

async function processStagingTable(pgClient,schema) {    	
	const sqlStatement = `select jsonImport(data,$1) from "JSON_STAGING"`;
	var results = await pgClient.query(sqlStatement,[schema]);
	return results.rows[0].jsonimport;
}

async function main(){
  
  try {
    const parameters = common.processArguments(process.argv,'export');
	const schema = parameters.TOUSER;

    const dumpFilePath = parameters.FILE;	
    const dumpFile = fs.createReadStream(dumpFilePath);
    dumpFile.on('error',function(err) {console.log(err)})
	
	let logWriter = process.stdout;
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
	await pgClient.connect();
	pgClient.on('notice',function(msg){ console.log(msg)});
	
    await createStagingTable(pgClient);
    await loadStagingTable(pgClient,dumpFile);	
	const results = await processStagingTable(pgClient,schema);
	
	results.forEach( function(result) {
  	                   logWriter.write(`${new Date().toISOString()} - Table: "${result.tableName}". Rows: ${result.rowCount}. Elaspsed Time:${result.elapsedTime}ms. Throughput: ${Math.round((result.rowCount/result.elapsedTime) * 1000)} rows/s.\n`)
	})

	dumpFile.close();
	console.log('Closing Connection');
	await pgClient.end();
	console.log('Connection Closed');
  } catch (e) {
    console.log(e);
	await pgClient.end();
  }
}

main();





