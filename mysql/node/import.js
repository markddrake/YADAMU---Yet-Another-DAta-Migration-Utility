"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const mysql = require('mysql');
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;

function connect(conn) {
	
  return new Promise(function(resolve,reject) {
	                   conn.connect(function(err) {
		                              if (err) {
		                                reject(err);
	                                  }
  			                          resolve();
                                    })
				    })
}	
	  
function query(conn,sqlQuery,args) {
	
  return new Promise(function(resolve,reject) {
	                   conn.query(sqlQuery,args,function(err,rows,fields) {
		                                     if (err) {
		                                       reject(err);
	                                         }
											 resolve(rows);
                                           })
                     })
}  
	 
async function createStagingTable(conn,schema) {    	
	const sqlStatement = `CREATE TEMPORARY TABLE IF NOT EXISTS "JSON_STAGING"("DATA" JSON)`;					   
	const results = await query(conn,sqlStatement);
	return results;
}

async function createLoggingTable(conn) {    	
	const sqlStatement = `CREATE TABLE IF NOT EXISTS "IMPORT_LOGGING"("LOG" TEXT)`;					   
	const results = await query(conn,sqlStatement);
	return results;
}

async function loadStagingTable(conn,dumpfilePath) {    	
	const sqlStatement = `LOAD DATA LOCAL INFILE '${dumpfilePath}' INTO TABLE "JSON_STAGING" FIELDS ESCAPED BY ''`;					   
	const results = await query(conn,sqlStatement);
	return results;
}

async function verifyDataLoad(conn) {    	
	const sqlStatement = `SELECT COUNT(*) FROM "JSON_STAGING"`;				
	const results = await query(conn,sqlStatement);
	return results;
}

async function processStagingTable(conn,schema) {    	
	const sqlStatement = `CALL IMPORT_JSON(?)`;					   
	const results = await query(conn,sqlStatement,schema);
	return results;
}

async function fetchLogRecords(conn,schema) {    	
	const sqlStatement = `select "LOG" from "IMPORT_LOGGING"`;					   
	const results = await query(conn,sqlStatement);
	return results;
}

async function createTargetDatabase(conn,schema) {    	
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await query(conn,sqlStatement,schema);
	return results;
}

async function main(){
	
  let conn = undefined;
  let parameters = undefined;
  let logWriter = process.stdout;
  
  try {

    parameters = common.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
		   ,database  : parameters.DATABASE
    }

    conn = mysql.createConnection(connectionDetails);
	await connect(conn);
    await query(conn,'SET SESSION SQL_MODE=ANSI_QUOTES');
	
    const dumpFilePath = parameters.FILE;	
	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size
	
	let results = null;
    const schema = parameters.TOUSER;
	results = await createTargetDatabase(conn,schema);
	results = await createLoggingTable(conn);
	results = await createStagingTable(conn);
	const startTime = new Date().getTime();
	results = await loadStagingTable(conn,dumpFilePath);
	const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

	results = await processStagingTable(conn,schema);
    results = await fetchLogRecords(conn);
	results.forEach( function(result) {
		              const logEntry = JSON.parse(result.LOG);
  	                  logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elaspsed Time ${Math.round(logEntry.elapsedTime)}ms. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.\n`)
	})

	await conn.end();
	logWriter.write('Import operation successful.');
    if (logWriter !== process.stdout) {
	  console.log(`Import operation successful: See "${parameters.LOGFILE}" for details.`);
    }
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
      const results = await fetchLogRecords(conn);
	  console.log(results);
	  await conn.end();
	}
  }
  
  if (logWriter !== process.stdout) {
	logWriter.close();
  }
}

main();
