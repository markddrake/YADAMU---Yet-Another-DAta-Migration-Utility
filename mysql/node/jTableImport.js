"use strict";
const fs = require('fs');
const mysql = require('mysql');

const Yadamu = require('../../common/yadamuCore.js');
const MySQLCore = require('./mysqlCore.js');

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
	 
async function createStagingTable(conn) {    	
	const sqlStatement = `CREATE TEMPORARY TABLE IF NOT EXISTS "JSON_STAGING"("DATA" JSON)`;					   
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
	const sqlStatement = `SET @RESULTS = ''; CALL IMPORT_JSON(?,@RESULTS); SELECT @RESULTS "logRecords";`;					   
	const results = await query(conn,sqlStatement,schema);
	return results;
}

async function createTargetDatabase(conn,schema) {    	
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await query(conn,sqlStatement,schema);
	return results;
}

async function main(){
	
  let conn;
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;    
  let status;

  try {

    parameters = MySQLCore.processArguments(process.argv,'export');
    status = Yadamu.getStatus(parameters);
  
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
		   ,database  : parameters.DATABASE
		   ,multipleStatements: true
    }

    conn = mysql.createConnection(connectionDetails);
	await connect(conn);
    await query(conn,'SET SESSION SQL_MODE=ANSI_QUOTES');
    await query(conn,`SET GLOBAL local_infile = 'ON'`);

    const dumpFilePath = parameters.FILE;	
	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size
	
	let results = null;
    const schema = parameters.TOUSER;
	results = await createTargetDatabase(conn,schema);
	results = await createStagingTable(conn);
	const startTime = new Date().getTime();
	results = await loadStagingTable(conn,dumpFilePath);
	const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

	results = await processStagingTable(conn,schema);
    results = results.pop();
	results = JSON.parse(results[0].logRecords)
    Yadamu.processLog(results, status, logWriter)    
	await conn.end();
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
	  await conn.end();
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