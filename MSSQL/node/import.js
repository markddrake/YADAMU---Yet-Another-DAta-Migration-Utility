"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const sql = require('mssql');
const stream = require('stream');
	 
async function loadStagingTable(dbConn,stagingTable,dumpFilePath) {    	

  async function dropStagingTable(request,target) {
    try {
      const statement = `drop table "${target.schema === undefined ? target.table_name : target.schema + '"."' + target.tableName}"`;
      const results = await request.batch(statement)
      return results;
    } catch (e) {}
  }
	
  async function createStagingTable(request,target) {
   const statement = `create table "${target.schema === undefined ? target.table_name : target.schema + '"."' + target.tableName}" ("${target.column_name}" NVARCHAR(MAX))`;
   const results = await request.batch(statement)
   return results;
  } 

  async function initializeStagingTable(request,target) {
   const statement = `insert into "${target.schema === undefined ? target.table_name : target.schema + '"."' + target.tableName}" values ('')`;
   const results = await request.batch(statement)
   return results;
  } 
  
  class fileLoader extends stream.Writable {
	  
    constructor(request,statement,options) {
	  super(options)
	  this.request = request;
	  this.statement = statement;
	}
	   
    async _write(chunk, encoding, next) {
      try {
		const data = chunk.toString()
		var results = await this.request.input('data',sql.NVARCHAR,data).batch(this.statement);
		next(null,results);
      } catch (e) {
		next(e);
	  }  	   
	} 
  }	  

  const request = new sql.Request(dbConn);	  
  const statement = `update "${stagingTable.schema === undefined ? stagingTable.table_name : stagingTable.schema + '"."' + stagingTable.tableName}" set "${stagingTable.column_name}" .write(@data,null,null)`;
     
  const inputStream = fs.createReadStream(dumpFilePath);
  const loader = new fileLoader(request,statement);
  
  let results = await dropStagingTable(request,stagingTable);
  results = await createStagingTable(request,stagingTable);
  results = await initializeStagingTable(request,stagingTable);

  let startTime = undefined;
  return new Promise(function(resolve, reject) {
	loader.on('finish',function(chunk) {resolve(new Date().getTime() - startTime)})
	inputStream.on('error',function(err){reject(err)});
	loader.on('error',function(err){reject(err)});
	startTime = new Date().getTime();
    inputStream.pipe(loader);
  })
}

async function verifyDataLoad(dbConn,stagingTable) {    	
    const statement = `select ISJSON("${stagingTable.column_name}") "VALID_JSON" from "${stagingTable.schema === undefined ? stagingTable.table_name : stagingTable.schema + '"."' + stagingTable.tableName}"`;
	const startTime = new Date().getTime();
    const results = await dbConn.query(statement);
	console.log(`${new Date().toISOString()}: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${new Date().getTime() - startTime}ms.`);
	return results;
}

async function processStagingTable(dbConn,schema) {    	
  const request = new sql.Request(dbConn);	  
  request.input('TARGET_DATABASE',sql.VARCHAR,schema);
  const results = await request.execute('IMPORT_JSON');
  return results.recordsets[0][0];
}

async function createTargetDatabase(dbConn,schema) {    	
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await query(dbConn,sqlStatement,schema);
	return results;
}

async function main(){
	
  let parameters = undefined;
  let logWriter = process.stdout;
	
  try {

    let results = undefined;
    parameters = common.processArguments(process.argv,'import');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
	const config = {
      server    : parameters.HOSTNAME
     ,user      : parameters.USERNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
	 ,options: {
        encrypt: false // Use this if you're on Windows Azure
   	   ,requestTimeout: 24*60*60*1000
      }
    }

    const dbConn = new sql.ConnectionPool(config);
	await dbConn.connect()
	await dbConn.query(`SET QUOTED_IDENTIFIER ON`);
	
    const schema = parameters.TOUSER;
	// results = await createTargetDatabase(dbConn,schema);
	
    const dumpFilePath = parameters.FILE;	
	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size

	const stagingTable = { table_name : 'JSON_STAGING', column_name : 'DATA'}

	const startTime = new Date().getTime();
	results = await loadStagingTable(dbConn,stagingTable,dumpFilePath);
	const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

	results = await verifyDataLoad(dbConn,stagingTable);
	results = await processStagingTable(dbConn,schema);
	const jsonKey = Object.keys(results)[0];
	results = JSON.parse(results[jsonKey])
	
	results.forEach( function(logEntry) {
  	                  logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elapsed Time ${Math.round(logEntry.elapsedTime)}ms. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.\n`)
	})

	await dbConn.close();
	logWriter.write('Import operation successful.\n');
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
	if (sql !== undefined) {
      await sql.close();
	}
  }
  
  if (logWriter !== process.stdout) {
	logWriter.close();
  }  
  process.exit()
}

main();
