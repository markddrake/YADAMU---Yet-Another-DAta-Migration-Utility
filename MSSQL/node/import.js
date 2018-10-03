"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const sql = require('mssql');
const stream = require('stream');     
     
async function loadStagingTable(dbConn,stagingTable,dumpFilePath) {    	

  async function dropStagingTable(request,target) {
    try {
      const statement = `drop table if exists "${target.table_name}"`;
      const results = await request.batch(statement)
      return results;
    } catch (e) {}
  }
	
  async function createStagingTable(request,target) {
   const statement = `create table "${target.table_name}" ("${target.column_name}" NVARCHAR(MAX))`;
   const results = await request.batch(statement)
   return results;
  } 

  async function initializeStagingTable(request,target) {
   const statement = `insert into "${target.table_name}" values ('')`;
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
  const statement = `update "${stagingTable.table_name}" set "${stagingTable.column_name}" .write(@data,null,null)`;
     
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
    const statement = `select ISJSON("${stagingTable.column_name}") "VALID_JSON" from "${stagingTable.table_name}"`;
	const startTime = new Date().getTime();
    const results = await dbConn.query(statement);
	console.log(`${new Date().toISOString()}: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${new Date().getTime() - startTime}ms.`);
	return results;
}

function timeout(duration) {
  return new Promise(function(resolve, reject) {
			           setTimeout(
				         function(){
			               resolve();
   			             }, 
			             duration
			           )
	                 })
};

async function untilFinished(request,stagingTable) {

  return new Promise(async function(resolve,reject) {
	                         try {
							   while (true) {
                                 let results = await request.input('CONTEXT_ITEM',sql.NVARCHAR,'JSON_IMPORT').query(`select SESSION_CONTEXT(@CONTEXT_ITEM) IMPORT_STATE`);								 console.log(`${new Date().toISOString()}: [${JSON.stringify(results.recordsets[0][0].IMPORT_STATE)}]`)
                                 if (results.recordsets[0][0].IMPORT_STATE === 'IN-PROGRESS') {
 								   await timeout(10000);
								 }
								 else {
                                   if (results.recordsets[0][0].IMPORT_STATE === 'COMPLETED') {
								     const statement = `select  "${stagingTable.column_name}" "IMPORT_JSON" from "${stagingTable.table_name}" `;
								     results = await request.batch(statement)
									 resolve(results.recordsets[0][0])
								   }
								   else {
									  reject(`Data extraction process failed. IMPORT_STATE: ${results.recordsets[0][0].IMPORT_STATE}.`);
								   }
								}
                              }
							 } catch (e) {
                               reject(e);
							 }
                           })
                        								 
}							

async function processStagingTable(dbConn,stagingTable,schema) {    	
  const request = new sql.Request(dbConn);	  
  let results = undefined;

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
	
  let dbConn = undefined;
  let parameters = undefined;
  let sqlTrace = undefined;
  let logWriter = process.stdout;
	
  try {

    let results = undefined;
    parameters = common.processArguments(process.argv,'import');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
		
    if (parameters.SQLTRACE) {
      sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }
    
	const config = {
      server    : parameters.HOSTNAME
     ,user      : parameters.USERNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
	 ,options: {
        encrypt: false // Use this if you're on Windows Azure

      }
    }

    dbConn = new sql.ConnectionPool(config);
	await dbConn.connect()
	await dbConn.query(`SET QUOTED_IDENTIFIER ON`);
	
    const schema = parameters.TOUSER;
	
    const dumpFilePath = parameters.FILE;	
	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size
    
    const logDML         = (parameters.LOGLEVEL && (parameters.loglevel > 0));
    const logDDL         = (parameters.LOGLEVEL && (parameters.loglevel > 1));
    const logDDLIssues   = (parameters.LOGLEVEL && (parameters.loglevel > 2));
    const logTrace       = (parameters.LOGLEVEL && (parameters.loglevel > 3));
	
    const stagingTable = { table_name : 'JSON_STAGING', column_name : 'DATA'}
    
	const startTime = new Date().getTime();
	results = await loadStagingTable(dbConn,stagingTable,dumpFilePath);
	const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

	results = await verifyDataLoad(dbConn,stagingTable);
	results = await processStagingTable(dbConn,stagingTable,schema);
  
    results.forEach(function(record) {
                      const result = JSON.parse(record.LOG_ENTRY)[0];
                      const logEntryType = Object.keys(result)[0];
                      const logEntry = result[logEntryType];
                      switch (true) {
                        case (logEntryType === "dml") : 
                             logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elaspsed Time ${Math.round(logEntry.elapsedTime)}ms. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.\n`)
                             break;
                        case (logEntryType === "info") :
                             logWriter.write(`${new Date().toISOString()}[INFO]: "${JSON.stringify(logEntry)}".\n`);
                             break;
                        case (logDML && (logEntryType === "dml")) :
                             logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}".\n`)
                             break;
                        case (logDDL && (logEntryType === "ddl")) :
                             logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}".\n`) 
                             break;
                        case (logTrace && (logEntryType === "trace")) :
                             logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}".\n`)
                             break;
                        case (logEntryType === "error"):
						     switch (true) {
		                        case (logEntry.severity === 'FATAL') :
                                  errorRaised = true;
                                  logWriter.write(`${new Date().toISOString()}[${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName : ''}". Details:\n${logEntry.details}SQL:\n${logEntry.sqlStatement}\n`)
								  break
								case (logEntry.severity === 'WARNING') :
                                  warningRaised = true;
                                  logWriter.write(`${new Date().toISOString()}[${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName : ''}". Details:\n${logEntry.details}SQL:\n${logEntry.sqlStatement}\n`)
                                  break;
                                case (logDDLIssues) :
                                  logWriter.write(`${new Date().toISOString()}[${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName : ''}". Details:\n${logEntry.details}SQL:\n${logEntry.sqlStatement}\n`)
                             } 	
                      }
					  if ((parameters.SQLTRACE) && (logEntry.sqlStatement)) {
						sqlTrace.write(`${logEntry.sqlStatement}\n\/\n`)
					  }
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
