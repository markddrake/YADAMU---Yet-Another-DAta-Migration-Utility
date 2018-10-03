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
  let parameters = undefined;
  let logWriter = process.stdout;   
  let sqlTrace = undefined;
  
  let errorRaised = false;
  let warningRaised = false;
  let statusMsg = 'successfully';
  
  try {
    parameters = common.processArguments(process.argv,'export');
	
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
    if (parameters.SQLTRACE) {
      sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }

    const dumpFilePath = parameters.FILE;   
    const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size

    const logDML         = (parameters.LOGLEVEL && (parameters.loglevel > 0));
    const logDDL         = (parameters.LOGLEVEL && (parameters.loglevel > 1));
    const logDDLIssues   = (parameters.LOGLEVEL && (parameters.loglevel > 2));
    const logTrace       = (parameters.LOGLEVEL && (parameters.loglevel > 3));
    
    const connectionDetails = {
      user      : parameters.USERNAME
     ,host      : parameters.HOSTNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
    }

    const pgClient = new Client(connectionDetails);
	await pgClient.connect();
	pgClient.on('notice',function(n){ 
	                        const notice = JSON.parse(JSON.stringify(n));
							if (notice.code != '42P07') /* Duplicate Table */ {
							  console.log(n);
							}
	})

    const dumpFile = fs.createReadStream(dumpFilePath);
    dumpFile.on('error',function(err) {console.log(err)})
	
	const schema = parameters.TOUSER;
    await createStagingTable(pgClient);
    const elapsedTime = await loadStagingTable(pgClient,dumpFile);	
	dumpFile.close();
	
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

	const results = await processStagingTable(pgClient,schema);	

    if ((parameters.DUMPLOG) && (parameters.DUMPLOG == 'TRUE')) {
      const dumpFilePath = `${parameters.FILE.substring(0,parameters.FILE.lastIndexOf('.'))}.dump.import.${new Date().toISOString().replace(/:/g,'').replace(/-/g,'')}.json`;
      fs.writeFileSync(dumpFilePath,JSON.stringify(results));
    }
       
    results.forEach(function(result) {
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

	await pgClient.end();
    statusMsg = warningRaised ? 'with warnings' : statusMsg;
    statusMsg = errorRaised ? 'with errors'  : statusMsg;
    
    logWriter.write(`Import operation completed ${statusMsg}.`);
    if (logWriter !== process.stdout) {
      console.log(`Import operation completed ${statusMsg}: See "${parameters.LOGFILE}" for details.`);
    }
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
  if (parameters.SQLTRACE) {
    sqlTrace.close();
  }
  
}

main();





