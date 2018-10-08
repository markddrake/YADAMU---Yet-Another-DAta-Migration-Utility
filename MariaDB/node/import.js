"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const mariadb = require('mariadb');
	 
async function main(){

  let pool;	
  let conn;
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;
    
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
	
    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
           ,port      : parameters.PORT ? parameters.PORT : 3306
           ,database  : parameters.DATABASE
           ,multipleStatements: true
    }
    
    console.log(connectionDetails);
    
    pool = mariadb.createPool(connectionDetails);
    conn = await pool.getConnection();
    await conn.query(`SET SESSION SQL_MODE=ANSI_QUOTES`);
	
    
    const dumpFilePath = parameters.FILE;	
	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size
	
    const logDML         = (parameters.LOGLEVEL && (parameters.loglevel > 0));
    const logDDL         = (parameters.LOGLEVEL && (parameters.loglevel > 1));
    const logDDLIssues   = (parameters.LOGLEVEL && (parameters.loglevel > 2));
    const logTrace       = (parameters.LOGLEVEL && (parameters.loglevel > 3));
    
	let results = null;
    const schema = parameters.TOUSER;
    results = await conn.query(`CREATE DATABASE IF NOT EXISTS "${schema}"`);	
    results = await conn.query(`CREATE TEMPORARY TABLE IF NOT EXISTS "JSON_STAGING"("DATA" JSON)`);
	const startTime = new Date().getTime();
    results = await conn.query(`insert into "JSON_STAGING"("DATA") values (?)`,[fs.createReadStream(dumpFilePath)]);
	const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

    
	results = await conn.query(`set @RESULTS = ''; call IMPORT_JSON(?,@RESULTS); select @RESULTS "logRecords";`,[schema]);
    results = results.pop();
	results = JSON.parse(results[0].logRecords)

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
                                  logWriter.write(`${new Date().toISOString()}[${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName : ''}". Details:\n${logEntry.code} - ${logEntry.msg}\nSQL:${logEntry.sqlStatement}\n`)
								  break
								case (logEntry.severity === 'WARNING') :
                                  warningRaised = true;
                                  logWriter.write(`${new Date().toISOString()}[${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName : ''}". Details:\n${logEntry.code} - ${logEntry.msg}}\nSQL:${logEntry.sqlStatement}\n`)
                                  break;
                                case (logDDLIssues) :
                                  logWriter.write(`${new Date().toISOString()}[${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName : ''}". Details:\n${logEntry.code} - ${logEntry.msg}\nSQL:${logEntry.sqlStatement}\n`)
                             } 	
                      }
					  if ((parameters.SQLTRACE) && (logEntry.sqlStatement)) {
						sqlTrace.write(`${logEntry.sqlStatement}\n\/\n`)
					  }
    })
    
	await conn.end();
    await pool.end();

    statusMsg = warningRaised ? 'with warnings' : statusMsg;
    statusMsg = errorRaised ? 'with errors'  : statusMsg;
        
    logWriter.write(`Import operation completed ${statusMsg}.`);
    if (logWriter !== process.stdout) {
      console.log(`Import operation completed ${statusMsg}: See "${parameters.LOGFILE}" for details.`);
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
	  await conn.end();
	}
    if (pool !== undefined) {
	  await pool.end();
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
