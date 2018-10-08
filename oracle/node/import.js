
const fs = require('fs');
const common = require('./common.js');
const oracledb = require('oracledb');

async function importJSON (conn, parameters, json) {

  let sqlStatement = "BEGIN" + "\n";
  switch (parameters.MODE) {
	 case 'DDL_AND_DATA':
       sqlStatement = `${sqlStatement}  JSON_IMPORT.DATA_ONLY_MODE(FALSE);\n  JSON_IMPORT.DDL_ONLY_MODE(FALSE);\n`;
	   break;	   break
	 case 'DATA_ONLY':
       sqlStatement = `${sqlStatement}  JSON_IMPORT.DATA_ONLY_MODE(TRUE);\n  JSON_IMPORT.DDL_ONLY_MODE(FALSE);\n`;
       break;
	 case 'DDL_ONLY':
       sqlStatement = `${sqlStatement}  JSON_IMPORT.DDL_ONLY_MODE(TRUE);\n  JSON_IMPORT.DATA_ONLY_MODE(FALSE);\n`;
	   break;
  }	 
	 
  sqlStatement = `${sqlStatement}    :log := JSON_IMPORT.IMPORT_JSON(:json, :schema);\nEND;`;

  results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024}, json:json, schema:parameters.TOUSER}
                       );
  return results.outBinds.log;
};

async function main() {
    
  let conn = undefined
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;   
  
  let errorRaised = false;
  let warningRaised = false;
  let statusMsg = 'successfully';
  
  try {
    
    parameters = common.processArguments(process.argv,'import');

    if (parameters.LOGFILE) {
      logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

    if (parameters.SQLTRACE) {
      sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }

    let conn = await common.doConnect(parameters.USERID);

    const dumpFilePath = parameters.FILE;   
    const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size

    const logDML         = (parameters.LOGLEVEL && (parameters.loglevel > 0));
    const logDDL         = (parameters.LOGLEVEL && (parameters.loglevel > 1));
    const logDDLIssues   = (parameters.LOGLEVEL && (parameters.loglevel > 2));
    const logTrace       = (parameters.LOGLEVEL && (parameters.loglevel > 3));
    
    const startTime = new Date().getTime();
    const json = await common.loadTempLobFromFile(conn,dumpFilePath);
    const elapsedTime = new Date().getTime() - startTime;
    logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

    const log = await importJSON(conn,parameters,json);
    const results = JSON.parse(log);
    
    if ((parameters.DUMPLOG) && (parameters.DUMPLOG == 'TRUE')) {
      const dumpFilePath = `${parameters.FILE.substring(0,parameters.FILE.lastIndexOf('.'))}.dump.import.${new Date().toISOString().replace(/:/g,'').replace(/-/g,'')}.json`;
      fs.writeFileSync(dumpFilePath,JSON.stringify(results));
    }
       
    results.forEach(function(result) {
                      const logEntryType = Object.keys(result)[0];
                      const logEntry = result[logEntryType];
                      switch (true) {
                        case (logEntryType === "message") : 
                             logWriter.write(`${new Date().toISOString()}: ${logEntry}.\n`)
                             break;
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
    
    common.doRelease(conn);						   

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
      common.doRelease(conn);
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
   