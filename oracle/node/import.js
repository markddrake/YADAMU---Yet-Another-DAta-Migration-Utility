
const fs = require('fs');
const common = require('./common.js');
const oracledb = require('oracledb');

async function importJSON (conn, parameters, json) {

  results = await conn.execute(
                         "BEGIN" + "\n"  
					   + "  JSON_IMPORT.DATA_ONLY_MODE(FALSE);" + "\n"  
  					   + "  :log := JSON_IMPORT.IMPORT_JSON(:json, :schema); " + "\n"  
					   + "END;",
                         {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 64 * 1024}, json:json, schema:parameters.TOUSER}
		  	           );
  return results.outBinds.log;
};

async function main() {
	
  let conn = undefined
  let parameters = undefined;
  let logWriter = process.stdout;   
  
  try {
    parameters = common.processArguments(process.argv,'import');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

    let conn = await common.doConnect(parameters.USERID);

    const dumpFilePath = parameters.FILE;	
  	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size
	 
    const startTime = new Date().getTime();
    const json = await common.loadTempLobFromFile(conn,dumpFilePath);
	const elapsedTime = new Date().getTime() - startTime;
	logWriter.write(`${new Date().toISOString()}: Import Data file "${dumpFilePath}". Size ${fileSizeInBytes}. Elapsed Time ${elapsedTime}ms.  Throughput ${Math.round((fileSizeInBytes/elapsedTime) * 1000)} bytes/s.\n`)

    const log = await importJSON(conn,parameters,json);
	const results = JSON.parse(log);
    results.forEach(function(result) {
		              if (result.DML) {
						const logEntry = result.DML;
   	                    logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elaspsed Time ${Math.round(logEntry.elapsedTime)}ms. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.\n`)
					  }
	})
	
    // TODO : Add Support for Dumping SQL..
	
    common.doRelease(conn);

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
      common.doRelease(conn);
	}
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

}

main();
   