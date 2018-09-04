
const common = require('./common.js');
const oracledb = require('oracledb');

async function importJSON (conn, parameters, json) {
  try {
    results = await conn.execute(
                      "BEGIN" + "\n"  
					+ "  JSON_IMPORT.DATA_ONLY_MODE(FALSE);" + "\n"  
  					+ "  :log := JSON_IMPORT.IMPORT_JSON(:json, :schema); " + "\n"  
					+ "END;",
                      {  log:{ dir: oracledb.BIND_OUT, type: oracledb.CLOB}, json: json, schema: parameters.TOUSER}
		  	        );
	return results.outBinds.log;
  } catch (err) {
	console.log(err);
  }
};

async function main() {
   
   try {
     const parameters = common.processArguments(process.argv,'import');
  
     const conn = await common.doConnect(parameters.USERID);
     const json = await common.loadTempLobFromFile(conn,parameters.FILE);
     const log = await importJSON(conn,parameters,json);
     await common.writeClobToFile(log,parameters.LOGFILE);
     common.doRelease(conn);
  } catch (e) {
	console.log(e);
  }

}

main();
   