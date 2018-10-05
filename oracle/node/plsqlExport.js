
const common = require('./common.js');
const oracledb = require('oracledb');

async function exportJSON (conn, parameters) {
	
  try {
	let sqlStatement = "BEGIN" + "\n";
	
	switch (parameters.MODE) {
	  case 'DDL_ONLY':
  	    sqlStatement = sqlStatement + "  JSON_EXPORT.DDL_ONLY_MODE(TRUE);" + "\n"  
	  case 'DATA_ONLY':
  	    sqlStatement = sqlStatement + "  JSON_EXPORT.DATA_ONLY_MODE(TRUE);" + "\n"  
      case 'DDL_AND_CONTENT':
  	    sqlStatement = sqlStatement + "  JSON_EXPORT.DATA_ONLY_MODE(FALSE);" + "\n"  
	}
	
    sqlStatement = sqlStatement
                 + "  :json := JSON_EXPORT.EXPORT_SCHEMA(:schema); " + "\n"  
				 + "END;"
    results = await conn.execute(
                      sqlStatement
                     ,{ schema: parameters.OWNER, json:{ dir: oracledb.BIND_OUT, type: oracledb.CLOB}}
		  	        );
	return results.outBinds.json;
  } catch (err) {
	console.log(err);
  }
}

async function getSQLStatement (conn) {
	
  try {
    const sqlStatement = "BEGIN" + "\n"
                       + "  :generatedSQL := JSON_EXPORT.DUMP_SQL_STATEMENT(); " + "\n" 
   				       + "END;"
    results = await conn.execute(
                      sqlStatement
                     ,{ generatedSQL :{ dir: oracledb.BIND_OUT, type: oracledb.CLOB}}
		  	        );
	return results.outBinds.generatedSQL;
  } catch (err) {
	console.log(err);
  }
}

async function main() {
   
   try {
     const parameters = common.processArguments(process.argv,'export');

     const conn = await common.doConnect(parameters.USERID);
     const exportContent = await exportJSON(conn, parameters);
     await common.writeClobToFile(exportContent,parameters.FILE);
	 const sqlStatement = await getSQLStatement(conn);
     await common.writeClobToFile(sqlStatement,parameters.LOGFILE);
     common.doRelease(conn);
  } catch (e) {
	console.log(e);
  }

}
main();