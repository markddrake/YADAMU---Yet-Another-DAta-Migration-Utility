"use strict";

const Yadamu = require('../../common/yadamuCore.js');

const MySQLCore = require('./mysqlCore.js');
const MySQLReader = require('./dbReader.js');

async function main(){

  let conn;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = MySQLCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Export',logWriter);
    conn = await MySQLCore.getConnection(parameters,status,logWriter);
    const dbReader = new MySQLReader(conn, parameters.OWNER ,parameters.MODE, status, logWriter);
    const exportFile = await Yadamu.exportFile(parameters.FILE, dbReader, status, logWriter);
    await Yadamu.closeOutputStream(exportFile);
	await conn.end();
    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (conn !== undefined) {
      await conn.end();
    }
  }
  Yadamu.finalize(status,logWriter);
}

main();


