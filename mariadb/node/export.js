"use strict";

const Yadamu = require('../../common/yadamuCore.js');
const MariaCore = require('./mariaCore.js');
const MariaReader = require('./dbReader.js');

async function main(){

  let pool;
  let conn;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = MariaCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Export',logWriter);
    pool = await MariaCore.getConnectionPool(parameters,status,logWriter);
    conn = await MariaCore.getConnectionFromPool(pool,status,logWriter);
    const dbReader = new MariaReader(conn, parameters.OWNER, parameters.MODE, status, logWriter);
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
  if (pool !== undefined) {
    await pool.end();
  }
  Yadamu.finalize(status,logWriter);
  setTimeout(function() { process.exit()},500)
}

main();