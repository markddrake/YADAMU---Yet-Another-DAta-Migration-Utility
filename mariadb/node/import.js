"use strict"

const Yadamu = require('../../common/yadamuCore.js');

const MariaCore = require('./mariaCore.js');
const MariaWriter = require('./dbWriter.js');
  
async function main() {

  let pool;
  let conn;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = MariaCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Import',logWriter);
    pool = await MariaCore.getConnectionPool(parameters,status,logWriter);
    conn = await MariaCore.getConnectionFromPool(pool,status,logWriter);
 	const results = await MariaCore.createTargetDatabase(conn,status,parameters.TOUSER);
    const dbWriter = new MariaWriter(conn, parameters.TOUSER, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);  
    status.warningsRaised = await Yadamu.importFile(parameters.FILE, dbWriter, logWriter);
    await conn.end();
    Yadamu.reportStatus(status,logWriter)
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

main()