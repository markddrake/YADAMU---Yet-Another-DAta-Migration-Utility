"use strict"

const Yadamu = require('../../common/yadamuCore.js');

const MySQLCore = require('./mysqlCore.js');
const MySQLWriter = require('./dbWriter.js');
  
async function main() {

  let conn;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = MySQLCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Import',logWriter);
    conn = await MySQLCore.getConnection(parameters,status,logWriter);
 	const results = await MySQLCore.createTargetDatabase(conn,status,parameters.TOUSER);
    const dbWriter = new MySQLWriter(conn, parameters.TOUSER, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);  
    status.warningsRaised = await Yadamu.importFile(parameters.FILE, dbWriter, logWriter);
    await conn.end();
    Yadamu.reportStatus(status,logWriter)
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (conn !== undefined) {
      await conn.end();
    }
  }
  Yadamu.finalize(status,logWriter);
}

main()