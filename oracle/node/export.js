"use strict";

const Yadamu = require('../../common/yadamuCore.js');
const OracleCore = require('./oracleCore.js');
const OracleReader = require('./dbReader.js');

async function main(){

  let conn;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = OracleCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Export',logWriter);
    conn = await OracleCore.getConnection(parameters.USERID,status);
    const dbReader = new OracleReader(conn, parameters.OWNER, parameters.MODE, status, logWriter);
    const exportFile = await Yadamu.exportFile(parameters.FILE, dbReader, status, logWriter);
    await Yadamu.closeOutputStream(exportFile);
    await OracleCore.releaseConnection(conn);
    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (conn !== undefined) {
      await OracleCore.releaseConnection(conn);
    }
  }
  Yadamu.finalize(status,logWriter);
}

main();