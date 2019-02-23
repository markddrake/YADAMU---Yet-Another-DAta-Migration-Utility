"use strict"

const Yadamu = require('../../common/yadamuCore.js');

const OracleCore = require('./oracleCore.js');
const OracleWriter = require('./dbWriter.js');
  
async function main() {

  let conn;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = OracleCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Import',logWriter);
    conn = await OracleCore.getConnection(parameters.USERID,status);
    await OracleCore.setCurrentSchema(conn, parameters.TOUSER, status, logWriter);
    const dbWriter = new OracleWriter(conn, parameters.TOUSER, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.LOBCACHESIZE, parameters.MODE, status, logWriter);  
    status.warningsRaised = await Yadamu.importFile(parameters.FILE, dbWriter, logWriter);
    const currentUser = Yadamu.convertQuotedIdentifer(parameters.USERID.split('/')[0])
    await OracleCore.setCurrentSchema(conn, currentUser, status, logWriter);
    await OracleCore.releaseConnection(conn);					   
    Yadamu.reportStatus(status,logWriter)
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (conn !== undefined) {
      await OracleCore.releaseConnection(conn);
    }
  }
  status.importErrorMgr.close();
  Yadamu.finalize(status,logWriter);
}

main()