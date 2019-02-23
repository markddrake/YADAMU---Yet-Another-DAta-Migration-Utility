"use strict"

const Yadamu = require('../../common/yadamuCore.js');

const MsSQLCore = require('./mssqlCore.js');
const MsSQLWriter = require('./dbWriter.js');
  
async function main() {

  let pool;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = MsSQLCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Import',logWriter);
    pool = await MsSQLCore.getConnectionPool(parameters,status);
    const dbWriter = new MsSQLWriter(pool, parameters.DATABASE, parameters.TOUSER, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);  
    status.warningsRaised = await Yadamu.importFile(parameters.FILE, dbWriter, logWriter);
    await pool.close();
    Yadamu.reportStatus(status,logWriter)
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (pool !== undefined) {
      await pool.close();
    }
  }
  Yadamu.finalize(status,logWriter);
}

main()