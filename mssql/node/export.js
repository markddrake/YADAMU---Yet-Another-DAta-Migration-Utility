"use strict";

const Yadamu = require('../../common/yadamuCore.js');
const MsSQLCore = require('./mssqlCore.js');
const MsSQLReader = require('./dbReader.js');

async function main(){

  let pool;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = MsSQLCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Export',logWriter);
    pool = await MsSQLCore.getConnectionPool(parameters,status);
    const dbReader = new MsSQLReader(pool, parameters.OWNER, parameters.MODE, status, logWriter);
    const exportFile = await Yadamu.exportFile(parameters.FILE, dbReader, status, logWriter);
    await Yadamu.closeOutputStream(exportFile);
    await pool.close();
    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (pool !== undefined) {
      await pool.close();
    }
  }
  Yadamu.finalize(status,logWriter);
}

main();