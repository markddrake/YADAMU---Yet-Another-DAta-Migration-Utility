"use strict"

const Yadamu = require('../../common/yadamuCore.js');

const PostgresCore = require('./postgresCore.js');
const PostgresWriter = require('./dbWriter.js');
  
async function main() {

  let pgClient;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = PostgresCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Import',logWriter);
	pgClient = await PostgresCore.getClient(parameters,status,logWriter);
    const dbWriter = new PostgresWriter(pgClient, parameters.TOUSER, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);  
    status.warningsRaised = await Yadamu.importFile(parameters.FILE, dbWriter, logWriter);
    await pgClient.end();
    Yadamu.reportStatus(status,logWriter)
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (pgClient !== undefined) {
      await pgClient.end();
    }
  }
  Yadamu.finalize(status,logWriter);
}

main()