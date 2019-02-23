"use strict";

const Yadamu = require('../../common/yadamuCore.js');
const PostgresCore = require('./postgresCore.js');
const PostgresReader = require('./dbReader.js');

async function main(){

  let pgClient;
  let status;
  let parameters;
  let logWriter = process.stdout;
  
  try {  
    parameters = PostgresCore.processArguments(process.argv);
    logWriter = Yadamu.getLogWriter(parameters);
    status =  Yadamu.initialize(parameters,'Export',logWriter);
	pgClient = await PostgresCore.getClient(parameters,status,logWriter);
    const dbReader = new PostgresReader(pgClient, parameters.OWNER, parameters.MODE ,status,logWriter);
    const exportFile = await Yadamu.exportFile(parameters.FILE, dbReader, status, logWriter);
    await Yadamu.closeOutputStream(exportFile);
	await pgClient.end();
    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    Yadamu.reportError(e,parameters,status,logWriter);
    if (pgClient !== undefined) {
      await pgClient.end();
    }
  }
  Yadamu.finalize(status,logWriter);
}

main()