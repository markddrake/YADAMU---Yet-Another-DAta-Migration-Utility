"use strict"

const path = require('path');
const Yadamu = require('./yadamu.js');
const YadamuCLI = require('./yadamuCLI.js')

class YadamuCmd extends YadamuCLI {

}

async function main() {

  try {
    const yadamuCmd = new YadamuCmd();
    const commamd = yadamuCmd.getCommand()
    const yadamu = new Yadamu(commamd,yadamuCmd.getParameters());
    const yadamuLogger = yadamu.getYadamuLogger();
	// First line offset 1 character to right when writing to console in Electron
    yadamuCmd.setLogger(yadamuLogger);

    console.log(commamd);
    switch (commamd) {
      case 'IMPORT':
	    await yadamuCmd.doImport();
     	break;
      case 'UPLOAD':
	    await yadamuCmd.doUpload();
     	break;
	  case 'EXPORT':
	    await yadamuCmd.doExport();
	    break;
	  case 'COPY':
  	    await yadamuCmd.executeJobs();
	    break;
	  case 'TEST':
  	    await yadamuCmd.doTests();
	    break;
	}
    yadamuLogger.close(); 
    yadamu.close()  
  } catch (e) {
    console.log(e);
  }
  
}

main();
