"use strict"

import path from 'path';
import Yadamu from './yadamu.js';
import YadamuCLI from './yadamuCLI.js'

class YadamuCmd extends YadamuCLI {}

async function main() {

  try {
    const yadamuCmd = new YadamuCmd();
    try {
      const commamd = yadamuCmd.getCommand()
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
	} catch (e) {
      this.reportError(e)
	}
    await yadamuCmd.close();
  } catch (e) {
    this.reportError(e)
  }
  
}

main();
