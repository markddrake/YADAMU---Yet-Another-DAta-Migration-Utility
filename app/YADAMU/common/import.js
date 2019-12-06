"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')
const {CommandLineError} = require('./yadamuError.js');

class Import extends YadamuCLI {

}

async function main() {

  try {
    const yadamuImport = new Import();
    const yadamu = new Yadamu('IMPORT',yadamuImport.getParameters());
    const yadamuLogger = yadamu.getYadamuLogger();
    yadamuImport.setLogger(yadamuLogger);
    await yadamuImport.doImport();
    yadamuLogger.close(); 
    yadamu.close()  
  } catch (e) {
	if (e instanceof CommandLineError) {
      console.log(e.message);
	}
	else {
      console.log(e);
	}
  }
  
}

main()

module.exports = Import