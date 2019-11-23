"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')

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
    console.log(e);
  }
  
}

main()

module.exports = Import