"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')

class Export extends YadamuCLI {
	
}

async function main() {
  
  const operation = 'EXPORT'

  try {
    const yadamuExport = new Export();
    const yadamu = new Yadamu(operation,yadamuExport.getParameters());
    const yadamuLogger = yadamu.getYadamuLogger();
    yadamuExport.setLogger(yadamuLogger);
    await yadamuExport.doExport();
    yadamuLogger.close(); 
    yadamu.close()  
  } catch (e) {
    console.log(e);
  }
  
}

main()

module.exports = Export