"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')

class Copy extends YadamuCLI {
	
}

async function main() {
  
  const operation = 'COPY'

  try {
    const yadamuCopy = new Copy();
    const yadamu = new Yadamu(operation,yadamuCopy.getParameters());
    const yadamuLogger = yadamu.getYadamuLogger();
    yadamuCopy.setLogger(yadamuLogger);
    await yadamuCopy.doCopy();
    yadamuLogger.close(); 
    yadamu.close()  
  } catch (e) {
    console.log(e);
  }
  
}

main()

module.exports = Copy