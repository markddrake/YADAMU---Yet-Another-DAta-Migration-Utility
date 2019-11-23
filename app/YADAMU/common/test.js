"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')

class Test extends YadamuCLI {
	
}

async function main() {
  
  const operation = 'EXPORT'

  try {
    const yadamuTest = new Test();
    const yadamu = new Yadamu(operation,yadamuTest.getParameters());
    const yadamuLogger = yadamu.getYadamuLogger();
    yadamuTest.setLogger(yadamuLogger);
    await yadamuTest.doTests();
    yadamuLogger.close(); 
    yadamu.close()  
  } catch (e) {
    console.log(e);
  }
  
}

main()

module.exports = Test