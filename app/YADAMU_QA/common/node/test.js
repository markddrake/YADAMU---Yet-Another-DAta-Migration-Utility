"use strict"

const YadamuCLI = require('../../../YADAMU/common/yadamuCLI.js')
const Yadamu = require('../../../YADAMU/common/yadamu.js')
const {CommandLineError} = require('../../../YADAMU/common/yadamuError.js');

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
	if (e instanceof CommandLineError) {
      console.log(e.message);
	}
	else {
      console.log(e);
	}
  }
  
}

main()

module.exports = Test