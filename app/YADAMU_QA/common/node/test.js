"use strict"

const YadamuCLI = require('../../../YADAMU/common/yadamuCLI.js')
const Yadamu = require('../../../YADAMU/common/yadamu.js')
const {ConfigurationFileError, CommandLineError} = require('../../../YADAMU/common/yadamuError.js');

class Test extends YadamuCLI {}

async function main() {
  
  try {
    const yadamuTest = new Test();
    try {
      await yadamuTest.doTests();
    } catch (e) {
      Test.reportError(e)
    }
    await yadamuTest.close();
  } catch (e) {
    Test.reportError(e)
  }

}

main()

module.exports = Test