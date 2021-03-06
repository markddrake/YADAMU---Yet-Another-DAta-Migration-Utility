"use strict"

const YadamuCLI = require('../../../YADAMU/common/yadamuCLI.js')
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js')

class Test extends YadamuCLI {}

async function main() {
  
  try {
    const yadamuTest = new Test();
    try {
      await yadamuTest.doTests();
    } catch (e) {
	  yadamuTest.reportError(e)
    }
    await yadamuTest.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Test