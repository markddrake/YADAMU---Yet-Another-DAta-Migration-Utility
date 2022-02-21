"use strict"

import YadamuCLI from '../../../YADAMU/common/yadamuCLI.js'
import YadamuLibrary from '../../../YADAMU/common/yadamuLibrary.js'

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

export { Test as default}