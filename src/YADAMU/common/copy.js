"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class Copy extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuCopy = new Copy();
    try {
      await yadamuCopy.doCopy();
    } catch (e) {
	  yadamuCopy.reportError(e)
    }
    await yadamuCopy.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Copy