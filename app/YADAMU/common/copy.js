"use strict"
const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')
const {ConfigurationFileError, CommandLineError} = require('./yadamuError.js');

class Copy extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuCopy = new Copy();

    try {
      await yadamuCopy.doCopy();
    } catch (e) {
	  Copy.reportError(e)
    }
    await yadamuCopy.close();
  } catch (e) {
    Copy.reportError(e)
  }

}

main()

module.exports = Copy