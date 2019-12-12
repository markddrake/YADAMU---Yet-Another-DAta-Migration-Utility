"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')
const {CommandLineError} = require('./yadamuError.js');

class Export extends YadamuCLI {}

async function main() {
  
  try {
    const yadamuExport = new Export();
    try {
      await yadamuExport.doExport();
    } catch (e) {
	  Export.reportError(e)
	}
    await yadamuExport.close();
  } catch (e) {
    Export.reportError(e)
  }

}

main()

module.exports = Export