"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class Export extends YadamuCLI {}

async function main() {

  try {
    const yadamuExport = new Export();
    try {
      await yadamuExport.doExport();
    } catch (e) {
	  yadamuExport.reportError(e)
	}
    await yadamuExport.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Export