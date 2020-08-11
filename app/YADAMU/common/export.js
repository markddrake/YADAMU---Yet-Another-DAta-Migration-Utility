"use strict"

const YadamuCLI = require('./yadamuCLI.js')

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