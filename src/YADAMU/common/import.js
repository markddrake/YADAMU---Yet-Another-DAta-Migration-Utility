"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class Import extends YadamuCLI {}

async function main() {

  try {
    const yadamuImport = new Import();
    try {
      await yadamuImport.doImport();
    } catch (e) {
      yadamuImport.reportError(e)
	}
    await yadamuImport.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Import