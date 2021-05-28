"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class DirectLoad extends YadamuCLI {}

async function main() {

  try {
    const yadamuDirectLoad = new DirectLoad();
    try {
      await yadamuDirectLoad.doCopyBasedImport();
    } catch (e) {
      yadamuDirectLoad.reportError(e)
	}
    await yadamuDirectLoad.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

module.exports = DirectLoad