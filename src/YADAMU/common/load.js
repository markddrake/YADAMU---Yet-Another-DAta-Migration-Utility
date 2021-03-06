"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class Load extends YadamuCLI {}

async function main() {

  try {
    const yadamuLoad = new Load();
    try {
      await yadamuLoad.doLoad();
    } catch (e) {
      yadamuLoad.reportError(e)
	}
    await yadamuLoad.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Load