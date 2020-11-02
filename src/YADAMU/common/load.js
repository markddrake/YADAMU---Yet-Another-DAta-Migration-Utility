"use strict"

const YadamuCLI = require('./yadamuCLI.js')

class Load extends YadamuCLI {}

async function main() {

  try {
    const yadamuLoad = new Load();
    try {
      await yadamuLoad.doLoad();
    } catch (e) {
      Load.reportError(e)
	}
    await yadamuLoad.close();
  } catch (e) {
	Load.reportError(e)
  }

}

main()

module.exports = Load