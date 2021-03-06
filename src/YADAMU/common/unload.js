"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class Unload extends YadamuCLI {}

async function main() {

  try {
    const yadamuUnload = new Unload();
    try {
      await yadamuUnload.doUnload();
    } catch (e) {
      yadamuUnload.reportError(e)
	}
    await yadamuUnload.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Unload