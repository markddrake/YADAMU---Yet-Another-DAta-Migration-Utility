"use strict"

const YadamuCLI = require('./yadamuCLI.js')

class Unload extends YadamuCLI {}

async function main() {

  try {
    const yadamuUnload = new Unload();
    try {
      await yadamuUnload.doUnload();
    } catch (e) {
      Unload.reportError(e)
	}
    await yadamuUnload.close();
  } catch (e) {
	Unload.reportError(e)
  }

}

main()

module.exports = Unload