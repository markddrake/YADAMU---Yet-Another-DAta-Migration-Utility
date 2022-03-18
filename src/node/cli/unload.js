"use strict"

import YadamuCLI from './yadamuCLI.js'
import YadamuLibrary from '../lib/yadamuLibrary.js'

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

export { Unload as default}