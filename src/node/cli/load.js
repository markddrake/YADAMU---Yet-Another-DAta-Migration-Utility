"use strict"

import YadamuCLI from './yadamuCLI.js'
import YadamuLibrary from '../lib/yadamuLibrary.js'

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

export { Load as default}