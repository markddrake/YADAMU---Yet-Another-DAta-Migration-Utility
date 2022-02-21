"use strict"

import YadamuCLI from './yadamuCLI.js'
import YadamuLibrary from '../lib/yadamuLibrary.js'

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

export { Import as default}