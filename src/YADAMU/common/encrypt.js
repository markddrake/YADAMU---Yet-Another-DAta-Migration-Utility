"use strict"

import YadamuCLI from './yadamuCLI.js'
import YadamuLibrary from './yadamuLibrary.js'

class Encrypt extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuEncrypt = new Encrypt();
    try {
      await yadamuEncrypt.doEncrypt();
    } catch (e) {
	  yadamuEncrypt.reportError(e)
    }
    await yadamuEncrypt.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

export { Encrypt as default}