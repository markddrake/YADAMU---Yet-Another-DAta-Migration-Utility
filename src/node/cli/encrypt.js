
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

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