
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Decrypt extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuDecrypt = new Decrypt();
    try {
      await yadamuDecrypt.doDecrypt();
    } catch (e) {
	  yadamuDecrypt.reportError(e)
    }
    await yadamuDecrypt.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

export { Decrypt as default}