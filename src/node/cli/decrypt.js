
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Decrypt extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()
  
  try {
	const yadamuDecrypt = new Decrypt();
    try {
      await yadamuDecrypt.doDecrypt();
    } catch (e) {
	  yadamuDecrypt.reportError(e)
    }
    await yadamuDecrypt.close();
    YadamuCLI.setExitCode(yadamuDecrypt.EXIT_CODE)
  } catch (e) {
    YadamuLibrary.reportError(e)
    YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { Decrypt as default}