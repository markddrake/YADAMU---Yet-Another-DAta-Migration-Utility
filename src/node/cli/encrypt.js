
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Encrypt extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()
  
  try {
	const yadamuEncrypt = new Encrypt();
    try {
      await yadamuEncrypt.doEncrypt();
    } catch (e) {
	  yadamuEncrypt.reportError(e)
    }
    await yadamuEncrypt.close();
    YadamuCLI.setExitCode(yadamuEncrypt.EXIT_CODE)
  } catch (e) {
    YadamuLibrary.reportError(e)
    YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { Encrypt as default}