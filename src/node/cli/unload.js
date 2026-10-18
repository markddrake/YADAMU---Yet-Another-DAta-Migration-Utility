
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Unload extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()

  try {
    const yadamuUnload = new Unload();
    try {
      await yadamuUnload.doUnload();
    } catch (e) {
      yadamuUnload.reportError(e)
	}
    await yadamuUnload.close();
    YadamuCLI.setExitCode(yadamuUnload.EXIT_CODE)
  } catch (e) {
	YadamuLibrary.reportError(e)
	YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { Unload as default}