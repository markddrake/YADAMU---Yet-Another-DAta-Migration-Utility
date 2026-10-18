
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Load extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()

  try {
    const yadamuLoad = new Load();
    try {
      await yadamuLoad.doLoad();
    } catch (e) {
      yadamuLoad.reportError(e)
	}
    await yadamuLoad.close();
    YadamuCLI.setExitCode(yadamuLoad.EXIT_CODE)
  } catch (e) {
	YadamuLibrary.reportError(e)
	YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { Load as default}