
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class DirectLoad extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()

  try {
    const yadamuDirectLoad = new DirectLoad();
    try {
      await yadamuDirectLoad.doLoadStagedData();
    } catch (e) {
      yadamuDirectLoad.reportError(e)
	}
    await yadamuDirectLoad.close();
    YadamuCLI.setExitCode(yadamuDirectLoad.EXIT_CODE)
  } catch (e) {
	YadamuLibrary.reportError(e)
	YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { DirectLoad as default}