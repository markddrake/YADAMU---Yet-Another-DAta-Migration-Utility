
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class DirectLoad extends YadamuCLI {}

async function main() {

  try {
    const yadamuDirectLoad = new DirectLoad();
    try {
      await yadamuDirectLoad.doCopyStagedData();
    } catch (e) {
      yadamuDirectLoad.reportError(e)
	}
    await yadamuDirectLoad.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

export { DirectLoad as default}