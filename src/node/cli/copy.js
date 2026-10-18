
import YadamuLibrary      from '../lib/yadamuLibrary.js'
import YadamuCLI          from './yadamuCLI.js'

class Copy extends YadamuCLI {}

export { Copy as default}

async function main() {
  YadamuCLI.guardExitCode()
  
  try {
	const copy = new Copy();
    try {
      await copy.doCopy()
    } catch (e) {
	  copy.reportError(e)
    }
    await copy.close();
    YadamuCLI.setExitCode(copy.EXIT_CODE)
  } catch (e) {
    YadamuLibrary.reportError(e)
    YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

