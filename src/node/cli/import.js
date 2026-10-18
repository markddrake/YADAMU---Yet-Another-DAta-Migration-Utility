
import YadamuLibrary      from '../lib/yadamuLibrary.js'
import YadamuCLI          from './yadamuCLI.js'

class Import extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()

  try {
    const yadamuImport = new Import();
    try {
      await yadamuImport.doImport();
    } catch (e) {
      yadamuImport.reportError(e)
	}
    await yadamuImport.close();
    YadamuCLI.setExitCode(yadamuImport.EXIT_CODE)
  } catch (e) {
	YadamuLibrary.reportError(e)
	YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }

}

main()

export { Import as default}