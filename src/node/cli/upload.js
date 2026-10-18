
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Upload extends YadamuCLI {}

async function main() {
  YadamuCLI.guardExitCode()

  try {
    const yadamuUpload = new Upload();
    try {
      await yadamuUpload.doUpload();
    } catch (e) {
      yadamuUpload.reportError(e)
	}
    await yadamuUpload.close();
    YadamuCLI.setExitCode(yadamuUpload.EXIT_CODE)
  } catch (e) {
    YadamuLibrary.reportError(e)
    YadamuCLI.setExitCode(YadamuCLI.getExitCode(e))
  }
  
}

main()

export { Upload as default}