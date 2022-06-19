
import YadamuLibrary      from '../lib/yadamuLibrary.js'

import YadamuCLI          from './yadamuCLI.js'

class Copy extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuCopy = new Copy();
    try {
      await yadamuCopy.doCopy();
    } catch (e) {
	  yadamuCopy.reportError(e)
    }
    await yadamuCopy.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

export { Copy as default}