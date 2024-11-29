
import YadamuCLI              from './yadamuCLI.js'

class Copy extends YadamuCLI {}

export { Copy as default}

async function main() {
  
  try {
	const copy = new Copy();
    try {
      await copy.doCopy()
    } catch (e) {
	  copy.reportError(e)
    }
    await copy.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

