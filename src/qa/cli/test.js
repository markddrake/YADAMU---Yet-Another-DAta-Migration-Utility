
import YadamuCLI       from '../../node/cli/yadamuCLI.js'
import YadamuLibrary   from '../../node/lib/yadamuLibrary.js'

class Test extends YadamuCLI {}

async function main() {
  
  try {
    const yadamuTest = new Test();
    try {
      await yadamuTest.doTests();
    } catch (e) {
	  yadamuTest.reportError(e)
    }
    await yadamuTest.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

export { Test as default}