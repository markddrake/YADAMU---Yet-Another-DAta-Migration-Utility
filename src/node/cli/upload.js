"use strict"

import YadamuCLI from './yadamuCLI.js'
import YadamuLibrary from '../lib/yadamuLibrary.js'

class Upload extends YadamuCLI {}

async function main() {

  try {
    const yadamuUpload = new Upload();
    try {
      await yadamuUpload.doUpload();
    } catch (e) {
      yadamuUpload.reportError(e)
	}
    await yadamuUpload.close()  
  } catch (e) {
    YadamuLibrary.reportError(e)
  }
  
}

main()

export { Upload as default}