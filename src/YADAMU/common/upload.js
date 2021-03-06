"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

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

module.exports = Upload





