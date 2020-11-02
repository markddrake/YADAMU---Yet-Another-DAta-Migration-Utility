"use strict"

const YadamuCLI = require('./yadamuCLI.js')

class Upload extends YadamuCLI {}

async function main() {

  try {
    const yadamuUpload = new Upload();
    try {
      await yadamuUpload.doUpload();
    } catch (e) {
      Upload.reportError(e)
	}
    await yadamuUpload.close()  
  } catch (e) {
    Upload.reportError(e)
  }
  
}

main()

module.exports = Upload





