"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class Decrypt extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuDecrypt = new Decrypt();
    try {
      await yadamuDecrypt.doDecrypt();
    } catch (e) {
	  yadamuDecrypt.reportError(e)
    }
    await yadamuDecrypt.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Decrypt