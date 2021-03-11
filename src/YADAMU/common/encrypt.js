"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const YadamuLibrary = require('./yadamuLibrary.js')

class Encrypt extends YadamuCLI {}

async function main() {
  
  try {
	const yadamuEncrypt = new Encrypt();
    try {
      await yadamuEncrypt.doEncrypt();
    } catch (e) {
	  yadamuEncrypt.reportError(e)
    }
    await yadamuEncrypt.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

module.exports = Encrypt