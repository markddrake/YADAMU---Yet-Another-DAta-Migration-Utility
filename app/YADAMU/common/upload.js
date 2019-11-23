"use strict"

const YadamuCLI = require('./yadamuCLI.js')
const Yadamu = require('./yadamu.js')

class Upload extends YadamuCLI {
}

async function main() {

  try {
    const yadamuUpload = new Upload();
    const yadamu = new Yadamu('UPLOAD',yadamuUpload.getParameters());
    const yadamuLogger = yadamu.getYadamuLogger();
    yadamuUpload.setLogger(yadamuLogger);
    await yadamuUpload.doUpload();
    yadamuLogger.close(); 
    yadamu.close()  
  } catch (e) {
    console.log(e);
  }
  
}

main()

module.exports = Upload





