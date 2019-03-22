"use strict"

const Yadamu = require('../../common/yadamu.js').Yadamu;
  
async function main() {

  const yadamu = new Yadamu('Import');
  await yadamu.cloneFile(yadamu.getParameters().SOURCE);
  
}

main()