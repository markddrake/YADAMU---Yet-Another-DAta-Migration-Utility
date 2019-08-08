"use strict"

const Yadamu = require('../../common/yadamu.js').Yadamu;
const MongoDBI = require('./mongoDBI.js');
  
async function main() {

  const yadamu = new Yadamu('Import');
  const dbi = new MongoDBI(yadamu);  
  await yadamu.doImport(dbi);
  
}

main()