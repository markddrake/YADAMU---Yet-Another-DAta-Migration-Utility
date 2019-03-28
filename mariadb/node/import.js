"use strict"

const Yadamu = require('../../common/yadamu.js').Yadamu;
const DBInterface = require('./MariadbDBI.js');
  
async function main() {

  const yadamu = new Yadamu('Import');
  const dbi = new DBInterface(yadamu);  
  await yadamu.doImport(dbi,yadamu.getParameters().FILE);
  
}

main()