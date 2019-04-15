"use strict"

const Yadamu = require('../../common/yadamu.js').Yadamu;
const DBInterface = require('./oracleDBI.js');
  
async function main() {

  const yadamu = new Yadamu('Import');
  const dbi = new DBInterface(yadamu);  
  await yadamu.doImport(dbi);
  
}

main()