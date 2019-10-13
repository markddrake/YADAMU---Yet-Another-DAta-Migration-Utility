"use strict"

const Yadamu = require('../../common/yadamu.js');
const DBInterface = require('./oracleDBI.js');

async function main() {

  const yadamu = new Yadamu('Import');
  const dbi = new DBInterface(yadamu);  
  await yadamu.doServerImport(dbi,yadamu.getParameters().FILE);
  
}

main()