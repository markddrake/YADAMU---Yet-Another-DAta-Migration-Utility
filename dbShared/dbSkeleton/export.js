"use strict"

const Yadamu = require('../../common/yadamu.js').Yadamu;
const DBInterface = require('./dbInterface.js');

async function main() {

  const yadamu = new Yadamu('Export');
  const dbi = new DBInterface(yadamu);  
  await yadamu.doExport(dbi,yadamu.getParameters().FILE);
  
}

main()