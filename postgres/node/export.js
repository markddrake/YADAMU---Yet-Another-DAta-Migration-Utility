"use strict"

const DBInterface = require('./postgresDBI.js');

async function main() {

  const yadamu = new Yadamu('Export');
  const dbi = new DBInterface(yadamu);  
  await yadamu.doExport(dbi);
  
}

main()