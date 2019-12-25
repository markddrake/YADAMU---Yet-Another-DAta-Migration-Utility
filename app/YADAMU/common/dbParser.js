"use strict" 


const YadamuParser = require('./yadamuParser.js')

class DBParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
  }
}

module.exports = DBParser