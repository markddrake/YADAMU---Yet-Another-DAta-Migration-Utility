"use strict" 


const YadamuParser = require('./yadamuParser.js')

class DefaultParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
  } 
}

module.exports = DefaultParser