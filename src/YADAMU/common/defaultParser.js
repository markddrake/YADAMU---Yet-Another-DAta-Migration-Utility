"use strict" 


const YadamuParser = require('./yadamuParser.js')

class DefaultParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);      
  } 
}

module.exports = DefaultParser