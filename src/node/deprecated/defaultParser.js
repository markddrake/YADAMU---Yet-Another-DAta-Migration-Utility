"use strict" 

import YadamuParser from './yadamuParser.js'

class DefaultParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);      
  } 
}

export { DefaultParser as default}