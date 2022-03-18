"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class ExampleParser extends YadamuParser {
  
  constructor(queryInfo,yadamuLogger,parseDelay) {
    super(queryInfo,yadamuLogger,parseDelay);     	
  }
   
}

export { ExampleParser as default}