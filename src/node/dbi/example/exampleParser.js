"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class ExampleParser extends YadamuParser {
  
  constructor(dbi,queryInfo,yadamuLogger,parseDelay) {
    super(dbi,queryInfo,yadamuLogger,parseDelay)    	
  }
   
}

export { ExampleParser as default}