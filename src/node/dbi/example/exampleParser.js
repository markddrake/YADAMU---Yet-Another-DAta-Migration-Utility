"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class ExampleParser extends YadamuParser {
  
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);     	
  }
   
}

export { ExampleParser as default}