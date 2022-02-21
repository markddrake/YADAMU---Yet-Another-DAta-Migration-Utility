"use strict" 

import YadamuParser from '../../common/yadamuParser.js'

class ExampleParser extends YadamuParser {
  
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);     	
  }
   
}

export { ExampleParser as default}