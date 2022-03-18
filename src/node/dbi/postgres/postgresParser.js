"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class PostgresParser extends YadamuParser {
    
  constructor(queryInfo,yadamuLogger,parseDelay) {
    super(queryInfo,yadamuLogger,parseDelay);     
  }
  
}

export { PostgresParser as default }