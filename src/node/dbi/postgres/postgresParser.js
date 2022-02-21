"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class PostgresParser extends YadamuParser {
    
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);     
  }
  
}

export { PostgresParser as default }