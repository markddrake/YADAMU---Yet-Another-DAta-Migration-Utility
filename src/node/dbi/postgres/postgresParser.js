"use strict" 

import YadamuParser from '../../common/yadamuParser.js'

class PostgresParser extends YadamuParser {
    
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);     
  }
  
}

export { PostgresParser as default }