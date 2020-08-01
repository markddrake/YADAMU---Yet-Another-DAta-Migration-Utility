"use strict" 

const SharedParser = require('../../dbShared/mysql/mysqlParser.js')

class MariadbParser extends SharedParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);
  }    

}

module.exports = MariadbParser