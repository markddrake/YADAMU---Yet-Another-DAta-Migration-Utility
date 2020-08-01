"use strict" 

const SharedParser = require('../../dbShared/mysql/mysqlParser.js')

class MySQLParser extends SharedParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger); 
  }    
  
  // MySQL requires Object to Array Transformation
  
  async _transform (data,encoding,callback) {
    data = Object.values(data)    
	super._transform(data,encoding,callback)
  }
  
}

module.exports = MySQLParser