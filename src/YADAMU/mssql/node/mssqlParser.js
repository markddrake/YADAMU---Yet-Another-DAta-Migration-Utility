"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class MsSQLParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);   
  }

  async _transform (data,encoding,callback) {
  	this.rowCount++
	data = Object.values(data)    
    // if (this.rowCount === 1) console.log(data)
    this.push({data: data})
    callback();
  }
}

module.exports = MsSQLParser
