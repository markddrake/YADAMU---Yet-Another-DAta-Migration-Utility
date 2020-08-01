"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class MsSQLParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);      
  }

  async _transform (data,encoding,callback) {
  	this.counter++
    data = Object.values(data)    
    // if (this.counter === 1) console.log(data)
    this.push({data: data})
    callback();
  }
}

module.exports = MsSQLParser
