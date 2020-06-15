"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class SnowflakeParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
    this.columnList = JSON.parse("[" + tableInfo.COLUMN_LIST + "]");
  }

  async _transform (data,encoding,callback) {
    let output = []
    this.counter++;
    

    output = this.columnList.map((key) => {
      return data[key] !== 'NULL' ? data[key] : null
      return data[key]
    })

    if (!this.objectMode) {
      output = JSON.stringify(output)
    }

    // if (this.counter === 1) { console.log(data,output)}

    this.push({data : output})
    callback();
  }
}

module.exports = SnowflakeParser