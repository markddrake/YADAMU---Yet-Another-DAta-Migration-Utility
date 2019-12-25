"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class DBParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
    this.columnList = JSON.parse("[" + tableInfo.COLUMN_LIST + "]");
  }

  async _transform (data,encodoing,done) {
    let output = []
    this.counter++;
    

    output = this.columnList.map(function(key) {
      return data[key] !== 'NULL' ? data[key] : null
      return data[key]
    },this)

    if (!this.objectMode) {
      output = JSON.stringify(output)
    }

    // if (this.counter === 1) { console.log(data,output)}

    this.push({data : output})
    done();
  }
}

module.exports = DBParser