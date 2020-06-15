"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class MariadbParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
  }
  
  async _transform (data,encoding,callback) {
    // typeof JSON appears to be different when target is MySQL rather than Maria ?
    this.counter++;
    if (this.objectMode) {
      if (typeof data.json === "string") {
        data.json = JSON.parse(data.json);
      }
    }
    else {
      if (typeof data.json !== "string") {
        data.json = JSON.stringify(data.json);
      }
    }
    this.push({data:data.json})
    callback();
  }
}

module.exports = MariadbParser