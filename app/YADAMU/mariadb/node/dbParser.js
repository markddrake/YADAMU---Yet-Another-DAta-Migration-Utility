"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class DBParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
  }
  
  async _transform (data,encodoing,done) {
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
    done();
  }
}

module.exports = DBParser