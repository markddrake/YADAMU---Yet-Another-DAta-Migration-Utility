"use strict" 

const Transform = require('stream').Transform;

class YadamuParser extends Transform {
    
  constructor(tableInfo,objectMode,yadamuLogger) {
    super({objectMode: true });  
    this.tableInfo = tableInfo;
    this.objectMode = objectMode
    this.yadamuLogger = yadamuLogger
    this.counter = 0
    
    this.columnMetadata = undefined;
    this.includesLobs = false;
    
  }
    
  getCounter() {
    return this.counter;
  }

  // For use in cases where the database generates a single column containing a serialized JSON reprensentation of the row.
  
  async _transform (data,encodoing,done) {
    this.counter++;
    if (this.objectMode) {
      data.json = JSON.parse(data.json);
    }
    this.push({data:data.json})
    done();
  }
  
}

module.exports = YadamuParser