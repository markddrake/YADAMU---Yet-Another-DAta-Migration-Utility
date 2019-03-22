"use strict" 

const Transform = require('stream').Transform;

class DBParser extends Transform {
  
  constructor(query,objectMode,logWriter) {
    super({objectMode: true });   
    this.query = query;
    this.objectMode = objectMode
    this.logWriter = logWriter;
    this.counter = 0
    this.columnList = JSON.parse(`[${this.query.COLUMN_LIST}]`);

    
  }

  getCounter() {
    return this.counter;
  }
  
  async _transform (data,encodoing,done) {
    let output = []

    this.counter++;

    for (let i=0; i < this.columnList.length; i++) {
      if (data[this.columnList[i]] instanceof Buffer) {
        output.push(data[this.columnList[i]].toString('hex'))
      }
      else {
        output.push(data[this.columnList[i]]);
      }
    }
    if (!this.objectMode) {
      output = JSON.stringify(output)
    }
    this.push({data : output})
    done();
  }
}

module.exports = DBParser