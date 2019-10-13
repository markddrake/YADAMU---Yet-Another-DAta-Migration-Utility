"use strict" 

const Transform = require('stream').Transform;

class DBParser extends Transform {
  
  constructor(query,objectMode,yadamuLogger) {
    super({objectMode: true });   
    this.objectMode = objectMode
    this.columnList = JSON.parse("[" + query.COLUMN_LIST + "]");
    this.counter = 0
  }

  getCounter() {
    return this.counter;
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