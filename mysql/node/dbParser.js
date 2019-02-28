"use strict" 

const Transform = require('stream').Transform;

class DBParser extends Transform {
  
  constructor(query,objectMode,logWriter) {
    super({objectMode: true });   
    this.query = query;
    this.objectMode = objectMode
    this.logWriter = logWriter;
    this.counter = 0
    
  }

  getCounter() {
    return this.counter;
  }
  
  async _transform (data,encodoing,done) {
    this.counter++;
    if (!this.objectMode) {
      data.json = JSON.stringify(data.json);
    }
    this.push({data:data.json})
    done();
  }
}

module.exports = DBParser