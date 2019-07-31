"use strict" 

const Transform = require('stream').Transform;

class DBParser extends Transform {
  
  constructor(query,objectMode,yadamuLogger) {
    super({objectMode: true });   
    this.query = query;
    this.objectMode = objectMode
    this.yadamuLogger = yadamuLogger;
    this.counter = 0
  }

  getCounter() {
    return this.counter;
  }
  
  // Use in cases where query generates a column called JSON containing a serialized reprensentation of the generated JSON.
  
  async _transform (data,encodoing,done) {
    this.counter++;
    if (this.objectMode) {
      data.json = JSON.parse(data.json);
    }
    this.push({data:data.json})
    done();
  }
}

module.exports = DBParser