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