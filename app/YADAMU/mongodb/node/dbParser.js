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
    this.counter++;
    if (this.query.stripID === true) {
      delete data._id
    }

    if (this.query.outputMode === 'ARRAY') {
      data = this.query.columns.map(function(key) {
        return data[key]
      },this)
    }
        
    if (!this.objectMode) {
      data = JSON.stringify(data);
    }
        
    this.push({data:data})
    done();
  }
}

module.exports = DBParser