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
	
    switch (this.query.transformation) {
	  case 'DOCUMENT_TO_ARRAY' :
        data = this.query.columns.map(function(key) {
          return data[key]
        },this)
		break;
      default:
    }
        
    if (!this.objectMode) {
      data = JSON.stringify(data);
    }
	
    this.push({data:data})
    done();
  }
}

module.exports = DBParser