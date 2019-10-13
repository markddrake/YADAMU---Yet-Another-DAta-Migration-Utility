"use strict" 

const Transform = require('stream').Transform;

const YadamuLibrary = require('../../common/yadamuLibrary.js');

class DBParser extends Transform {
    
  constructor(query,objectMode,yadamuLogger,dbi) {
    super({objectMode: true });   
    this.query = query;
    this.objectMode = objectMode
    this.yadamuLogger = yadamuLogger;
    this.dbi = dbi;
    this.counter = 0
    
  }

  getCounter() {
    return this.counter;
  }
  
  // Use in cases where query generates a column called JSON containing a serialized reprensentation of the generated JSON.
  
  async _transform (data,encodoing,done) {
    this.counter++
   
    if (this.objectMode === true) {
      if (typeof data.json === 'string') {
		data.json = JSON.parse(data.json);
	  }
	}
	else {
      if (typeof data.json === 'object') {
        data.json = JSON.stringify(data.json);
	  }
    }
    
	this.push({data:data.json})
    done();
  }
}

module.exports = DBParser