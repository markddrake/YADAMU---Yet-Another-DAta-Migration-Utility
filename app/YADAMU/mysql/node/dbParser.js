"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class DBParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
  }
  
  
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