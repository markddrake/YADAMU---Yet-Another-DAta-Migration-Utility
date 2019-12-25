"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class DBParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
  }
  
  async _transform (data,encodoing,done) {
    this.counter++;
    if (this.tableInfo.stripID === true) {
      delete data._id
    }
	
    switch (this.tableInfo.transformation) {
	  case 'DOCUMENT_TO_ARRAY' :
        data = this.tableInfo.columns.map(function(key) {
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