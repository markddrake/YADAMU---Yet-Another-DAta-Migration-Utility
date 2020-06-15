"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class MongoParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger); 
  }
  
  async _transform (data,encoding,callback) {
	this.counter++;
    if (this.tableInfo.idTransformation === 'STRIP') {
      delete data._id
    }
	else {
	  data._id = data._id.toString()
    }
	
    switch (this.tableInfo.readTransformation) {
	  case 'DOCUMENT_TO_ARRAY' :
	data = this.tableInfo.columns.map((key) => {
          return data[key]
        })
		break;
      default:
    }
    if (!this.objectMode) {
      data = JSON.stringify(data);
    }
    this.push({data:data})
    callback();
  }
}

module.exports = MongoParser