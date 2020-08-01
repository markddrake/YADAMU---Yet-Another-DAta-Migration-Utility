"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class SnowflakeParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);      
  }
  
  async _transform (data,encoding,callback) {
    // Snowflake generates object based output, not array based outout. Transform object to array based on columnList
    this.counter++;
	// if (this.counter === 1) console.log('Snowflake Parser',data)
    data = Object.values(data)
	data.forEach((val,idx) => {
	   if (data[idx] === 'NULL') {
		 data[idx] = null;
	   }
       if (Buffer.isBuffer(data[idx])) {
         data[idx] = data[idx].toStringSf()
       }
	})
    this.push({data : data})
    callback();
  }
}

module.exports = SnowflakeParser