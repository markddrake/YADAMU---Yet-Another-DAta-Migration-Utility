"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class PostgresParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);     
    
    /*
    this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType) => {
	  switch (dataType.toLowerCase()) {
	  }
	})
	
	// Use a dummy rowTransformation function if there are no transformations required.

    this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
    */
  }
    
  async _transform (data,encoding,callback) {
    this.rowCount++;
	data = Object.values(data)
    // this.rowTransformation(data)
    // if (this.rowCount===1) console.log(data)
    this.push({data:data})
    callback();
  }
}

module.exports = PostgresParser