"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class ExampleParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);      

	const dataTypes = JSON.parse(tableInfo.DATA_TYPES)
    this.transformations = dataTypes.map((dataType) => {
	  switch (dataType.toLowerCase()) {
		 default:
		   return null
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
	
  }
    
  async _transform (data,encoding,callback) {
    this.counter++;
	if (!Array.isArray(data)) {
	  data = Object.values(data)
	}
    this.rowTransformation(data)
    this.push({data:data.json})
    callback();
  }
}

module.exports = ExampleParser