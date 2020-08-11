"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class MariadbParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);      

	this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType) => {
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
   this.counter++
    this.rowTransformation(data)
    // if (this.counter === 1) console.log(data)
    this.push({data:data})
    callback();
  }
}

module.exports = MariadbParser