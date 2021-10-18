"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class MariadbParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);    

	this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType) => {
	  switch (dataType.toLowerCase()) {
		 case "decimal":
		   return (row,idx) => {
			  row[idx] = typeof row[idx] === 'string' ? row[idx].replace(/(\.0*|(?<=(\..*))0*)$/, '') : row[idx]
		   }
		 /*
		 case "set":
		   // Convert comma seperated list to string array. Assume that a value cannont contain a ',' which seems to enforced at DDL time
		   return (row,idx) => {
			  row[idx] = row[idx].split(',')
		   }				  
		 */
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
   this.rowCount++
    this.rowTransformation(data)
    // if (this.rowCount === 1) console.log(data)
    this.push({data:data})
    callback();
  }
}

module.exports = MariadbParser