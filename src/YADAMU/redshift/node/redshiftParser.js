"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')
const YadamuLibrary = require('../../common/yadamuLibrary.js')

class RedshiftParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);     
	
    const transformations = tableInfo.TARGET_DATA_TYPES.map((dataType) => {
	  switch (true) {
		 case YadamuLibrary.isBinaryType(dataType):
		   return (row,idx) => {
             row[idx] = Buffer.from(row[idx],'hex')
           }			   
		 case YadamuLibrary.isSpatialType(dataType):
		   return (row,idx) => {
             row[idx] = Buffer.from(row[idx],'hex')
           }			   
		 case YadamuLibrary.isXML(dataType):
		   return (row,idx) => {
             row[idx] = row[idx].length === 0 ? null : row[idx]
           }			  
	  }
	})

	// Use a dummy rowTransformation function if there are no transformations required.

    this.rowTransformation = transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      transformations.forEach((transformation,idx) => {
        if (transformation && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
  }
    
  async _transform (data,encoding,callback) {
    this.rowCount++;
	this.rowTransformation(data)
    // if (this.rowCount===1) console.log('redshiftParser',data)
    this.push({data:data})
    callback();
  }
}

module.exports = RedshiftParser