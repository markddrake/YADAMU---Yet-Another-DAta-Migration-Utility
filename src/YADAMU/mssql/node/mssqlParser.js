"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class MsSQLParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);   
  
    this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
	  switch (dataType) {
		 case 'xml':
		   // Replace Entities for Non-Breaking space with ' ' and New Line with `\n' 
		   // Potential Problem with document centric XML ? 
		   /// Issue with XML declaration
		   return (row,idx)  => {
             row[idx] = row[idx].replace(/&#x0A;/g,'\n').replace(/&#x20;/g,' ')
		   }     
		default:
  		   return null;
      }
    })

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
	data = Object.values(data)    
	this.rowTransformation(data)
    // if (this.rowCount === 1) console.log(data)
    this.push({data: data})
    callback();
  }
}


module.exports = MsSQLParser
