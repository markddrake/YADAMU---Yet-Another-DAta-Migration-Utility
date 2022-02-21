"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class MsSQLParser extends YadamuParser {
  
  generateTransformations(queryInfo) {
  
    return queryInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
	  switch (dataType) {
		 case 'xml':
		   // Replace Entities for Non-Breaking space with ' ' and New Line with `\n' 
		   // Potential Problem with document centric XML ? 
		   // Issue with XML declaration
		   return (row,idx)  => {
             row[idx] = row[idx].replace(/&#x0A;/g,'\n').replace(/&#x20;/g,' ')
		   }     
		default:
  		   return null;
      }
    })

  }
  
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);   
  
  }

  async doTransform(data) {
	data = Object.values(data)    
	return super.doTransform(data)
  }
}


export { MsSQLParser as default }
