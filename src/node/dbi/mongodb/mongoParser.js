"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class MongoParser extends YadamuParser {
  
  generateTransformations(queryInfo) {

	return queryInfo.DATA_TYPE_ARRAY.map((dataType) => {
	  switch (dataType) {
		 case 'binData':
		   return (row,idx)  => {
             row[idx] = row[idx].buffer;
		   }
         case 'objectId':0
		   return (row,idx)  => {
             row[idx] = Buffer.from(row[idx].toHexString(),'hex')
		   }
		 case 'long':
		 case 'decimal':
		   return (row,idx)  => {
  		     row[idx] = row[idx].toString()
           }
		 /*
		 case "object":
		 case "array":
		    return (row,idx) => {
		      row[idx] = JSON.stringify(row[idx])
			}
	     */
         default:
		   return null;
      }
    })
  }
  
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger); 	
  }
  
  async doTransform(data) {

	if (this.queryInfo.ID_TRANSFORMATION === 'STRIP') {
      delete data._id
    }
	
    switch (this.queryInfo.READ_TRANSFORMATION) {
	  case 'DOCUMENT_TO_ARRAY' :
	    // Need to assemble array in correct order.
	    data = this.queryInfo.JSON_KEY_NAME_ARRAY.map((c) => { return data[c] })
		break;
      default:
    }
	// if (this.rowCount === 1) console.log(data)
		
    return await super.doTransform(data)
  }
}

export { MongoParser as default }