
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js' 

import YadamuParser         from '../base/yadamuParser.js'
import YadamuDataTypes      from '../base/yadamuDataTypes.js' 

class DB2Parser extends YadamuParser {
  
  constructor(dbi,queryInfo,yadamuLogger,parseDelay) {
    super(dbi,queryInfo,yadamuLogger,parseDelay)    	
  }

  generateTransformations(queryInfo) {
	  
	return queryInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
	  switch (dataType) {
		 case this.dbi.DATA_TYPES. BLOB_TYPE:
		   return (row,idx)  => {
             row[idx] = Buffer.from(row[idx],'hex')
		   }     
		 case this.dbi.DATA_TYPES.VARBINARY_TYPE:
		 case this.dbi.DATA_TYPES.BINARY_TYPE:
		 case this.dbi.DATA_TYPES.VARBINARY_TYPE:
		   return (row,idx)  => {
             row[idx] = Buffer.from(row[idx],'hex')
		   }     
		 case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
             return (row,idx)  => {
               row[idx] = `${row[idx][12] === 'T' ? row[idx] : row[idx].replace(' ','T')}Z`
		     }     
		 case this.dbi.DATA_TYPES.JSON_TYPE:
		   // Unwrap wrapped content.
		   return (row,idx) => {
			  const val = JSON.parse(row[idx])
			  row[idx] = Object.keys(val)[0] === 'yadamu' ? val.yadamu : val
		   }
 		 default:
  		   return null;
      }
    })

  }
  
   
  async doTransform(data) {
	data = Object.values(data)    
	return await super.doTransform(data)
  }
  
}


export { DB2Parser as default}