
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js' 

import YadamuParser         from '../base/yadamuParser.js'
import YadamuDataTypes      from '../base/yadamuDataTypes.js' 

class DB2Parser extends YadamuParser {
  
  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super(dbi,queryInfo,pipelineState,yadamuLogger)    	
  }

  generateTransformations(queryInfo) {
	  
	return queryInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
	  switch (dataType) {

		 case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
             return (row,idx)  => {
               row[idx] = `${row[idx][12] === 'T' ? row[idx] : row[idx].replace(' ','T')}Z`
		     }    
			 
		 case this.dbi.DATA_TYPES.JSON_TYPE:
		   // Top Level of a BSON object must be an object: Unwrap wrapped content.
		   return (row,idx) => {
			  const val = JSON.parse(row[idx])
			  row[idx] = Object.keys(val)[0] === 'yadamu' ? val.yadamu : val
		   }
		 case this.dbi.DATA_TYPES.XML_TYPE:
		   // XML returned as BLOB to prevent client and server attempting to negotiate character set conversions.
		   return (row,idx) => {
			  row[idx] = row[idx].toString()
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