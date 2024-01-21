
import YadamuLibrary       from '../../lib/yadamuLibrary.js'

import YadamuParser        from '../base/yadamuParser.js'

class MariadbParser extends YadamuParser {
  
  generateTransformations(queryInfo) {
	  
	 return queryInfo.DATA_TYPE_ARRAY.map((dataType) => {
	  switch (dataType.toLowerCase()) {
		 case this.dbi.DATA_TYPES.NUMERIC_TYPE:
		   return (row,idx) => {
			  row[idx] = typeof row[idx] === 'string' ? YadamuLibrary.stripInsignificantZeros(row[idx]) : row[idx]
		   }
		 case this.dbi.DATA_TYPES.MYSQL_SET_TYPE:
           // Unlike MySQL which appeares to render as set sa string,MariaDB appears to natively render a set as a JSON array so not transformation is required
           return null
           /*
		   // Convert comma seperated list to string array. Assume that a value cannont contain a ',' which seems to enforced at DDL time
		   return (row,idx) => {
              row[idx] = row[idx].split(',')
		   }				  
           */
		 default:
		   return null
	  }
	})	 
  }
  
  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super(dbi,queryInfo,pipelineState,yadamuLogger)   
  }
  
}

export { MariadbParser as default }