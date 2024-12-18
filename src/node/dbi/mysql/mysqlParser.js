
import YadamuLibrary     from '../../lib/yadamuLibrary.js'

import YadamuParser      from '../base/yadamuParser.js'

class MySQLParser extends YadamuParser {

  generateTransformations(queryInfo) {
	 
    return queryInfo.DATA_TYPE_ARRAY.map((dataType) => {
	  switch (dataType.toLowerCase()) {
		 case this.dbi.DATA_TYPES.NUMERIC_TYPE:
		   // Trim excessive insiginicant zeros resulting from mapping for unbounded numbers
		   return (row,idx) => {
			  row[idx] = typeof row[idx] === 'string' ? YadamuLibrary.stripInsignificantZeros(row[idx]) : row[idx]
		   }
		 case this.dbi.DATA_TYPES.MYSQL_SET_TYPE:
		   // Convert comma seperated list to string array. Assume that a value cannont contain a ',' which seems to enforced at DDL time
		   return (row,idx) => {
			  row[idx] = row[idx].split(',')
		   }
		 default:
		   return null
	  }
	})
  }

  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super(dbi,queryInfo,pipelineState,yadamuLogger)     
  }

  async doTransform(data) {
    // data = Object.values(data)    
	return await super.doTransform(data)
  }
}

export { MySQLParser as default }