"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class PostgresParser extends YadamuParser {
    
  constructor(queryInfo,yadamuLogger,parseDelay) {
    super(queryInfo,yadamuLogger,parseDelay);     
  }
  
  generateTransformations(queryInfo) {
	 
    return queryInfo.DATA_TYPE_ARRAY.map((dataType) => {
	  switch (dataType.toLowerCase()) {
		 case this.dbi.DATA_TYPES.INTERVAL_YEAR_TO_MONTH_TYPE:
		   // Fix Non Standand rendering of '0' with INTERVAL YEAR TO MONTH columns
		   return (row,idx) => {
			 row[idx] = row[idx] === 'PT0S' ? 'P0Y' : row[idx]
		   }
		 default:
		   return null
	  }
	})
  }

  
}

export { PostgresParser as default }