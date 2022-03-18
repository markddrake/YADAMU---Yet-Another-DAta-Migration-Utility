"use strict" 

import YadamuParser from '../base/yadamuParser.js'

class MariadbParser extends YadamuParser {
  
  generateTransformations(queryInfo) {
	  
	 return queryInfo.DATA_TYPE_ARRAY.map((dataType) => {
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
  }
  
  constructor(queryInfo,yadamuLogger,parseDelay) {
    super(queryInfo,yadamuLogger,parseDelay);    
  }
  
}

export { MariadbParser as default }