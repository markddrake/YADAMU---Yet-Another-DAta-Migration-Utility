"use strict" 

import YadamuParser from '../../common/yadamuParser.js'
import YadamuLibrary from '../../common/yadamuLibrary.js'

class RedshiftParser extends YadamuParser {

  generateTransformations(queryInfo) {
    return queryInfo.TARGET_DATA_TYPES.map((dataType) => {
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
  }
  
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);     
  }
    
}

export { RedshiftParser as default }