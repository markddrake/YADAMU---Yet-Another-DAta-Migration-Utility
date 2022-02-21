"use strict" 

import YadamuParser from '../base/yadamuParser.js'
import YadamuLibrary from '../../lib/yadamuLibrary.js'

class VerticaParser extends YadamuParser {

  generateTransformations(queryInfo) {
	  
    return queryInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
      switch (dataType.toLowerCase()) {
	    case 'geometry':
	    case 'geography':
	      return (row,idx) => {
			row[idx] = Buffer.from(row[idx],'hex')
		  }	
		case 'interval year to month':
		  return (row,idx) => {
			row[idx] = YadamuLibrary.intervalYearMonthTo8601(row[idx])
		  }
		case 'interval day to second':
 		  return (row,idx) => {
		    row[idx] = YadamuLibrary.intervaDaySecondTo8601(row[idx])
		  }
		case 'interval':
          switch (true) {
            case (queryInfo.TARGET_DATA_TYPE.length === 0):
		      return (row,idx) => {
			    row[idx] = YadamuLibrary.intervalYearMonthTo8601(row[idx])
		      }
            case (queryInfo.TARGET_DATA_TYPES[idx].toLowerCase().startsWith('interval year')):
		      return (row,idx) => {
			    row[idx] = YadamuLibrary.intervalYearMonthTo8601(row[idx])
		     }
		   case (queryInfo.TARGET_DATA_TYPES[idx].startsWith('interval day')):
   		     return (row,idx) => {
			   row[idx] = YadamuLibrary.intervaDaySecondTo8601(row[idx])
		     }
		   default:
		      return (row,idx) => {
			    row[idx] = YadamuLibrary.intervalYearMonthTo8601(row[idx])
		     }
	      }
		default:
		  return null;
      }
    })
  }

  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);     
	
    queryInfo.TARGET_DATA_TYPES?.forEach((dataType,idx) => {
	  switch (dataType.toUpperCase()) {
		 case 'XML':
		 case 'XMLTYPE':
		    // Replace xsl:space with xml:space
		   this.transformations[idx] = (row,idx)  => {
             row[idx] = row[idx].replace(/>\\n/g,'>\n')
		   }     
		default:
  		   return null;
      }
    })
  }
}

export { VerticaParser as default }