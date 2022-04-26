
import YadamuLibrary      from '../../lib/yadamuLibrary.js'

import YadamuParser       from '../base/yadamuParser.js'

class VerticaParser extends YadamuParser {

  generateTransformations(queryInfo) {  
	  
    return queryInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
      switch (dataType.toLowerCase()) {
	    case this.dbi.DATA_TYPES.INTERVAL_YEAR_TO_MONTH_TYPE:
		  return (row,idx) => {
			row[idx] = YadamuLibrary.intervalYearMonthTo8601(row[idx])
		  }
		case this.dbi.DATA_TYPES.INTERVAL_DAY_TO_SECOND_TYPE:
 		  return (row,idx) => {
		    row[idx] = YadamuLibrary.intervalDaySecondTo8601(row[idx])
		  }
		case this.dbi.DATA_TYPES.INTERVAL_TYPE:
          return (row,idx) => {
            row[idx] = YadamuLibrary.intervalDaySecondTo8601(row[idx])
	      }
   	    case this.dbi.DATA_TYPES.NUMERIC_TYPE:
		  // Trim excessive insiginicant zeros resulting from mapping for unbounded numbers
		  return (row,idx) => {
		    row[idx] = typeof row[idx] === 'string' ? row[idx].replace(/(\.0*|(?<=(\..*))0*)$/, '') : row[idx]
		  }
		default:
		  return null;
      }
    })
  }

  constructor(dbi,queryInfo,yadamuLogger,parseDelay) {
    super(dbi,queryInfo,yadamuLogger,parseDelay)    
  }
}

export { VerticaParser as default }