
import YadamuParser       from '../base/yadamuParser.js'
import YadamuLibrary      from '../../lib/yadamuLibrary.js'

class VerticaParser extends YadamuParser {

  generateTransformations(queryInfo) {  
	  
    return queryInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
      switch (dataType.toLowerCase()) {
	    case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
	    case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
	      return (row,idx) => {
			row[idx] = Buffer.from(row[idx],'hex')
		  }	
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