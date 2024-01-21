
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
		case this.dbi.DATA_TYPES.INTERVAL_TYPE:
 		  return (row,idx) => {
            row[idx] = YadamuLibrary.intervalDaySecondTo8601(row[idx])
		  }
   	    case this.dbi.DATA_TYPES.NUMERIC_TYPE:
		  // Trim excessive insiginicant zeros resulting from mapping for unbounded numbers
		  return (row,idx) => {
		    row[idx] = typeof row[idx] === 'string' ? row[idx].replace(/(\.0*|(?<=(\..*))0*)$/, '') : row[idx]
		  }
   	    case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
		  // Trim excessive insiginicant zeros resulting from mapping for unbounded numbers
		  return (row,idx) => {
		    row[idx] = YadamuLibrary.toBoolean(row[idx])
		  }
		case this.dbi.DATA_TYPES.DOUBLE_TYPE:
		  // Trim excessive insiginicant zeros resulting from mapping for unbounded numbers
		  return (row,idx) => {
		    row[idx] = Number(row[idx]).toExponential(17)
		  }
		default:
		  return null;
      }
    })
  }

  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super(dbi,queryInfo,pipelineState,yadamuLogger)    
  }
  
}

export { VerticaParser as default }