
import { 
  performance 
}                            from 'perf_hooks';
						
import YadamuLibrary         from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary  from '../../lib/yadamuSpatialLibrary.js'

import YadamuOutputManager   from '../base/yadamuOutputManager.js'

class SnowflakeWriter extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {  
	super(dbi,tableName,metrics,status,yadamuLogger)
  }

  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
    return targetDataTypes.map((targetDataType,idx) => {      
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
	
	  if (YadamuLibrary.isBinary(dataType.type)) {
		return (col,idx) =>  {
          return col.toString('hex')
		}
      }

	  switch (dataType.type.toUpperCase()) {
        case 'GEOMETRY': 
        case 'GEOGRAPHY':
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (col,idx)  => {
		       return Buffer.isBuffer(col) ? col.toString('hex') : col
			}
          }
		  return null
		case 'JSON':
		case 'OBJECT':
		case 'ARRAY':
          return (col,idx) => {
            return JSON.stringify(col)
		  }
        case 'VARIANT':
          return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
		  }
        case "BOOLEAN" :
 		  return (col,idx) => {
             return YadamuLibrary.toBoolean(col)
          }
          break;
		case "TIME" :
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              col = components.length === 1 ? components[0] : components[1]
			  col = col.split('Z')[0]
			  col = col.split('+')[0]
			  col = col.split('-')[0]
			  return col
            }
            else {
              return `${col.getUTCHours()}:${col.getUTCMinutes()}:${col.getUTCSeconds()}.${col.getUTCMilliseconds()}`;  
            }
		  }
          break;
        /*
        **
        ** Snowflake-sdk appears to have some issues around Infinity, -Infinity and NaN.
        **
        ** Convert Infinity, -Infinity and NaN to 'Inf', '-Inf' & 'NaN'
        ** However this seems to then require all finite values be passed as strings.
        **
        */
 		case "REAL":
        case "FLOAT":
		case "DOUBLE":
		case "DOUBLE PRECISION":
		case "BINARY_FLOAT":
		case "BINARY_DOUBLE":
		  return (col,idx) => {
	        if (!isFinite(col)) {
			  switch(col) {				
				case Infinity:
			    case 'Infinity':
			      return 'Inf'
			    case '-Infinity':
		     	case -Infinity:
				  return '-Inf'
			    default:
				  return 'NaN'
			  }
		    }
            return col.toString()
	      }
        default :
		  return null
      }
    })
	
  }
        
  cacheRow(row) {
	  
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	
    this.rowTransformation(row)

    if (this.tableInfo.parserRequired) {
      this.batch.push(...row);
    }
    else {
  	  this.batch.push(row);
    }
	
    this.COPY_METRICS.cached++
	return this.skipTable
	
  }
  
}

export { SnowflakeWriter as default }