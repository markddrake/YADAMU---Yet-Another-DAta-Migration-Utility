
import { 
  performance 
}                               from 'perf_hooks';
						
import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

import TeradataIntervals        from './teradataIntervals.js'

class TeradataOutputManager extends YadamuOutputManager {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }

  generateTransformations(dataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
	
	let spatialFormat = this.SPATIAL_FORMAT

    // console.log(dataTypes,this.SPATIAL_FORMAT)
    
    return dataTypes.map((dataType,idx) => {      
      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType);
      // The length of a time or timestamp value with the column's fractional precision. A copy between Teradata databases maps 'TS' to 'TS',
      // so the precision comes from the column's size constraint rather than the data type.
      const fractionLength = (wholeSeconds) => {
        const precision = dataTypeDefinition.length ?? this.tableInfo.sizeConstraints?.[idx]?.[0] ?? this.dbi.DATA_TYPES.TIMESTAMP_PRECISION
        return (precision > 0) ? wholeSeconds + 1 + precision : wholeSeconds
      }
      // An ISO 8601 duration becomes Teradata's text for the column's INTERVAL type, and a range a PERIOD literal (see teradataIntervals.js)
      // A YEAR arrives in an INTERVAL YEAR as a number. The driver binds a number as FLOAT, which Teradata can't convert to an interval (Error 5407), but it converts text.
      if (TeradataIntervals.isInterval(dataTypeDefinition.type)) {
        return (col,idx) => { return typeof col === 'number' ? col.toString() : TeradataIntervals.intervalFromISO(dataTypeDefinition.type,col,this.tableInfo.sizeConstraints?.[idx]) }
      }
      if (TeradataIntervals.isPeriod(dataTypeDefinition.type)) {
        return (col,idx) => { return TeradataIntervals.periodFromJSON(dataTypeDefinition.type,col) }
      }
	  switch (dataTypeDefinition.type.toUpperCase()) {
        case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
  	      spatialFormat = 'WKT'
          switch (this.SPATIAL_FORMAT) {
            case "WKB":
		    case "EWKB":
		      return (col,idx) => {
                return Buffer.isBuffer(col) ? YadamuSpatialLibrary.bufferToWKT(col) : YadamuSpatialLibrary.hexBinaryToWKT(col)
              }
            case "GeoJSON":
    	      return (col,idx) => {
                return YadamuSpatialLibrary.geoJSONtoWKT(typeof col === 'object' ? col : JSON.parse(col))
		      }
            default:
		      return null
		  }
		case this.dbi.DATA_TYPES.TIME_TYPE:
		  // Teradata rejects more fractional digits than the column's precision
		  const timeLength = fractionLength(8)
		  return (col,idx) => {
		    if (typeof col === 'string') {
		      col = (col[10] === 'T') ? col.substring(11) : col
		      col = col.replace(/(Z|[+-]\d\d:\d\d)$/,'').slice(0,timeLength)
		    }
		    return col
		  }
		case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
		   return (col,idx) => {
			 return typeof col === 'string' && col[10] === 'T' ? col.substring(11) : col
		   }	       
		case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		  const timestampLength = fractionLength(19)
		  return (col,idx) => {
		    col = (col instanceof Date) ? col.toISOString() : col
		    if (typeof col === 'string') {
		      col = (col[10] === 'T') ? col.replace('T',' ') : col
		      col = col.replace(/(Z|[+-]\d\d:\d\d)$/,'').slice(0,timestampLength)
		    }
		    return col
		  }
		case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
		  const tzLength = fractionLength(19)
		  return (col,idx) => {
            if (col instanceof Date) {
              // Avoid unexpected Time Zone Conversions when inserting from a Javascript Date object
              col = col.toISOString();
            }
            // Keep the column's fractional digits and the offset, and write UTC as Z
            const components = (typeof col === 'string') && col.match(/^(\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:\.\d*)?)(.*)$/)
            if (components) {
              const offset = ((components[2] === '') || (components[2] === '+00:00') || (components[2] === '-00:00')) ? 'Z' : components[2]
              col = components[1].slice(0,tzLength) + offset
            }
            col = (col[10] === 'T') ? col.replace('T',' ') : col
            return col
	      }
		case this.dbi.DATA_TYPES.DATE_TYPE:
		   return (col,idx) => {
			 return typeof col === 'string' ? col.substring(0,10) : col
		   }
		   break;
		case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
           return (col,jdx) =>  {
             return YadamuLibrary.booleanToInt(col)
           }
	    case this.dbi.DATA_TYPES.BINARY_TYPE:
		  return(col,idx) => {
			// Set the transformation based on the type of first data received 
		    switch (typeof col) {
			  case "string" :
			    this.transformations[idx] = (col,idx) => {
			      const buf = Buffer.allocUnsafe(4)
				    buf.writeInt32LE(parseInt(col))
					return buf
			    }
				break
			  case "number" :
			    this.transformations[idx] = (col,idx) => {
				  const buf = Buffer.allocUnsafe(4)
				  buf.writeInt32LE(col)
				  return buf
				}
				break
			  default:
			    this.transformations[idx] = (col,idx) => {
				  return col
				}				
			}
            return this.transformations[idx](col,idx)
		  }
        case this.dbi.DATA_TYPES.JSON_TYPE:
		case this.dbi.DATA_TYPES.VARCHAR_TYPE:
		case this.dbi.DATA_TYPES.CLOB_TYPE:
          return (col,jdx) =>  {
            // JSON must be shipped in Serialized Form
            return typeof col === 'object' ? JSON.stringify(col) : col
          }
          break;
		case this.dbi.DATA_TYPES.DOUBLE_TYPE:
          return (col,idx) => {
			if (typeof col === 'string') {
		       this.transformations[idx] = (col,idx) => {
	             if (!isFinite(col)) {
			       switch(col) {				
     			    case 'INF':
			        case 'Infinity':
			          return Number.POSITIVE_INFINITY
			        case '-Infinity':
    			    case '-INF':
				      return Number.NEGATIVE_INFINITY
			        default:
				      return NaN
			       }
			     }
				 return parseFloat(col)
			   }
			   return this.transformations[idx](col,idx)
			}
			else {
   	          this.transformations[idx] = null
			  return col
		    }
	      }	    
		default :
		  return null
      }
    })
	
    this.tableInfo._SPATIAL_FORMAT = spatialFormat

  }
  
}

export { TeradataOutputManager as default }