
import { 
  performance 
}                               from 'perf_hooks';
						
import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

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
		   return (col,idx) => {
			 return typeof col === 'string' && col[10] === 'T' ? col.substring(11,26) : col
		   }	       
		case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
		   return (col,idx) => {
			 return typeof col === 'string' && col[10] === 'T' ? col.substring(11) : col
		   }	       
		case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		   return (col,idx) => {
			 col = ((typeof col === 'string') && (col[10] === 'T')) ? col.replace('T',' ') : col
			 col = ((typeof col === 'string') && (col.length > 26)) ? col.substring(0,26) : col
			 return col
		   }	       
		case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
		  const tz_length = ((dataTypeDefinition.length === 0) || !dataTypeDefinition.hasOwnProperty('length')) ? 19 : (20 + dataTypeDefinition.length)
		  return (col,idx) => {
            if (typeof col === 'string') {
              col = col.slice(0,tz_length) + 'Z'
            }
            else {
              if (col.endsWith('+00:00')) {
			    if (col.length > tz_length+6) {
			      col = col.slice(0,tz_length) + '+00:00'
				}
			  }
		      else {
                // Avoid unexpected Time Zone Conversions when inserting from a Javascript Date object 
                col = col.toISOString();
              }
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