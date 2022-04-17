
import { 
  performance 
}                            from 'perf_hooks';
						
import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

class SnowflakeWriter extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {  
	super(dbi,tableName,metrics,status,yadamuLogger)
  }

  generateTransformations(dataTypes) {

    // Set up Transformation functions to be applied to the incoming rows

    let spatialFormat = this.SPATIAL_FORMAT	

    return dataTypes.map((dataType,idx) => {      
      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType);
	  switch (dataTypeDefinition.type.toUpperCase()) {
        case this.dbi.DATA_TYPES.BLOB_TYPE:
  		case this.dbi.DATA_TYPES.BINARY_TYPE:
  		  return (col,idx) =>  {
            return col.toString('hex')
		  }
        case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
		  switch (this.SPATIAL_FORMAT) {
            case "WKB":
		    case "EWKB":
		      // Snowflake's handling of WKB appears a little 'flaky' :) - Convert to WRT
			  /*
              return (col,idx)  => {
		         return Buffer.isBuffer(col) ? col.toString('hex') : col
		      }
			  */
			  spatialFormat = 'WKT'
			  return (col,idx) => {
                return YadamuSpatialLibrary.bufferToWKT(col)
              }
            case "GeoJSON":
    		  return (col,idx) => {
			    return typeof col === 'object' ? JSON.stringify(col) : col
		      }
		    default:
		      return null
		  }
		case this.dbi.DATA_TYPES.JSON_TYPE:
		  return (col,idx) => {
			return typeof col === 'object' ? JSON.stringify(col) : col
		  }
		case this.dbi.DATA_TYPES.SNOWFLAKE_VARIANT_TYPE:
		  return (col,idx) => {
			return typeof col === 'object' ? JSON.stringify(col) : col
		  }
        case this.dbi.DATA_TYPES.SNOWFLAKE_VARIANT_TYPE:
          return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
		  }
        case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
 		  return (col,idx) => {
             return YadamuLibrary.toBoolean(col)
          }
          break;
		case this.dbi.DATA_TYPES.TIME_TYPE:
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
		case this.dbi.DATA_TYPES.DOUBLE_TYPE:
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
		
	this.tableInfo._SPATIAL_FORMAT = spatialFormat
	
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