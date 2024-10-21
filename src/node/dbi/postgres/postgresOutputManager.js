
import { 
  performance 
}                               from 'perf_hooks';
						
import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

class PostgresOutputManager extends YadamuOutputManager  {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }
  
  generateTransformations(dataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
	
	return dataTypes.map((dataType,idx) => {
      
	  const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType)
	  
	  // Conversion from JSON to PGSQL specific data types is handled by Functions loaded into the database.
     
	  if (this.dbi.DATA_TYPES.isJSON(dataTypeDefinition.type)) {
		// https://github.com/brianc/node-postgres/issues/442
        return (col,idx) => {
          return typeof col === 'object' ? JSON.stringify(col) : col
        }
	  }	  
	  
	  switch (dataTypeDefinition.type.toLowerCase()) {
        case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
 		  return (col,idx) => {
             return YadamuLibrary.toBoolean(col)
          }
        case this.dbi.DATA_TYPES.TIME_TYPE:
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              col = components.length === 1 ? components[0] : components[1]
              return col.split('Z')[0].split('+')[0]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds();  
            }
		  }
        case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
          return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              return components.length === 1 ? components[0] : components[1]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds();
            }
		  }
        case this.dbi.DATA_TYPES.DATE_TYPE:
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
		  return (col,idx) => {
            if (typeof col === 'string') {
              if (col.endsWith('Z') && col.length === 28) {
                col = col.slice(0,-2) + 'Z'
              }
              else {
                if (col.endsWith('+00:00')) {
			      if (col.length > 32) {
					col = col.slice(0,26) + '+00:00'
				  }
				}
				else {
                  if (col.length === 27) {                                
                    col = col.slice(0,-1) 
                  }
                }
              }               
            }
            else {
              // Avoid unexpected Time Zone Conversions when inserting from a Javascript Date object 
              col = col?.toISOString() 
            }
			return col
		  }
        case this.dbi.DATA_TYPES.BIT_STRING_TYPE:
	      const fixedLength = this.tableInfo.sizeConstraints[idx][0]
		  return (col,idx) => {
			return col.padStart(fixedLength,'0')
		  }
        case this.dbi.DATA_TYPES.PGSQL_OID_TYPE:                     
        case this.dbi.DATA_TYPES.PGSQL_REG_CLASS_TYPE:               
		case this.dbi.DATA_TYPES.PGSQL_REG_COLLATION_TYPE:           
		case this.dbi.DATA_TYPES.PGSQL_REG_TEXTSEARCH_CONFIG_TYPE:   
		case this.dbi.DATA_TYPES.PGSQL_REG_TEXTSEARCH_DICT_TYPE:     
		case this.dbi.DATA_TYPES.PGSQL_REG_NAMESPACE_TYPE:           
		case this.dbi.DATA_TYPES.PGSQL_REG_OPERATOR_NAME_TYPE:       
		case this.dbi.DATA_TYPES.PGSQL_REG_OPERATOR_ARGS_TYPE:       
		case this.dbi.DATA_TYPES.PGSQL_REG_FUNCTION_NAME_TYPE:       
		case this.dbi.DATA_TYPES.PGSQL_REG_FUNCTION_ARGS_TYPE:       
		case this.dbi.DATA_TYPES.PGSQL_REG_ROLE_TYPE:                
	    case this.dbi.DATA_TYPES.PGSQL_REG_TYPE_TYPE:
		  return (col,idx) => {
			return Buffer.isBuffer(col) ? col.readUInt32BE(0) : col
		  }
		case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
		case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
		  if (!this.dbi.POSTGIS_INSTALLED) {
		    switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
		      case 'WKB':
		      case 'EWKB':
 		        return (col,idx) => {
                  return Buffer.isBuffer(col) ? col : Buffer.from(col,'hex')
		      }     
              case "WKT":
              case "EWKT":
			    return NULL
              case "GeoJSON":
		        return (col,idx) => {
			      return typeof col === 'object' ? val = JSON.stringify(col) : col
		        }
              default :
                return null
		    }
		  }
          return null
		case this.dbi.DATA_TYPES.POINT_TYPE:
		case this.dbi.DATA_TYPES.LINE_TYPE:
		case this.dbi.DATA_TYPES.PATH_TYPE:
		case this.dbi.DATA_TYPES.POLYGON_TYPE:
		case this.dbi.DATA_TYPES.BOX_TYPE:
		case this.dbi.DATA_TYPES.POLYGON_TYPE:
		  if (!this.dbi.POSTGIS_INSTALLED) {
			return YadamuSpatialLibrary.toGeoJSON(this.dbi.INBOUND_SPATIAL_FORMAT)
		  }
          return null
        default :
		  return null
      }
    })

  }
  
  cacheRow(row) {
	  
    // if (this.metrics.cached === 1) console.log('postgresWriter',row)
		
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	this.rowTransformation(row)
    this.batch.push(...row);
    this.PIPELINE_STATE.cached++
	return this.skipTable
  }
      
}

export { PostgresOutputManager as default }