
import { 
  performance 
}                               from 'perf_hooks';
						
import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

class RedshiftWriter extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
    return  targetDataTypes.map((targetDataType,idx) => {
      const dataType = YadamuDataTypes.decomposeDataType(targetDataType);
      switch (dataType.type.toLowerCase()) {
		case "tsvector":
        case "json" :
		case "jsonb":
	      // https://github.com/brianc/node-redshift/issues/442
	      return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
          }
        case "boolean" :
 		  return (col,idx) => {
             return YadamuLibrary.toBoolean(col)
          }
          break;
        case "time" :
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              col = components.length === 1 ? components[0] : components[1]
              return col.split('Z')[0]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds();  
            }
		  }
          break;
        case "timetz" :
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              return components.length === 1 ? components[0] : components[1]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds();
            }
		  }
          break;
        case 'date':
        case 'datetime':
        case 'timestamp':
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
              col = col.toISOString();
            }
			return col
		  }
          break;
        default :
		  return null
      }
    })

    // Use a dummy rowTransformation function if there are no transformations required.

	return transformations.every((currentValue) => { return currentValue === null}) 
	? (row) => {} 
	: (row) => {
      transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
      }) 
    }

  }

  
}

export { RedshiftWriter as default }