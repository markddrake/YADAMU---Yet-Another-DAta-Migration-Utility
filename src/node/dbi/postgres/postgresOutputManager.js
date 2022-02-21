"use strict"

import { performance } from 'perf_hooks';

import Yadamu from '../../common/yadamu.js';
import YadamuLibrary from '../../common/yadamuLibrary.js';
import YadamuOutputManager from '../../common/yadamuOutputManager.js';

class PostgresOutputManager extends YadamuOutputManager  {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
  
    generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 	  
  	return targetDataTypes.map((targetDataType,idx) => {
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
      switch (dataType.type.toLowerCase()) {
		case "tsvector":
        case "json" :
		case "jsonb":
	      // https://github.com/brianc/node-postgres/issues/442
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

  }
  
  cacheRow(row) {
	  
    // if (this.metrics.cached === 1) console.log('postgresWriter',row)
		
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
  	
    this.rowTransformation(row)
    this.batch.push(...row);
    this.COPY_METRICS.cached++
	return this.skipTable
  }
      
}

export { PostgresOutputManager as default }