  "use strict"

import { performance } from 'perf_hooks';

import YadamuOutputManager from '../../common/yadamuOutputManager.js';
import YadamuLibrary from '../../common/yadamuLibrary.js';
import YadamuSpatialLibrary from '../../common/yadamuSpatialLibrary.js';
import {DatabaseError,RejectedColumnValue} from '../../common/yadamuException.js';

class MySQLOutputManager extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
   
  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
    return targetDataTypes.map((targetDataType,idx) => { 
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
      switch (dataType.type.toLowerCase()) {
        case "json" :
          return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
          }
          break;
        case "geometry" :
  	    case 'point':
		case 'linestring':
		case 'polygon':
		case 'multipoint':
		case 'multilinestring':
		case 'multipolygon':
		case 'geometrycollection':
	      if (this.SPATIAL_FORMAT === 'GeoJSON') {
            return (col,idx) => {
              return typeof col === 'object' ? JSON.stringify(col) : col
            }
          }
          return null;
        case "boolean":
          return (col,idx) => {
            return YadamuLibrary.booleanToInt(col)
          }
          break; 
        case "date":
        case "time":
        case "datetime":
        case "timestamp":
          return (col,idx) => {
            // If the the input is a string, assume 8601 Format with "T" seperating Date and Time and Timezone specified as 'Z' or +00:00
            // Neeed to convert it into a format that avoiods use of convert_tz and str_to_date, since using these operators prevents the use of Bulk Insert.
            // Session is already in UTC so we safely strip UTC markers from timestamps
            if (col instanceof Date) {
              col = col.toISOString();
            }             
            col = col.substring(0,10) + ' '  + (col.endsWith('Z') ? col.substring(11).slice(0,-1) : (col.endsWith('+00:00') ? col.substring(11).slice(0,-6) : col.substring(11)))
            // Truncate fractional values to 6 digit precision
            // ### Consider rounding, but what happens at '9999-12-31 23:59:59.999999
            return col.substring(0,26);
 		  }
 		  break;
 		case "real":
        case "float":
		case "double":
		case "double precision":
		case "binary_float":
		case "binary_double":
		  switch (this.dbi.INFINITY_MANAGEMENT) {
		    case 'REJECT':
              return (col, idx) => {
			    if (!isFinite(col)) {
			      throw new RejectedColumnValue(this.tableInfo.columnNames[idx],col);
			    }
				return col;
		      }
		    case 'NULLIFY':
			  return (col, idx) => {
			    if (!isFinite(col)) {
                  this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],`Column "${this.tableInfo.columnNames[idx]}" contains unsupported value "${col}". Column nullified.`);
	  		      return null;
				}
			    return col
		      }   
			default:
			  return null;
	      }
 		case "varchar":
		case "text":
		case "longtext":
		case "mediumtext":
		  return (col,idx) => {
			if (typeof col === 'object') {
			  this.transformations[idx] = (col,idx) => {
				return JSON.stringify(col)
			  }
			  return JSON.stringify(col)
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
	
  }

  async setTableInfo(tableName) {
    await super.setTableInfo(tableName)
    this.tableInfo.args =  '(' + Array(this.tableInfo.columnCount).fill('?').join(',')  + ')'; 
  }
 
  cacheRow(row) {
 
    // Apply transformations and cache transformed row.
	
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.

    // this.yadamuLogger.trace([this.constructor.name,'YADAMU WRITER',this.COPY_METRICS.cached],'cacheRow()')    
	
	try {
	  
      this.rowTransformation(row)

      // Rows mode requires an array of column values, rather than an array of rows.

      if (this.tableInfo.insertMode === 'Rows')  {
  	    this.batch.push(...row)
      }
      else {
       
	   this.batch.push(row)
      }

      this.COPY_METRICS.cached++
	  return this.skipTable;
	} catch (e) {
  	  if (e instanceof RejectedColumnValue) {
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],e.message);
        this.dbi.yadamu.REJECTION_MANAGER.rejectRow(this.tableName,row);
		this.COPY_METRICS.skipped++
        return
	  }
	  throw e
	}	
  }  

}

export { MySQLOutputManager as default }