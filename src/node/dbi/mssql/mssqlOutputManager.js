"use strict"

import sql from 'mssql';

import { performance } from 'perf_hooks';
import YadamuOutputManager from '../base/yadamuOutputManager.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import NullWriter from '../../util/nullWriter.js';
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js';
import {DatabaseError,RejectedColumnValue} from '../../core/yadamuException.js';

class MsSQLOutputManager extends YadamuOutputManager {
    
  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
  
  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows

	const decomposedDataTypes  = YadamuLibrary.decomposeDataTypes(this.tableInfo.targetDataTypes)		
    this.tableInfo.decomposedDataTypes = decomposedDataTypes
	return decomposedDataTypes.map((dataType,idx) => {      
	  switch (dataType.type.toLowerCase()) {
        case "json":
		  return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
		  }
          break;
		case 'bit':
        case 'boolean':
		  return (col,idx) => {
            return YadamuLibrary.toBoolean(col)
		  }
          break;
        case "datetime":
		  return (col,idx) => {
            if (typeof col === 'string') {
              col = col.endsWith('Z') ? col : (col.endsWith('+00:00') ? `${col.slice(0,-6)}Z` : `${col}Z`)
            }
            else {
              // Alternative is to rebuild the table with these data types mapped to date objects ....
              col = col.toISOString();
            }
            if (col.length > 23) {
               col = `${col.substr(0,23)}Z`;
            }
			return col;
		  }
          break;
		case "time":
        case "date":
        case "datetime2":
        case "datetimeoffset":
		  return (col,idx) => {
            if (typeof col === 'string') {
              col = col.endsWith('Z') ? col : (col.endsWith('+00:00') ? `${col.slice(0,-6)}Z` : `${col}Z`)
            }
            else {
              // Alternative is to rebuild the table with these data types mapped to date objects ....
              col = col.toISOString();
            }
			return col;
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
		default :
		  return null
      }
    })
	
  }	  

  newBatch() {
	this.COPY_METRICS.cached = 0;
    this.COPY_METRICS.batchNumber++;
	if (this.tableInfo.insertMode === 'BCP') {
	  return this.dbi.createBulkOperation(this.dbi.DATABASE_NAME, this.tableInfo.tableName, this.tableInfo.columnNames, this.tableInfo.dataTypes) 
	}
	else {
      return new sql.Table()
	}
  }
  
  cacheRow(row) {
      
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	
	try {
      this.rowTransformation(row)	
      this.batch.rows.add.apply(this.batch.rows,row);
 
  	  this.COPY_METRICS.cached++;
	  return this.skipTable;
	} catch (e) {
  	  if (e instanceof RejectedColumnValue) {
		// ### Should this use HandleIterative Error ???
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],e.message);
        this.dbi.yadamu.REJECTION_MANAGER.rejectRow(this.tableName,row);
		this.COPY_METRICS.skipped++
        return
	  }
	  throw e
	}
  }  
}

export { MsSQLOutputManager as default }