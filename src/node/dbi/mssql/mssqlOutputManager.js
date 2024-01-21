
import { 
  performance 
}                               from "perf_hooks";

import sql from "mssql";						

import YadamuLibrary            from "../../lib/yadamuLibrary.js"
import YadamuSpatialLibrary     from "../../lib/yadamuSpatialLibrary.js"

import {
  RejectedColumnValue
}                               from "../../core/yadamuException.js";

import YadamuDataTypes          from "../base/yadamuDataTypes.js"
import YadamuOutputManager      from "../base/yadamuOutputManager.js"

class MsSQLOutputManager extends YadamuOutputManager {
    
  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }
  
  createBatch() {
	 
	if (this.tableInfo.insertMode === "BCP") {
	  return this.dbi.createBulkOperation(this.dbi.DATABASE_NAME, this.tableInfo.tableName, this.tableInfo.columnNames, this.tableInfo.dataTypeDefinitions) 
	}
	else {
      return new sql.Table()
	}
  }
  
  resetBatch(batch) {
	batch.rows.length = 0;
  }

  generateTransformations(dataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
    let spatialFormat = this.SPATIAL_FORMAT
		
	const dataTypeDefinitions  = YadamuDataTypes.decomposeDataTypes(dataTypes)		
    this.tableInfo.dataTypeDefinitions = dataTypeDefinitions
	return dataTypeDefinitions.map((dataType,idx) => {      
	  switch (dataType.type.toLowerCase()) {
        case this.dbi.DATA_TYPES.JSON_TYPE.toLowerCase():
		  return (col,idx) => {
            return typeof col === "object" ? JSON.stringify(col) : col
		  }
          break;
		case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
		case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
		case this.dbi.DATA_TYPES.SPATIAL_TYPE:
		  // Test based on incoming spatial format. 
		  if (this.SPATIAL_FORMAT === "GeoJSON") {
			spatialFormat = "WKT"
		    return (col,idx) => {
              return YadamuSpatialLibrary.geoJSONtoWKT(col)
		    }
		  }
		  return null
		case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
		  return (col,idx) => {
            return YadamuLibrary.toBoolean(col)
		  }
          break;
        case this.dbi.DATA_TYPES.MSSQL_DATETIME_TYPE:
		  return (col,idx) => {
            if (typeof col === "string") {
              col = col.endsWith("Z") ? col : (col.endsWith("+00:00") ? `${col.slice(0,-6)}Z` : `${col}Z`)
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
		case this.dbi.DATA_TYPES.TIME_TYPE:
        case this.dbi.DATA_TYPES.DATE_TYPE:
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		  return (col,idx) => {
            if (typeof col === "string") {
              col = col.endsWith("Z") ? col : (col.endsWith("+00:00") ? `${col.slice(0,-6)}Z` : `${col}Z`)
            }
            else {
              // Alternative is to rebuild the table with these data types mapped to date objects ....
              col = col.toISOString();
            }
			return col;
		  }
          break;
 		case this.dbi.DATA_TYPES.FLOAT_TYPE:
        case this.dbi.DATA_TYPES.DOUBLE_TYPE:
		  switch (this.dbi.INFINITY_MANAGEMENT) {
		    case "REJECT":
              return (col, idx) => {
			    if (!isFinite(col)) {
			      throw new RejectedColumnValue(this.tableInfo.columnNames[idx],col);
			    }
				return col;
		      }
		    case "NULLIFY":
			  return (col, idx) => {
			    if (!isFinite(col)) {
                  this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName],`Column "${this.tableInfo.columnNames[idx]}" contains unsupported value "${col}". Column nullified.`);
	  		      return null;
				}
			    return col
		      }   
			default:
			  return null;
	      }
		case this.dbi.DATA_TYPES.VARCHAR_TYPE:
	      switch (dataTypes[idx]) {
            case this.dbi.DATA_TYPES.ORACLE_BFILE_TYPE:
		      return (col,idx) => {
                return typeof col === "object" ? JSON.stringify(col) : col
    		  }
            default:
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
	
	try {
      this.rowTransformation(row)	
      this.batch.rows.add.apply(this.batch.rows,row);
 
  	  this.PIPELINE_STATE.cached++;
	  return this.skipTable;
	} catch (e) {
  	  if (e instanceof RejectedColumnValue) {
		// ### Should this use HandleIterative Error ???
        this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName],e.message);
        this.dbi.yadamu.REJECTION_MANAGER.rejectRow(this.tableName,row);
		this.PIPELINE_STATE.skipped++
        return
	  }
	  throw e
	}
  }  
}

export { MsSQLOutputManager as default }