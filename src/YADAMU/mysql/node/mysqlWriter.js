  "use strict"

const { performance } = require('perf_hooks');
const util = require('util');

const YadamuWriter = require('../../common/yadamuWriter.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');
const {DatabaseError,RejectedColumnValue} = require('../../common/yadamuException.js');

class MySQLWriter extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
   
  setTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
    const transformations = targetDataTypes.map((targetDataType,idx) => { 
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
			  transformations[idx] = (col,idx) => {
				return JSON.stringify(col)
			  }
			  return JSON.stringify(col)
			}
			else {
			  transformations[idx] = null
			  return col
			}		  
		  }
       default :
          return null
      }
    })
	
    // Use a dummy rowTransformation function if there are no transformations required.

	return transformations.every((currentValue) => { currentValue === null}) 
	? (row) => {} 
	: (row) => {
      transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
      }) 
    }

  }

  setTableInfo(tableName) {
    super.setTableInfo(tableName)

    this.tableInfo.columnCount = this.tableInfo.columnNames.length;
    this.tableInfo.args =  '(' + Array(this.tableInfo.columnCount).fill('?').join(',')  + ')'; 
    this.rowTransformation  = this.setTransformations(this.tableInfo.targetDataTypes)
  }
  
  getMetrics()  {
	const results = super.getMetrics()
    results.insertMode = this.tableInfo ? this.tableInfo.insertMode : 'Batch'
 	return results;
  }

  cacheRow(row) {
 
    // Apply transformations and cache transformed row.
	
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.

    // this.yadamuLogger.trace([this.constructor.name,'YADAMU WRITER',this.metrics.cached],'cacheRow()')    
	
	try {
	  
      this.rowTransformation(row)

      // Rows mode requires an array of column values, rather than an array of rows.

      if (this.tableInfo.insertMode === 'Rows')  {
  	    this.batch.push(...row)
      }
      else {
       
	   this.batch.push(row)
      }

      this.metrics.cached++
	  return this.skipTable;
	} catch (e) {
  	  if (e instanceof RejectedColumnValue) {
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],e.message);
        this.dbi.yadamu.REJECTION_MANAGER.rejectRow(this.tableName,row);
		this.metrics.skipped++
        return
	  }
	  throw e
	}
	
  }  

  async processWarnings(results,row) {

    // ### Output Records that generate warnings

    let badRow = 0;

    if (results.warningCount >  0) {
      const warnings = await this.dbi.executeSQL('show warnings');
      // warnings.forEach(async (w1arning,idx) => {
      for (const warning of warnings) {
        if (warning.Level === 'Warning') {
          let nextBadRow = warning.Message.split('row')
          nextBadRow = parseInt(nextBadRow[nextBadRow.length-1])
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode,nextBadRow],`${warning.Code} Details: ${warning.Message}.`)
		  
		  // Only write rows to Rejection File in Iterative Mode. 
		  
          if ((this.tableInfo.insertMode === 'Iterative') && (badRow !== nextBadRow)) {
            const columnOffset = (nextBadRow-1) * this.tableInfo.columnNames.length
            this.dbi.yadamu.WARNING_MANAGER.rejectRow(this.tableName,row);
            badRow = nextBadRow;
          }
        }
      }
    }
  }
  
  recodeSpatialColumns(batch,msg) {
    this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableName,`INSERT ROWS`,this.metrics.cached,this.SPATIAL_FORMAT],`${msg} Converting batch to "WKT".`);
    YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch,false)
  }  
  
  async retryGeoJSONAsWKT(sqlStatement,rowNumber,row) {
    YadamuSpatialLibrary.recodeSpatialColumns('GeoJSON','WKT',this.tableInfo.targetDataTypes,row,false)
    try {
	  // Create a bound row by cloning the current set of binds and adding the column value.
      sqlStatement = sqlStatement.replace(/ST_GeomFromGeoJSON\(\?\)/g,'ST_GeomFromText(?)')
      const results = await this.dbi.executeSQL(sqlStatement,row)
      this.metrics.written++
    } catch (cause) {
	  this.handleIterativeError('INSERT ONE',cause,rowNumber,row);
    }
  }
  
  reportBatchError(batch, operation,cause) {
	if (this.tableInfo.insertMode === 'Rows') {
      super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
	}
	else {
   	  super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
	}
  }
  
  async _writeBatch(batch,rowCount) {     
    
	let recodedBatch = false;
    this.metrics.batchCount++;
    // this.yadamuLogger.trace([this.constructor.name,'_writeBatch',this.tableName,this.tableInfo.insertMode,this.metrics.batchCount,rowCount,batch.length],'Start')    
	
    switch (this.tableInfo.insertMode) {
      case 'Batch':
        try {
          await this.dbi.createSavePoint();
		  const bulkInsertStatement =  `${this.tableInfo.dml} ?`
          const results = await this.dbi.executeSQL(bulkInsertStatement,[batch]);
          await this.processWarnings(results,null);
          this.endTime = performance.now();
          await this.dbi.releaseSavePoint();
   		  this.metrics.written += rowCount;
          this.releaseBatch(batch)
          return this.skipTable
        } catch (cause) {
   		  this.reportBatchError(batch,'INSERT MANY',cause)
          await this.dbi.restoreSavePoint(cause);
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Multi-row insert mode.`);     
		  this.tableInfo.insertMode = "Rows"
          batch = batch.flat()
        }
      case 'Rows':
	    if (this.SPATIAL_FORMAT === 'GeoJSON') {
	      recodedBatch = true
          this.recodeSpatialColumns(batch,'Recoding GeoJSON as WKT')
		  this.tableInfo.rowConstructor = this.tableInfo.rowConstructor.replace(/ST_GeomFromGeoJSON\(\?\)/g,'ST_GeomFromText(?)')
		}
	    while (true) {
          try {
            await this.dbi.createSavePoint();    
            const multiRowInsert = `${this.tableInfo.dml} ${new Array(rowCount).fill(this.tableInfo.rowConstructor).join(',')}`
            const results = await this.dbi.executeSQL(multiRowInsert,batch);
            await this.processWarnings(results,null);
            this.endTime = performance.now();
            await this.dbi.releaseSavePoint();
	   	    this.metrics.written += rowCount;
            this.releaseBatch(batch)
            return this.skipTable
          } catch (cause) {
            await this.dbi.restoreSavePoint(cause);
			// If it's a spatial error recode the entire batch and try again.
            if ((cause instanceof DatabaseError) && cause.spatialError() && !recodedBatch) {
              recodedBatch = true;
			  this.recodeSpatialColumns(batch,cause.message)
			  this.tableInfo.rowConstructor = this.tableInfo.rowConstructor.replace(/ST_GeomFromWKB\(\?\)/g,'ST_GeomFromText(?)')
			  continue;
		    }
	   	    this.reportBatchError(batch,'INSERT ROWS',cause)
            this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);    
  		    this.tableInfo.insertMode = "Iterative"
            break;			
		  }
        }
      case 'Iterative':     
	    break;
      default:
    }     
		
    // Batch or Rows failed, or iterative was selected.
	const singleRowInsert = `${this.tableInfo.dml} ${this.tableInfo.rowConstructor}`
	for (let row = 0; row < rowCount; row++) {
	  const offset = row * this.tableInfo.columnCount
      const nextRow  = batch.length > rowCount ? batch.slice(offset,offset + this.tableInfo.columnCount) : batch[row]
      try {
		const results = await this.dbi.executeSQL(singleRowInsert,nextRow)
        await this.processWarnings(results,nextRow);
  	    this.metrics.written++;
      } catch (cause) { 
	    if (cause.spatialErrorGeoJSON()) {
		  await this.retryGeoJSONAsWKT(singleRowInsert,row,nextRow)
	    }
		else {
          this.handleIterativeError(`INSERT ONE`,cause,row,nextRow);
		}
		if (this.skipTable) {
	      break;
		}
      }
	}

    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable 
  }
}

module.exports = MySQLWriter;