"use strict"

const { performance } = require('perf_hooks');

const YadamuWriter = require('../../common/yadamuWriter.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');

class MySQLWriter extends YadamuWriter {

  constructor(dbi,tableName,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,status,yadamuLogger)
  }
  
  setTableInfo(tableName) {
	super.setTableInfo(tableName)   
	this.tableInfo.columnCount = this.tableInfo.columnNames.length;
    this.tableInfo.args =  '(' + Array(this.tableInfo.columnCount).fill('?').join(',')  + ')'; 
    
	this.transformations = this.tableInfo.targetDataTypes.map((targetDataType,idx) => {
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
      switch (dataType.type.toLowerCase()) {
        case "json" :
          return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
	      }
          break;
        case "geometry" :
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
            if (typeof col !== 'string') {
              col = col.toISOString();
            }             
            col = col.substring(0,10) + ' '  + (col.endsWith('Z') ? col.substring(11).slice(0,-1) : (col.endsWith('+00:00') ? col.substring(11).slice(0,-6) : col.substring(11)))
            // Truncate fractional values to 6 digit precision
            // ### Consider rounding, but what happens at '9999-12-31 23:59:59.999999
            return col.substring(0,26);
		  }
		  break;
        default :
          return null
      }
    })

  }
  
  getStatistics()  {
	const results = super.getStatistics()
    results.insertMode = this.tableInfo.insertMode
 	return results;
  }

  async processWarnings(results) {
    // ### Output Records that generate warnings
    if (results.warningCount >  0) {
      const warnings = await this.dbi.executeSQL('show warnings');
      warnings.forEach((warning,idx) => {
        if (warning.Level === 'Warning') {
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode,idx],`${warning.Code} Details: ${warning.Message}.`)
		  this.dbi.yadamu.WARNING_MANAGER.rejectRow(this.tableInfo.tableName,this.batch[idx]);
        }
      })
    }
  }
  
  recodeSpatialColumns(batch,row,msg) {
      
    // If MySQL rejects a WKB record recode the entire batch as WKT
    
    this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,`INSERT ONE`,this.batch.length,row,this.SPATIAL_FORMAT],`${msg} Converting batch to "WKT".`);
    YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch,true)
  }
  
  reportBatchError(operation,cause) {
   	super.reportBatchError(operation,cause,this.batch[0],this.batch[this.batch.length-1])
  }
 
  async writeBatch() {     

    this.rowCounters.batchCount++;


    if (this.tableInfo.insertMode === 'Batch') {
      try {
        await this.dbi.createSavePoint();
        const results = await this.dbi.executeSQL(this.tableInfo.dml,[this.batch]);
        await this.processWarnings(results);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
		this.batch.length = 0;  
        this.rowCounters.written += this.rowCounters.cached;
		this.rowCounters.cached = 0;
        return this.skipTable
      } catch (cause) {
		this.reportBatchError(cause,'INSERT MANY')
        await this.dbi.restoreSavePoint(cause);
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'   
        this.tableInfo.dml = this.tableInfo.dml.slice(0,-1) + this.tableInfo.args
      }
    }
          
    let batchRecoded = false     
    let dml = this.tableInfo.dml
    for (const row in this.batch) {
      // Enable retry after correcting spatial issues.. Break on successful insert or unfixable error
      while (true) {      
        try {
          const results = await this.dbi.executeSQL(dml,this.batch[row])
          await this.processWarnings(results);
		  this.rowCounters.written++;
          break;
        } catch (cause) {
          if (cause.spatialInsertFailed() && !batchRecoded) {
		    this.recodeSpatialColumns(this.batch,row,cause.message)
            batchRecoded = true;
            dml = this.tableInfo.dml.replace(/ST_GeomFromWKB\(\?\)/g,'ST_GeomFromText(?)')
          }
          else {
            await this.handleIterativeError(`INSERT ONE`,cause,row,this.batch[row]);
            break;
          }
        }
      }
      if (this.skipTable) {
        break;
      }
    }     
    this.endTime = performance.now();
    this.batch.length = 0;  
	this.rowCounters.cached = 0;
    return this.skipTable 
  }
}

module.exports = MySQLWriter;