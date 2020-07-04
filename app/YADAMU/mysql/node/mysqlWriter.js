"use strict"

const { performance } = require('perf_hooks');

const WKX = require('wkx');

const YadamuWriter = require('../../common/yadamuWriter.js');

class MySQLWriter extends YadamuWriter {

  constructor(dbi,tableName,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,status,yadamuLogger)
    this.warningManager = this.dbi.yadamu.warningManager
  }
  
  setTableInfo(tableInfo) {
	super.setTableInfo(tableInfo)   
	this.tableInfo.columnCount = this.tableInfo.targetDataTypes.length;
    this.tableInfo.args =  '(' + Array(this.tableInfo.columnCount).fill('?').join(',')  + ')'; 
    
    this.transformations = this.tableInfo.dataTypes.map((dataType,idx) => {
      switch (dataType.type.toLowerCase()) {
        case "tinyblob" :
        case "blob" :
        case "mediumblob" :
        case "longblob" :
        case "varbinary" :
        case "binary" :
	      return (col,idx) => {
            return Buffer.from(col,'hex');
		  }
          break;
        case "json" :
          return (col,idx) => {
            if (typeof col === 'object') {
              return JSON.stringify(col);
            }
			return col
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
		  this.warningManager.rejectRow(this.tableInfo.tableName,this.batch[idx]);
        }
      })
    }
  }
  
  async recodeSpatialData(row,rowNumber) {  

    // Convert spatial data in current row from WKB to WKT and retry Insert...
    // Update current insert statement to reflect change in spatial data format.
    // 
    // ### TODO: Convert all remaining rows in current batch ? Convert all remaining batches 
    
    if ((this.tableInfo.spatialFormat === 'WKT') || (this.tableInfo.spatialFormat === 'EWKT')) {
      return false;
    }
      
    const newRow = this.tableInfo.dataTypes.map((dataType,idx) => {
      switch (dataType.type) {
        case "geography":
        case "geometry": 
          if (row[idx] !== null) {
            return WKX.Geometry.parse(Buffer.from(row[idx],'hex')).toWkt()
          }
          else {
            return row[idx]
          }
          break;
        default:  
          return row[idx]
      }
    })

    const dml = this.tableInfo.dml.replace(/ST_GeomFromWKB\(UNHEX\(\?\)\)/g,'ST_GeomFromText(?)')
     
    try {
      const results = await this.dbi.executeSQL(dml,newRow)
      await this.processWarnings(results);
	  this.rowCounters.written++;
    } catch (cause) {
      await this.handleIterativeError(`INSERT [WKB=>WKT]`,cause,rowNumber,newRow);
    }    
  }
  
  handleBatchError(operation,cause) {
   	super.handleBatchError(operation,cause,this.batch[0],this.batch[this.batch.length-1])
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
		this.handleBatchError(cause,'INSERT MANY')
        await this.dbi.restoreSavePoint(cause);
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'   
        this.tableInfo.dml = this.tableInfo.dml.slice(0,-1) + this.tableInfo.args
      }
    }
          
    for (const row in this.batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[row])
        await this.processWarnings(results);
		this.rowCounters.written++;
      } catch (cause) {
        if (cause.spatialInsertFailed()) {
          this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,`INSERT ONE`,this.batch.length,row,this.tableInfo.spatialFormat],`${cause.message} Converting to "WKT".`);
          await this.recodeSpatialData(this.batch[row],row);
        }
        else {
          await this.handleIterativeError(`INSERT ONE`,cause,row,this.batch[row]);
        }
        if (this.skipTable) {
          break;
        }
      }
    }     
	
    this.endTime = performance.now();
    this.batch.length = 0;  
	this.rowCounters.cached = 0;
    return this.skipTable 
  }
}

module.exports = MySQLWriter;