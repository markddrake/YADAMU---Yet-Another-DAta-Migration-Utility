"use strict"

const { performance } = require('perf_hooks');

const WKX = require('wkx');

const {BatchInsertError} = require('../../common/yadamuError.js')
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.tableInfo.columnCount = this.tableInfo.targetDataTypes.length;
    this.tableInfo.args =  '(' + Array(this.tableInfo.columnCount).fill('?').join(',')  + ')'; 
     
    this.transformations = this.tableInfo.dataTypes.map(function(dataType,idx) {
      switch (dataType.type) {
        case "tinyblob" :
        case "blob" :
        case "mediumblob" :
        case "longblob" :
        case "varbinary" :
        case "binary" :
	      return function(col,idx) {
            return Buffer.from(col,'hex');
		  }
          break;
        case "json" :
          return function(col,idx) {
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
          return function(col,idx) {
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
    },this)

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
      warnings.forEach(function(warning,idx) {
        if (warning.Level === 'Warning') {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Warnings reported by bulk insert operation. Details: ${JSON.stringify(warning)}`)
          this.yadamuLogger.writeDirect(`${this.batch[idx]}\n`)
        }
      },this)
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
      
    const newRow = this.tableInfo.dataTypes.map(function(dataType,idx) {
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
    },this)

    const dml = this.tableInfo.dml.replace(/ST_GeomFromWKB\(UNHEX\(\?\)\)/g,'ST_GeomFromText(?)')
     
    try {
      const results = await this.dbi.executeSQL(dml,newRow)
      await this.processWarnings(results);
	  this.rowCounters.written++;
    } catch (e) {
      const errInfo = [dml,newRow]
      this.skipTable = await this.handleInsertError(`INSERT (WKB->WKT)`,this.batch.length,rowNumber,newRow,e,errInfo);
    }    
  }
  
  handleBatchException(cause,message) {
   
    const batchException = new BatchInsertError(cause,this.tableName,this.tableInfo.dml,this.batch.length,this.batch[0],this.batch[this.batch.length-1])
    this.yadamuLogger.handleWarning([this.dbi.DATABASE_VENDOR,this.tableName,`WRITE`,this.insertMode],batchException)

  }
 
  async writeBatch() {     

    this.batchCount++;
   
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
      } catch (e) {
        await this.dbi.restoreSavePoint(e);
		handleBatchException(e,'Batch Insert')
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'   
        this.tableInfo.dml = this.tableInfo.dml.slice(0,-1) + this.tableInfo.args
      }
    }
          
    for (const row in this.batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[row])
        await this.processWarnings(results);
		this.rowCounters.written++;
      } catch (e) {
        if (e.spatialInsertFailed()) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.batch.length}], row ${row}. Spatial Insert ("${this.tableInfo.spatialFormat}") raised: "${e.code}". Converting to "WKT".`);
          await this.recodeSpatialData(this.batch[row],row);
        }
        else {
          const errInfo = [this.tableInfo.dml,this.batch[row]]
          this.skipTable = await this.handleInsertError(`INSERT ONE`,this.batch.length,row,this.batch[row],e,errInfo);
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

module.exports = TableWriter;