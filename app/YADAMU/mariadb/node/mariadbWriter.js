"use strict"

const { performance } = require('perf_hooks');

const YadamuWriter = require('../../common/yadamuWriter.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');

class MariadbWriter extends YadamuWriter {

  constructor(dbi,tableName,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,status,yadamuLogger)
  
    this.tableInfo.columnCount = this.tableInfo.columnNames.length;
    this.tableInfo.args =  '(' + Array(this.tableInfo.columnCount).fill('?').join(',')  + '),';
	
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
          return null;
      }
    })
	
  }

  async initialize() {
	await super.initialize()
  }
  
  getStatistics() {
    const results = super.getStatistics()
    results.insertMode = this.tableInfo.insertMode
    return results;
  }
  
  cacheRow(row) {
            
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
    	
	this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	})

    // Batch Mode : Create one large array. Iterative Mode : Create an Array of Arrays.    

    if (this.tableInfo.insertMode === 'Iterative') {
      this.batch.push(row);
    }
    else {
      this.batch.push(...row);
    
	}
    
    this.rowCounters.cached++
	return this.skipTable;
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
    
  reportBatchError(operation,cause) {
    // Use Slice to add first and last row, rather than first and last value.
	super.reportBatchError(operation,cause,this.batch.slice(0,this.tableInfo.columnCount),this.batch.slice(this.batch.length-this.tableInfo.columnCount,this.batch.length))
  }
  	
  async writeBatch() {

     // console.log(this.batch.slice(0,this.tableInfo.columnCount))

    this.rowCounters.batchCount++; 
    let repackBatch = false;

    if (this.tableInfo.insertMode === 'Batch') {
      try {    
        // Slice removes the unwanted last comma from the replicated args list.
        const args = this.tableInfo.args.repeat(this.rowCounters.cached).slice(0,-1);
        await this.dbi.createSavePoint();
        const results = await this.dbi.executeSQL(this.tableInfo.dml.slice(0,-1) + args, this.batch);
        await this.processWarnings(results);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
		this.batch.length = 0;  
        this.rowCounters.written += this.rowCounters.cached;
		this.rowCounters.cached = 0;
        return this.skipTable
      } catch (cause) {
		this.reportBatchError(`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        this.tableInfo.dml = this.tableInfo.dml.slice(0,-1) + this.tableInfo.args.slice(0,-1)
        repackBatch = true;
      }
    }

    for (let row =0; row < this.rowCounters.cached; row++) {
      const nextRow = repackBatch ?  this.batch.splice(0,this.tableInfo.columnCount) : this.batch[row]
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,nextRow);
        await this.processWarnings(results);
		this.rowCounters.written++;
      } catch (cause) {
        await this.handleIterativeError(`INSERT ONE`,cause,row,nextRow)
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

module.exports = MariadbWriter;