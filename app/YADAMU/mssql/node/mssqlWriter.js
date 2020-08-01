"use strict"

const { performance } = require('perf_hooks');
const YadamuWriter = require('../../common/yadamuWriter.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');

class MsSQLWriter extends YadamuWriter {
    
  constructor(dbi,tableName,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,status,yadamuLogger)
  }
  
  setTableInfo(tableName) {
	super.setTableInfo(tableName)
	this.insertMode = 'Bulk';
    this.dataTypes  = YadamuLibrary.decomposeDataTypes(this.tableInfo.targetDataTypes)

	this.transformations = this.dataTypes.map((dataType,idx) => {      
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
        default :
		  return null
      }
    })

  }
      
  getStatistics()  {
	const results = super.getStatistics()
	results.insertMode = this.tableInfo.bulkSupported === true ? 'Bulk' : 'Iterative'
	return results;
  }
  
  async finalize() {
    await super.finalize()
    if (this.dbi.preparedStatement !== undefined){
      await this.dbi.preparedStatement.unprepare();
	  this.dbi.preparedStatement = undefined;
    }
  }
  
  cacheRow(row) {
      
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	  
	this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	})
	
    this.tableInfo.bulkOperation.rows.add(...row);

	this.rowCounters.cached++;
	return this.skipTable;
  }

  reportBatchError(operation,cause) {
   
    const additionalInfo = {
      columnDefinitions: this.tableInfo.bulkOperation.columns
	}

    super.reportBatchError(operation,cause,this.tableInfo.bulkOperation.rows[0],this.tableInfo.bulkOperation.rows[this.tableInfo.bulkOperation.rows.length-1],additionalInfo)
  }
  
  async writeBatch() {
      
    this.rowCounters.batchCount++;
    
    if (this.SPATIAL_FORMAT === 'GeoJSON') {
      YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,this.tableInfo.bulkOperation.rows,true)
    } 

    if (this.tableInfo.bulkSupported) {
      try {        
        await this.dbi.createSavePoint();
        const results = await this.dbi.bulkInsert(this.tableInfo.bulkOperation);
        this.endTime = performance.now();
        this.tableInfo.bulkOperation.rows.length = 0;
		this.rowCounters.written += this.rowCounters.cached;
		this.rowCounters.cached = 0;
        return this.skipTable
      } catch (cause) {
        this.reportBatchError(`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
		this.yadamuLogger.warning([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableInfo.tableName}"`],`Switching to Iterative mode.`);          
        this.tableInfo.bulkSupported = false;
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.
 
    await this.dbi.cachePreparedStatement(this.tableInfo.dml, this.dataTypes,this.SPATIAL_FORMAT) 

    for (const row in this.tableInfo.bulkOperation.rows) {
      try {
        const args = {}
        for (const col in this.tableInfo.bulkOperation.rows[0]){
           args['C'+col] = this.tableInfo.bulkOperation.rows[row][col]
        }
        const results = await this.dbi.executeCachedStatement(args);
		this.rowCounters.written++
      } catch (cause) {
        await this.handleIterativeError(`INSERT ONE`,cause,row,this.tableInfo.bulkOperation.rows[row]);
        if (this.skipTable) {
          break;
        }
      }
    }       
      
    await this.dbi.clearCachedStatement();   
	
    this.endTime = performance.now();
    this.tableInfo.bulkOperation.rows.length = 0;
	this.rowCounters.cached = 0;
    return this.skipTable
  }
}

module.exports = MsSQLWriter;