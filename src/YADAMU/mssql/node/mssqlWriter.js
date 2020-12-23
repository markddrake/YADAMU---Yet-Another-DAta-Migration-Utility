"use strict"

const { performance } = require('perf_hooks');
const YadamuWriter = require('../../common/yadamuWriter.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const NullWriter = require('../../common/nullWriter.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');

class MsSQLWriter extends YadamuWriter {
    
  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  setTableInfo(tableName) {
	super.setTableInfo(tableName)
	this.useNext = 0;
	this.newBatch()
    	
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
      
  newBatch() {
	super.newBatch();
	// console.log('newBatch(): Using Operation',this.useNext)
	this.batch = this.tableInfo.bulkOperations[this.useNext]
	// Exclusive OR (XOR) operator 1 becomes 0, 0 becomes 1.
	this.useNext ^= 1;
  }
  
  releaseBatch(batch) {
	if (Array.isArray(batch.rows)) {
	  batch.rows.length = 0;
	}
  }
  

  getMetrics()  {
	const results = super.getMetrics()
	results.insertMode = this.tableInfo.bulkSupported === true ? 'Bulk' : 'Iterative'
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
	
    this.batch.rows.add(...row);

	this.metrics.cached++;
	return this.skipTable;
  }

  reportBatchError(batch,operation,cause) {
   
    const additionalInfo = {
      columnDefinitions: batch.columns
	}

    super.reportBatchError(operation,cause,batch.rows[0],batch.rows[batch.rows.length-1],additionalInfo)
  }
  
  async _writeBatch(batch,rowCount) {
    this.metrics.batchCount++;
    
    if (this.SPATIAL_FORMAT === 'GeoJSON') {
      YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch.rows,true)
    } 

    if (this.tableInfo.bulkSupported) {
      try {        
        await this.dbi.createSavePoint();
        const results = await this.dbi.bulkInsert(batch);
		this.endTime = performance.now();
        this.metrics.written += rowCount;
		this.releaseBatch(batch)
		return this.skipTable
      } catch (cause) {
        this.reportBatchError(batch,`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
		if (!this.dbi.TRANSACTION_IN_PROGRESS && this.dbi.tediousTransactionError) {
	  	  this.yadamuLogger.warning([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableInfo.tableName}"`],`Transaction aborted following BCP operation failure. Starting new Transaction`);          
		  await this.dbi.recoverTransactionState()
		  await this.beginTransaction()
		}	
	  	this.yadamuLogger.warning([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableInfo.tableName}"`],`Switching to Iterative mode.`);          
        this.tableInfo.bulkSupported = false;
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.
 
    try {
      await this.dbi.cachePreparedStatement(this.tableInfo.dml, this.dataTypes,this.SPATIAL_FORMAT) 
    } catch (cause) {
      this.abortTable()
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`INSERT ONE`,`"${this.tableInfo.tableName}"`],cause);
      this.endTime = performance.now();
      this.releaseBatch(batch)
      return this.skipTable          
    }	
	
	const sqlTrace = this.status.sqlTrace

    for (const row in batch.rows) {
      try {
        const args = {}
        for (const col in batch.rows[0]){
           args['C'+col] = batch.rows[row][col]
        }
        const results = await this.dbi.executeCachedStatement(args);
		this.metrics.written++
        this.status.sqlTrace = NullWriter.NULL_WRITER;
      } catch (cause) {
        this.handleIterativeError(`INSERT ONE`,cause,row,batch.rows[row]);
        if (this.skipTable) {
          break;
		}
      }
    }       
      
	this.status.sqlTrace = sqlTrace
	this.status.sqlTrace.write(this.dbi.traceComment(`Previous Statement repeated ${rowCount} times.`))
	await this.dbi.clearCachedStatement();   
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable
  }

  async finalize(cause) {
	await super.finalize(cause)
    await this.dbi.clearCachedStatement()
  }
  
}

module.exports = MsSQLWriter;