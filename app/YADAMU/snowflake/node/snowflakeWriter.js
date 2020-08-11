"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const {BatchInsertError} = require('../../common/yadamuError.js')

class SnowflakeWriter extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {  
	super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  setTableInfo(tableName) {
	super.setTableInfo(tableName)

    this.transformations = this.tableInfo.targetDataTypes.map((targetDataType,idx) => {      
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
	
	  if (YadamuLibrary.isBinaryDataType(dataType.type)) {
		return (col,idx) =>  {
          return col.toString('hex')
		}
      }

	  switch (dataType.type.toUpperCase()) {
        case 'GEOMETRY': 
        case 'GEOGRAPHY':
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (col,idx)  => {
		       return Buffer.isBuffer(col) ? col.toString('hex') : col
			}
          }
		  return null
		case 'JSON':
          return (col,idx) => {
            return JSON.stringify(col)
		  }
        case 'VARIANT':
          return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
		  }
        case "BOOLEAN" :
 		  return (col,idx) => {
             return YadamuLibrary.toBoolean(col)
          }
          break;
        default :
		  return null
      }
    })
	
  }
        
  cacheRow(row) {
	  
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	      
    this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {			
        row[idx] = transformation(row[idx],idx)
      }
	})
	 
    if (this.tableInfo.parserRequired) {
      this.batch.push(...row);
    }
    else {
  	  this.batch.push(row);
    }
	
    this.rowCounters.cached++
	return this.skipTable
	
  }
  
  reportBatchError(operation,cause) {
	if (this.tableInfo.parserRequired) {
      super.reportBatchError(operation,cause,this.batch.slice(0,this.tableInfo.columnCount),this.batch.slice(this.batch.length-this.tableInfo.columnCount,this.batch.length))
	}
	else {
   	  super.reportBatchError(operation,cause,this.batch[0],this.batch[this.batch.length-1])
	}
  }
  
  recodeSpatialColumns(batch,msg) {
    this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,`INSERT MANY`,this.tableInfo.parserRequired,this.rowCounters.cached,this.SPATIAL_FORMAT],`${msg} Converting batch to "WKT".`);
    YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch,!this.tableInfo.parserRequired)
  }  

  async writeBatch() {

    // Snowflake's handling of WKB appears a little 'flaky' :)
    if (this.tableInfo.targetDataTypes.includes('GEOGRAPHY')) {
      this.recodeSpatialColumns(this.batch,`Avoiding known 'WKB' issues.`)
    }
      
	let sqlStatement
    this.rowCounters.batchCount++;
    if (this.tableInfo.insertMode === 'Batch') {
      try {
		sqlStatement = this.tableInfo.dml
		if (this.tableInfo.parserRequired) {
		  sqlStatement = `${sqlStatement}  ${new Array(this.rowCounters.cached).fill(0).map(() => {return this.tableInfo.valuesBlock}).join(',')}`
		}
		const result = await this.dbi.executeSQL(sqlStatement,this.batch);
        this.endTime = performance.now();
        this.batch.length = 0;
        this.rowCounters.written += this.rowCounters.cached;
        this.rowCounters.cached = 0;
        return this.skipTable
      } catch (cause) {
		this.reportBatchError(`INSERT MANY`,cause)
		this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to Iterative mode.`);          
        /*
        if (cause.spatialInsertFailed()) {	
		  this.recodeSpatialColumns(this.batch,cause.message)
		}
        */
		this.tableInfo.insertMode = 'BinarySplit'   
      }
    }
	
    // Suppress SQL Trace after first Iterative operation

    let sqlExectionTime = 0
	const sqlTrace = this.status.sqlTrace
    const startTime = performance.now()
	
	/*
	**
	** By all accounts iterative inserts are to be avoided atall costs with Snowflake. To quote BD "Snowflake is not a good point insert system."
	** 
    ** Two choices 
	** - Client based operations
	** - Serialize the data into VARCHAR_MAX_SIZE chunks and use a Stored Procedure to perform the processing on the server (Avoids sending the same data multiple times, particularly with the "Binary Split" option)
	**
	** Insert options 
	** - "Point Insert": Row by Row - Simple to implement but see comment above   
	** - "Temp Table": Row by Row inserts into a temporary table followed by bulk insert into target table. ### Does using a temp table avoid issues associated with point inserts ???
	** - "Binary Spilt": Use a Binary Split method to identify the problematic row(s)
	**
	*/
	
	// Client side "Binary Split" implementation
	// 2020-07-09T18:49:57.858Z [ERROR][address][Batch]: Read 603. Written 597. Skipped 6. Reader Elapsed Time: 00:00:00.226s. Throughput 2666 rows/s. Writer Elapsed Time: 00:00:47.948s. SQL Exection Time: 00:00:00.000s. Throughput: 12 rows/s.

    let nextBatch
    let rowNumbers
    const batches = [this.batch]
    const rowTracking = [Array.from(Array(this.rowCounters.cached).keys())]
    let operationCount = 0
		
	if (this.tableInfo.parserRequired) {
	  // console.log('Snowflake Writer','Variant',this.batch.slice(0,this.tableInfo.columnNames.length))
      let batchRowCount
	  const columnCount = this.tableInfo.columnNames.length
	  while (batches.length > 0) {
		try {
		  nextBatch = batches.shift();
          rowNumbers = rowTracking.shift();
		  batchRowCount = Math.ceil(nextBatch.length/columnCount)
		  sqlStatement = `${this.tableInfo.dml} ${new Array(batchRowCount).fill(0).map(() => {return this.tableInfo.valuesBlock}).join(',')}`
		  const opStartTime = performance.now()
          operationCount++
          // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,'BINARY',this.tableInfo.parserRequired,rowNumbers[0],rowNumbers[rowNumbers.length-1],batchRowCount],`Operation ${operationCount}`)
          const result = await this.dbi.executeSQL(sqlStatement,nextBatch);
		  sqlExectionTime+= performance.now() - opStartTime
          this.status.sqlTrace = undefined
	      this.rowCounters.written += batchRowCount
	    } catch (cause) {
		  this.status.sqlTrace = undefined
	      if (batchRowCount > 1) {
            batches.push(nextBatch.splice(0,(Math.ceil(batchRowCount/2)*columnCount)),nextBatch)			  
            rowTracking.push(rowNumbers.splice(0,Math.ceil(rowNumbers.length/2)),rowNumbers)
		  }
		  else {
            await this.handleIterativeError(`BINARY`,cause,rowNumbers[0],nextBatch)
            if (this.skipTable) {
              break;
			}
		  }
		}
	  }  
    }
	else {
	  while (batches.length > 0) {
	    try {
		  nextBatch = batches.shift()
          rowNumbers = rowTracking.shift();
		  const opStartTime = performance.now()
          operationCount++  
          // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,'BINARY',this.tableInfo.parserRequired,rowNumbers[0],rowNumbers[rowNumbers.length-1]],`Operation ${operationCount}`)
       	  sqlStatement = this.tableInfo.dml
		  const result = await this.dbi.executeSQL(sqlStatement,nextBatch)
		  sqlExectionTime+= performance.now() - opStartTime
          this.status.sqlTrace = undefined
	      this.rowCounters.written += nextBatch.length
        } catch (cause) {
          this.status.sqlTrace = undefined
	      if (nextBatch.length > 1) {
            batches.push(nextBatch.splice(0,Math.ceil(nextBatch.length/2)),nextBatch)
            rowTracking.push(rowNumbers.splice(0,Math.ceil(rowNumbers.length/2)),rowNumbers)
		  }
		  else {
            await this.handleIterativeError(`BINARY`,cause,rowNumbers[0],nextBatch[0]);
            if (this.skipTable) {
              break;
			}
          }
		}
      }
    }

    this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,'BINARY',this.tableInfo.parserRequired,this.rowCounters.cached,this.rowCounters.skipped],`Binary insert required ${operationCount} operations`)
	this.status.sqlTrace = sqlTrace
    
    this.endTime = performance.now();
    this.batch.length = 0;
    this.rowCounters.cached = 0;
	return this.skipTable
  }
}

module.exports = SnowflakeWriter;