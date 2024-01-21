
import { 
  performance 
}                              from 'perf_hooks'

import Yadamu                  from '../../core/yadamu.js'

import {
  YadamuError             
, BatchInsertError
}                              from '../../core/yadamuException.js'

import YadamuLibrary           from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary    from '../../lib/yadamuSpatialLibrary.js'

import YadamuWriter            from '../base/yadamuWriter.js'
import SnowflakeError          from './snowflakeException.js'

class SnowflakeWriter extends YadamuWriter {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {  
	super(dbi,tableName,pipelineState,status,yadamuLogger)
  }

  reportBatchError(operation,cause,batch) {
	if (this.tableInfo.parserRequired) {
      super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
	}
	else {
   	  super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
	}
  }
    
  async _writeBatch(batch,rowCount) {
	  
	let sqlStatement

	if (this.tableInfo.parserRequired && (this.dbi.countBinding(batch) > this.dbi.ARRAY_BINDING_THRESHOLD)) {
	  // Disable Single Batch Insert. Split into 'n' batches where each batch is (probably) smaller than the ARRAY_BINDING_THRESHOLD
  	  this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`ARRAY_BINDING_THRESHOLD exceeded. Switching to Multi-Batch mode.`);        
      this.tableInfo.insertMode = 'MultiBatch'
    } 
 
    if (this.tableInfo.insertMode === 'Batch') {
				
	  try {
		sqlStatement = `${this.tableInfo.dml}  ${this.tableInfo.parserRequired ? new Array(rowCount).fill(0).map(() => {return this.tableInfo.args}).join(',') : this.tableInfo.args}`
		const result = await this.dbi.executeSQL(sqlStatement,batch);
        this.endTime = performance.now();
        this.adjustRowCounts(rowCount);
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
		if (!cause.requestTooLarge()) {
		  this.reportBatchError(`INSERT MANY`,cause,batch)
		  this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
	    }
		this.dbi.resetExceptionTracking()
		this.tableInfo.insertMode = 'BinarySplit'   
	  }
    }
	
    // Suppress SQL Trace after first Iterative operation

    let sqlExectionTime = 0
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
	let batches = []
    let operationCount = 0
    const rowTracking = [Array.from(Array(rowCount).keys())]
 
	if (this.tableInfo.parserRequired) {
      let batchRowCount

	  if (this.tableInfo.insertMode === "MultiBatch") {   
        const batchRowCount = Math.floor(this.dbi.countBinding(batch)/this.dbi.ARRAY_BINDING_THRESHOLD)
	    const batchLength = batchRowCount * this.tableInfo.columnNames.length	  
	    while (batch.length > 0) {
   	      batches.push(batch.splice(0,batchLength))
		  rowTracking.push(rowTracking[0].splice(0,batchRowCount))
	    }
	    rowTracking.shift()
	  }
	  else {
        batches.push(batch)
	  }
	  
	  const columnCount = this.tableInfo.columnNames.length
	  while (batches.length > 0) {
		try {
		  nextBatch = batches.shift();
          rowNumbers = rowTracking.shift();
		  batchRowCount = Math.ceil(nextBatch.length/columnCount)
		  sqlStatement = `${this.tableInfo.dml} ${new Array(batchRowCount).fill(0).map(() => {return this.tableInfo.args}).join(',')}`
		  const opStartTime = performance.now()
          operationCount++
          // this.LOGGER.trace([this.dbi.DATABASE_VENDOR,this.tableName,'BINARY',this.tableInfo.parserRequired,rowNumbers[0],rowNumbers[rowNumbers.length-1],batchRowCount],`Operation ${operationCount}`)
          const result = await this.dbi.executeSQL(sqlStatement,nextBatch);
		  sqlExectionTime+= performance.now() - opStartTime
          this.dbi.SQL_TRACE.disable()
	      this.adjustRowCounts(batchRowCount)
	    } catch (cause) {
		  this.dbi.SQL_TRACE.disable()
	      if ((batchRowCount > 1 ) && (!YadamuError.lostConnection(cause))){
			// Split the Batch in two and retry
            batches.push(nextBatch.splice(0,(Math.ceil(batchRowCount/2)*columnCount)),nextBatch)			  
            rowTracking.push(rowNumbers.splice(0,Math.ceil(rowNumbers.length/2)),rowNumbers)
			this.dbi.resetExceptionTracking()
		  }
		  else {
			if ((cause instanceof SnowflakeError) && cause.spatialInsertFailed()) {
		      this.handleSpatialError(`BINARY`,cause,rowNumbers[0],nextBatch[0])
			}
			else {
			  // operation,cause,rowNumber,record,info
              this.handleIterativeError(`BINARY`,cause,rowNumbers[0],nextBatch)
			}
            if (this.skipTable) {
              break;
			}
		  }
		}
	  }  
    }
	else {
      batches.push(batch)
	  while (batches.length > 0) {
	    try {
		  nextBatch = batches.shift()
          rowNumbers = rowTracking.shift();
		  const opStartTime = performance.now()
          operationCount++  
          // this.LOGGER.trace([this.dbi.DATABASE_VENDOR,this.tableName,'BINARY',this.tableInfo.parserRequired,rowNumbers[0],rowNumbers[rowNumbers.length-1]],`Operation ${operationCount}`)
       	  sqlStatement = `${this.tableInfo.dml} ${this.tableInfo.args}`
		  const result = await this.dbi.executeSQL(sqlStatement,nextBatch)
		  sqlExectionTime+= performance.now() - opStartTime
          this.dbi.SQL_TRACE.disable()
	      this.adjustRowCounts(nextBatch.length)
        } catch (cause) {
          this.dbi.SQL_TRACE.disable()
	      if ((nextBatch.length > 1) && (!YadamuError.lostConnection(cause))) {
			// Split the Batch in two and retry
            batches.push(nextBatch.splice(0,Math.ceil(nextBatch.length/2)),nextBatch)
            rowTracking.push(rowNumbers.splice(0,Math.ceil(rowNumbers.length/2)),rowNumbers)
			this.dbi.resetExceptionTracking()
		  }
		  else {
			if ((cause instanceof SnowflakeError) && cause.spatialInsertFailed()) {
		      this.handleSpatialError(`BINARY`,cause,rowNumbers[0],nextBatch[0])
			}
			else {
              this.handleIterativeError(`BINARY`,cause,rowNumbers[0],nextBatch[0])
			}
            if (this.skipTable) {
              break;
			}
          }
		}
      }
    }

    // this.LOGGER.trace([this.dbi.DATABASE_VENDOR,this.tableName,'BINARY',this.tableInfo.parserRequired,rowCount,this.PIPELINE_STATE.skipped],`Binary insert required ${operationCount} operations`)
	this.dbi.SQL_TRACE.enable()
	
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable
  }
}

export { SnowflakeWriter as default }