
import { performance } from 'perf_hooks';

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuWriter from '../base/yadamuWriter.js';

class CockroackWriter extends YadamuWriter {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }

  commitWork() {
	// Cockroach does not really support SavePoints so we must commit after every batch 
	return true
  }
      
  reportBatchError(operation,cause,batch) {
    // Use Slice to add first and last row, rather than first and last value.
	super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
  }

  async _writeBatch(batch,rowCount) {
   
	let repackBatch = false;

    // console.log(batch)

	if (this.tableInfo.insertMode === 'Batch') {
               
	  // SavePoints are no-ops in Cockroach as they don't really work the way one would expect
	  
      try {
        await this.dbi.createSavePoint();
        let argNumber = 1;
        const args = Array(rowCount).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`}).join(',');
        const sqlStatement = this.tableInfo.dml + args
		const results = await this.dbi.insertBatch(sqlStatement,batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.adjustRowCounts(rowCount);
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
		await this.reportBatchError(`INSERT MANY`,cause,batch)
        await this.dbi.restoreSavePoint(cause);
		this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
		this.dbi.resetExceptionTracking()
		await this.dbi.rollbackTransaction()
		await this.dbi.beginTransaction()
        repackBatch = true;
      }
    } 
     
    let argNumber = 1;
    const args = Array(1).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`}).join(',');
    const sqlStatement = this.tableInfo.dml + args
	
	let retryCount = 0
    await this.dbi.retryableOperation(async () => {
	  
	  let rowsWritten = 0;
      for (let row = 0; row < rowCount; row++) {
        const offset = row * this.tableInfo.columnCount;
        const nextRow = repackBatch ? batch.slice(offset, offset + this.tableInfo.columnCount) : batch[row];
    
        try {
          await this.dbi.createSavePoint(); // no-op
          const results = await this.dbi.executeSQL(sqlStatement, nextRow);
          await this.dbi.releaseSavePoint(); // no-op
		  rowsWritten++
          // this.adjustRowCounts(1);
		  
        } catch (cause) {
          // If it's a transaction abort, throw it up to retryableOperation
          if (cause.transactionAborted && cause.transactionAborted()) {
            throw cause; // Abort entire batch, retry from row 0
          }
      
          // Otherwise it's a row-specific error - log and continue
          await this.handleIterativeError(`INSERT ONE`, cause, row, nextRow);
        }
     
	    if (this.skipTable) {
          break;
        }
      } 

      // Commit all successfully processed rows
      await this.dbi.commitTransaction();
	  this.adjustRowCounts(rowsWritten)
      await this.dbi.beginTransaction();
    }, 'ITERATIVE INSERT');
	
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable   
  }
  
}

export { CockroackWriter as default }