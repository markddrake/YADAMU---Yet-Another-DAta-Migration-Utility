
import { performance } from 'perf_hooks';

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuWriter from '../base/yadamuWriter.js';

class RedshiftWriter extends YadamuWriter {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }
  
  reportBatchError(operation,cause,batch) {
    // Use Slice to add first and last row, rather than first and last value.
	super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
  }
      
  async _writeBatch(batch,rowCount) {

    let repackBatch = false;
	
	if (this.tableInfo.insertMode === 'Batch') {
               
      try {
        await this.dbi.createSavePoint();
        let argNumber = 1;
        const args = Array(rowCount).fill(0).map(() => {return `(${this.tableInfo.columnNames.map((operator) => {return `$${argNumber++}`}).join(',')})`}).join(',');
        const sqlStatement = this.tableInfo.dml + args
		const results = await this.dbi.insertBatch(sqlStatement,batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.PIPELINE_STATE.written += rowCount;
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
		this.reportBatchError(`INSERT MANY`,cause,batch)
        await this.dbi.restoreSavePoint(cause);
		this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
		this.dbi.resetExceptionTracking()
        repackBatch = true;
      }
    } 
     
    let argNumber = 1;
    const args = Array(1).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`}).join(',');
    const sqlStatement = this.tableInfo.dml + args
	for (let row = 0; row < rowCount; row++) {
	  const offset = row * this.tableInfo.columnCount
      const nextRow  = repackBatch ? batch.slice(offset,offset + this.tableInfo.columnCount) : batch[row]
      try {
        await this.dbi.createSavePoint();
        const results = await this.dbi.insertBatch(sqlStatement,nextRow);
        await this.dbi.releaseSavePoint();
		this.PIPELINE_STATE.written++;
      } catch(cause) {
        await this.dbi.restoreSavePoint(cause);
        this.handleIterativeError(`INSERT ONE`,cause,row,nextRow);
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

export { RedshiftWriter as default }