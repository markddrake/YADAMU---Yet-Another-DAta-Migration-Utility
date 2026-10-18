
import { performance } from 'perf_hooks';

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuWriter from '../base/yadamuWriter.js';

class PostgresWriter extends YadamuWriter {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }

  // Values bound per row. Generated columns are inserted as DEFAULT, so their values are not bound.
  get BOUND_COLUMN_COUNT() { return this.tableInfo.boundColumnCount || this.tableInfo.columnCount }

  reportBatchError(operation,cause,batch) {
    // Use Slice to add first and last row, rather than first and last value.
	super.reportBatchError(operation,cause,batch.slice(0,this.BOUND_COLUMN_COUNT),batch.slice(batch.length-this.BOUND_COLUMN_COUNT,batch.length))
  }

  generateValuesClause(rowCount) {
    let argNumber = 1;
    return Array(rowCount).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.includes('$%') ? operator.replace('$%',`$${argNumber++}`) : operator}).join(',')})`}).join(',');
  }
      
  async _writeBatch(batch,rowCount) {
   
    // console.log(this.tableInfo,batch)

	if (this.tableInfo.insertMode === 'Batch') {
               
      try {
        await this.dbi.createSavePoint();
        const args = this.generateValuesClause(rowCount)
        const sqlStatement = this.tableInfo.dml + args
		const results = await this.dbi.insertBatch(sqlStatement,batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.adjustRowCounts(rowCount);
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
		this.reportBatchError(`INSERT MANY`,cause,batch)
        await this.dbi.restoreSavePoint(cause);
		this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
		this.dbi.resetExceptionTracking()
        this.tableInfo.insertMode = 'Iterative' 
      }
    } 
     
    const args = this.generateValuesClause(1)
    const sqlStatement = this.tableInfo.dml + args
	for (let row = 0; row < rowCount; row++) {
	  const offset = row * this.BOUND_COLUMN_COUNT
      const nextRow  = batch.slice(offset,offset + this.BOUND_COLUMN_COUNT)
	  try {
        await this.dbi.createSavePoint();
        const results = await this.dbi.insertBatch(sqlStatement,nextRow);
        await this.dbi.releaseSavePoint();
		this.adjustRowCounts(1);
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

export { PostgresWriter as default }