
import { 
  performance 
}                           from 'perf_hooks';

import YadamuWriter         from '../base/yadamuWriter.js';

class ExampleWriter extends YadamuWriter {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
     
  /*
  **
  ** Establish a Savepoint
  ** Attempt a batch insert operation
  ** If the batch insert fails restore to save point and attempt an iterative (row by row) insert
  **
  ** The code for a simplified implementation is shown below. Is is extremely unlikely that this code would be sufficient
  ** for a the real-world. It is important that any implementation handles incrementing and resetting metrics correctly.
  **	
  
  async _writeBatch(batch,rowCount) {
	  
    if (this.tableInfo.insertMode === 'Batch') {
      try {    
        await this.dbi.createSavePoint();
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
		this.adjustRowCounts(rowCount);
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
  		this.reportBatchError(`INSERT MANY`,cause,batch[0],batch[batch.length-1])
        await this.dbi.restoreSavePoint(cause);
        this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'    
      }
    }

    let insertCount = rowCount
    this.dbi.SQL_TRACE.enable()

    for (const row in batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row]);
   	    this.adjustRowCounts(1)
        this.dbi.SQL_TRACE.disable()
      } catch (cause) {
        this.dbi.SQL_TRACE.comment(`Previous Statement repeated ${row} times.`)
        this.dbi.SQL_TRACE.enable()
        const errInfo = {}
        this.handleIterativeError(`INSERT ONE`,cause,row,batch[row],errInfo);
        if (this.skipTable) {
		  let insertCont = row
          break;
        }
      }
    }     
   
    this.dbi.SQL_TRACE.enable()
    this.dbi.SQL_TRACE.comment(`Previous Statement repeated ${insertCount} times.`)

    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable     
  }
  
  */
  
}

export { ExampleWriter as default }