"use strict"

import { performance } from 'perf_hooks';

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuWriter from '../base/yadamuWriter.js';
import {BatchInsertError} from '../../core/yadamuException.js'

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
  		this.reportBatchError(batch,`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'    
      }
    }

    for (const row in batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row]);
   	    this.adjustRowCounts(1)
      } catch (cause) {
        const errInfo = {}
        this.handleIterativeError(`INSERT ONE`,cause,row,batch[row],errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }     
   
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable     
  }
  
  */
  
}

export { ExampleWriter as default }