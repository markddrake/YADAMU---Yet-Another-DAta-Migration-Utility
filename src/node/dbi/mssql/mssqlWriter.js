"use strict"

import sql from 'mssql';

import { performance } from 'perf_hooks';
import YadamuWriter from '../base/yadamuWriter.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js';
import {DatabaseError,RejectedColumnValue} from '../../core/yadamuException.js';

import MsSQLConstants from './mssqlConstants.js'

class MsSQLWriter extends YadamuWriter {
   	
  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
    this.dbi.CANCEL_REQUESTED = false
  }
   
  reportBatchError(batch,operation,cause) {
   
    const additionalInfo = {
      columnDefinitions: batch.columns
	}	
	super.reportBatchError(operation,cause,batch.rows[0],batch.rows[batch.rows.length-1],additionalInfo)
  }
  
  async _writeBatch(batch,rowCount) {
	  	  
    // console.log(this.constructor.name,'writeBatch()',this.tableInfo.bulkSupported,)
    // console.dir(batch,{depth:null})
    
    if (this.tableInfo.insertMode === 'BCP') {
      try {       
        await this.dbi.createSavePoint()
        const results = await this.dbi.bulkInsert(batch);
		this.endTime = performance.now();
        this.adjustRowCounts(rowCount)
		this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
        if (this.dbi.isExpectedCancellation(cause)) {
	      await this.dbi.restoreSavePoint(cause);
          this.endTime = performance.now();
          this.releaseBatch(batch)
          return this.skipTable          
	    }
        // Reminder Report Batch Error throws cause if there are lost rows
		this.reportBatchError(batch,`INSERT MANY`,cause)
	    await this.dbi.restoreSavePoint(cause);
		await this.dbi.verifyTransactionState()
	  	this.yadamuLogger.warning([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableName}"`],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative';
      }
    }
    
    // Cannot process table using BCP Mode. Prepare a statement use with record by record processing.
 
    try {
      await this.dbi.cachePreparedStatement(this.tableInfo.dml, this.tableInfo.dataTypeDefinitions, this.SPATIAL_FORMAT) 
    } catch (cause) {
      if (this.rowsLost()) {
		throw cause
      }
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`INSERT ONE`,`"${this.tableName}"`],cause);
      this.endTime = performance.now();
      this.releaseBatch(batch)
      return this.skipTable          
    }	
	
	this.dbi.SQL_TRACE.enable()
	let insertCount = rowCount

    for (const row in batch.rows) {
      try {
        const args = {}
        for (const col in batch.rows[0]){
           args['C'+col] = batch.rows[row][col]
        }
        const results = await this.dbi.executeCachedStatement(args);
		this.adjustRowCounts(1)
        this.dbi.SQL_TRACE.disable()
      } catch (cause) {
		await this.dbi.verifyTransactionState()
        this.handleIterativeError(`INSERT ONE`,cause,row,batch.rows[row]);
        if (this.skipTable) {
		  this.insertCount = row
          break;
		}
      }
    }       

    this.dbi.SQL_TRACE.enable()
    this.dbi.SQL_TRACE.comment(`Statement executed ${insertCount} times.`)

	await this.dbi.clearCachedStatement();   
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable
  
  }

  async doFinal() {
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),this.COPY_METRICS.received,this.COPY_METRICS.cached,this.COPY_METRICS.written,this.COPY_METRICS.skipped,this.COPY_METRICS.lost],'doFinal()')
    await this.dbi.clearCachedStatement()
	await super.doFinal()
  }
  
  async doDestroy(err) {
	 
	
	if ((err && this.dbi.request) && (this.dbi.ON_ERROR === 'ABORT')) {
	  // Need to cancel any outstanding requests (Bulk operations).
	  // Cannot proceed until the operation has completed.
      await this.dbi.cancelRequest(true)
      await this.dbi.clearCachedStatement()
	}
	await super.doDestroy(err)	
  }
    
}

export { MsSQLWriter as default }