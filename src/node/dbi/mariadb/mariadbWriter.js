"use strict"

import { performance } from 'perf_hooks';

import YadamuWriter from '../base/yadamuWriter.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import {DatabaseError,RejectedColumnValue} from '../../core/yadamuException.js';
																				   

class MariadbWriter extends YadamuWriter {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
  
  getMetrics() {
    const results = super.getMetrics()
    results.insertMode = this.tableInfo.insertMode
    return results;
  }
  
  async processWarnings(results,row) {

    // ### Output Records that generate warnings

    let badRow = 0;

    if (results.warningCount >  0) {
      const warnings = await this.dbi.executeSQL('show warnings');
      // warnings.forEach(async (warning,idx) => {
      for (const warning of warnings) {
        if (warning.Level === 'Warning') {
          let nextBadRow = warning.Message.split('row')
          nextBadRow = parseInt(nextBadRow[nextBadRow.length-1])
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode,nextBadRow],`${warning.Code} Details: ${warning.Message}.`)
   		  // Only write rows to Rejection File in Iterative Mode. 
         
          if ((this.tableInfo.insertMode === 'Iterative') && (badRow !== nextBadRow)) {
            const columnOffset = (nextBadRow-1) * this.tableInfo.columnNames.length
            const row = this.tableInfo.insertMode === 'Batch'  ? batch.slice(columnOffset,columnOffset +  this.tableInfo.columnNames.length) : batch[nextBadRow-1]
	  	    await this.dbi.yadamu.WARNING_MANAGER.rejectRow(this.tableName,row);
            badRow = nextBadRow;
          }
        }
      }
    }
  }
      
  reportBatchError(batch,operation,cause) {
    // Use Slice to add first and last row, rather than first and last value.
	super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
  }
  	
  async _writeBatch(batch,rowCount) {
   // console.log(batch.slice(0,this.tableInfo.columnCount))

   let repackBatch = false

    switch (this.tableInfo.insertMode) {
      case 'Batch':
        try {    
          const args = new Array(rowCount).fill(this.tableInfo.rowConstructor).join(',')
          await this.dbi.createSavePoint();
          const sqlStatement = `${this.tableInfo.dml} ${args}`
          const results = await this.dbi.executeSQL(sqlStatement,batch);
          await this.processWarnings(results,null);
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
          repackBatch = true;
        }
        break;  
      case 'Iterative':     
        const sqlStatement = `${this.tableInfo.dml} ${this.tableInfo.rowConstructor}`
        for (let row =0; row < rowCount; row++) {
          const nextRow = repackBatch ?  batch.splice(0,this.tableInfo.columnCount) : batch[row]
          try {
            const results = await this.dbi.executeSQL(this.tableInfo.dml,nextRow);
            await this.processWarnings(results,nextRow);
		    this.adjustRowCounts(1);
          } catch (cause) {
            this.handleIterativeError(`INSERT ONE`,cause,row,nextRow)
            if (this.skipTable) {
              break;
            }
          }
        }     
        break;
      default:
    }     
   
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable          
  }

}

export { MariadbWriter as default }