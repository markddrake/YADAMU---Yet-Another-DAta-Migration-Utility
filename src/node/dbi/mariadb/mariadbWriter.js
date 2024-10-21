
import { performance } from 'perf_hooks';

import YadamuWriter from '../base/yadamuWriter.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import {DatabaseError,RejectedColumnValue} from '../../core/yadamuException.js';

import fsp from 'fs/promises'																				   

class MariadbWriter extends YadamuWriter {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }
  
  async processWarnings(results,row) {

    // ### Output Records that generate warnings

    let badRow = 0;
   
	
	/*
    **
    
	// MariaDB 2.x Implementation	

    if (results.warningCount >  0) {
      const warnings = await this.dbi.executeSQL('show warnings');
      // warnings.forEach(async (warning,idx) => {
      for (const warning of warnings) {
        if (warning.Level === 'Warning') {
          let nextBadRow = warning.Message.split('row')
          nextBadRow = parseInt(nextBadRow[nextBadRow.length-1])
          this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode,nextBadRow],`${warning.Code} Details: ${warning.Message}.`)
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
	
	**
	*/
	
	// MariadDB 3.x Implementation
	
	if (results.warningCount >  0) {
      const warnings = await this.dbi.executeSQL('show warnings');
      // warnings.forEach(async (w1arning,idx) => {
      for (const warning of warnings) {
        if (warning.Level === 'Warning') {
          let nextBadRow = warning.Message.split('row')
          nextBadRow = parseInt(nextBadRow[nextBadRow.length-1])
          this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode,nextBadRow],`${warning.Code} Details: ${warning.Message}.`)
		  
		  // Only write rows to Rejection File in Iterative Mode. 
		  
          if ((this.tableInfo.insertMode === 'Iterative') && (badRow !== nextBadRow)) {
            const columnOffset = (nextBadRow-1) * this.tableInfo.columnNames.length
            this.dbi.yadamu.WARNING_MANAGER.rejectRow(this.tableName,row);
            badRow = nextBadRow;
          }
        }
      }
    }
  }
    
  async _writeBatch(batch,rowCount) {
	  
    // console.log(batch[0])

    const sqlStatement = `${this.tableInfo.dml} ${this.tableInfo.rowConstructor}`
	        
    switch (this.tableInfo.insertMode) {
      case 'Batch':
        try {    
          await this.dbi.createSavePoint();
          const results = await this.dbi.batch(sqlStatement,batch);
          await this.processWarnings(results,null);
          this.endTime = performance.now();
          await this.dbi.releaseSavePoint();
		  this.adjustRowCounts(rowCount);
	      this.releaseBatch(batch)
		  return this.skipTable
        } catch (cause) {
  	      this.reportBatchError(`INSERT MANY`,cause,batch[0],batch[batch.length-1]) 
	      await this.dbi.restoreSavePoint(cause);
	      this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to v2 bulk insert mode.`);          
		  this.dbi.resetExceptionTracking()
		  
		  /* 
		  **
		  ** Fallback to 2.x style batch insert - Appears to be necessary with very long rowss
		  **
		  ** E.G. Following  no: 1156, SQLState: 08S01) Got packets out of order
		  **
		  */
		  
          try {    
            await this.dbi.createSavePoint();
			const rows = batch.flat()
		    const args = sqlStatement.slice(sqlStatement.indexOf(' values ')+' values '.length)
			const v2InsertStatement = sqlStatement.replace(args,new Array(batch.length).fill(args).join(','))
            const results = await this.dbi.executeSQL(v2InsertStatement,rows);
            await this.processWarnings(results,null);
            this.endTime = performance.now();
            await this.dbi.releaseSavePoint();
		    this.adjustRowCounts(rowCount);
	        this.releaseBatch(batch)
		    return this.skipTable
          } catch (cause) {
  	        this.reportBatchError(`INSERT MANY`,cause,batch[0],batch[batch.length-1]) 
	        await this.dbi.restoreSavePoint(cause);
	        this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
		    this.dbi.resetExceptionTracking()
		  }
		
          this.tableInfo.insertMode = 'Iterative'
        }
      case 'Iterative':     
        for (let row =0; row < rowCount; row++) {
          try {
            const results = await this.dbi.executeSQL(sqlStatement,batch[row]);
            await this.processWarnings(results,batch[row]);
		    this.adjustRowCounts(1);
          } catch (cause) {
            this.handleIterativeError(`INSERT ONE`,cause,row,batch[row])
            if (this.skipTable) {
              break;
            }
          }
        }     
        break;
      default:
    }     
   
        
	
  }

}

export { MariadbWriter as default }