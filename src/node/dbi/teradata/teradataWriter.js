
import { performance }         from 'perf_hooks';

import WKX                     from 'wkx';

import Yadamu                  from '../../core/yadamu.js';
import {BatchInsertError}      from '../../core/yadamuException.js'
import SpatialLibrary          from '../../lib/yadamuSpatialLibrary.js';
import YadamuLibrary           from '../../lib/yadamuLibrary.js';
import YadamuWriter            from '../base/yadamuWriter.js';

const MAX_CHARACTER_SIZE = 16777216
const ROW_SIZE_FUDGE_FACTOR = 160

class TeradataWriter extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  reportBatchError(batch, operation,cause) {
    super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
  }

  async _writeBatch(batch) {
	  
	/*
	**
	
	console.log(this.tableInfo)
	console.log(batch)
	
	**
    */
		
	if (this.tableInfo.insertMode === 'Batch') {
      try {
		const result = await this.dbi.executeSQL(this.tableInfo.dml,batch);
		this.adjustRowCounts(batch.length)
        this.endTime = performance.now();
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
   	    /*
        if (cause.message.includes('{fn teradata_nativesql}{fn teradata_get_errors}') > -1) {
          const results = await this.dbi.executeSQL('{fn teradata_nativesql}{fn teradata_get_errors}')
		  console.log(results);
		}
		*/
		this.reportBatchError(batch,`INSERT MANY`,cause)
		this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'   
      }
    }
	
	let sqlExectionTime = 0
    const startTime = performance.now()
    for (const row in batch) {
      try {
		const opStartTime = performance.now()
		const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row])
		sqlExectionTime+= performance.now() - opStartTime
	    // Suppress SQL Trace for Iterative Inserts after the first insert
		this.dbi.SQL_TRACE.disable()
   	    this.adjustRowCounts(1)
	  } catch (cause) {
        await this.handleIterativeError(`INSERT ONE`,cause,row,batch[row]);
        if (this.skipTable) {
          break;
        }
      }
    }   
	    
    this.dbi.SQL_TRACE.enable()
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable  
  }
}

export { TeradataWriter as default }