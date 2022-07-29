
import fs                     from 'fs'

import { 
  performance 
}                             from 'perf_hooks';

import YadamuWriter           from '../base/yadamuWriter.js';

class DB2Writer extends YadamuWriter {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
     
  reportBatchError(operation,cause,batch,rowCount,info) {
	 super.reportBatchError(operation,cause,this.reassembleRow(batch,0),this.reassembleRow(batch,rowCount),info)
  }
	 
  reassembleRow(batch,rowNumber) {
	return batch.params.map((param) => {
	  return Array.isArray(param) ? param[3][rowNumber] : param.Data[rowNumber]
	})
  }
	 
  async _writeBatch(batch,rowCount) {
	
	batch.ArraySize = rowCount
	
	let idxList = Object.keys(Array(batch.params.length).fill(0))
	
	// Calculate Length for all string based arrays
	
	for (let i=0; i < rowCount && idxList.length > 0; i++) {
	  idxList = idxList.flatMap((idx) => {
	    const value = batch.params[idx].Data[i] 
		if (value !== null) {
		  if (typeof value === 'string') {
		    // const byteLength = this.dbi.DATA_TYPES.UCS2_TYPES.includes(this.tableInfo.targetDataTypes[idx]) ? value.length * 2 : Buffer.byteLength(value)
			// batch.params[idx].SQLType = this.tableInfo.targetDataTypes[idx] === this.dbi.DATA_TYPES.NCLOB_TYPE ? this.dbi.DATA_TYPES.NCLOB_TYPE : batch.params[idx].SQLType
			// Test using ">" to ensure value is set if not already defined (undefined > byteLength is false)
		    const byteLength = Buffer.byteLength(value) 
			batch.params[idx].SQLType = 1
		    batch.params[idx].Length = batch.params[idx].Length > byteLength ? batch.params[idx].Length : byteLength
			return idx
		  }
		  return []
	    }
		return idx
	  })
	}	
	
	/*
	**
	
	if (this.BATCH_METRICS.batchNumber === 1) {
	  console.dir(batch,{depth:null})
      console.log(this.tableInfo)
	}
			
	**
	*/
	
	if (this.tableInfo.insertMode === 'Batch') {
      try {    
        // await this.dbi.createSavePoint();
	    fs.writeFileSync("batch.json",JSON.stringify(batch," ",2))
        const results = await this.dbi.batchInsert(batch);
        this.endTime = performance.now();
        // await this.dbi.releaseSavePoint();
		this.adjustRowCounts(rowCount);
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
	    this.reportBatchError(`INSERT MANY`,cause,batch,rowCount)
        // await this.dbi.restoreSavePoint(cause);
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'    
      }
    }

    let insertCount = rowCount
    this.dbi.SQL_TRACE.enable()

    for (let i=0; i< rowCount; i++) {
      try {
	    const row = this.reassembleRow(batch,i)
		this.tableInfo.paramsTemplate.forEach((param,idx)  => {
		  row[idx] = param.SQLType === 1 ? row[idx] : { ParamType: 'INPUT', SQLType : param.SQLType, Data : row[idx] }
		})
		const results = await this.dbi.executeSQL(batch.sql,row)
		this.adjustRowCounts(1)
        this.dbi.SQL_TRACE.disable()
      } catch (cause) {
        this.dbi.SQL_TRACE.disable()
		await this.handleIterativeError('INSERT ONE',cause,i,this.reassembleRow(batch,i));
        if (this.skipTable) {
		  insertCount = i
          break;
        }
	  }
    } 	
	
		
    this.dbi.SQL_TRACE.enable()
    this.dbi.SQL_TRACE.comment(`Statement executed ${insertCount} times.`)
    
	this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable
  }

  
}

export { DB2Writer as default }