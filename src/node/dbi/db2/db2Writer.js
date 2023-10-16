
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
	
    batch.params.filter((param) => {
	  switch (param.SQLType) {
		case this.dbi.DATA_TYPES.CHAR_TYPE:
        case this.dbi.DATA_TYPES.NCHAR_TYPE:
        case this.dbi.DATA_TYPES.VARCHAR_TYPE:
        case this.dbi.DATA_TYPES.NARCHAR_TYPE:
        case this.dbi.DATA_TYPES.CLOB_TYPE:
        case this.dbi.DATA_TYPES.NCLOB_TYPE:
		  for (const value of param.Data) {
  		    const byteLength = Buffer.byteLength(value)
   		    param.Length = param.Length > byteLength ? param.Length : byteLength
		  }
		  break;
        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
		  for (const value of param.Data) {
   		    param.Length = param.Length > value.length ? param.Length : value.length
		  }
		  break;
	  }
	   
    })
		
	console.dir(batch,{depth:null})

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
        this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
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