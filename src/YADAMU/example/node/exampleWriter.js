"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const {BatchInsertError} = require('../../common/yadamuException.js')

class ExampleWriter extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  setTableInfo(tableInfo) {
	super.setTableInfo(tableInfo)

	this.transformations = this.tableInfo.targetDataTypes.map((targetDataType,idx) => {        
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
      /*	
	  if (YadamuLibrary.isBinaryDataType(dataType.type)){
        // For Interfaces that what Binary content rendered as hexBinary string 
        return (col,idx) => {
		  return (Buffer.isBuffer(col)) return col.toString('hex') : col
          }
	    } 
	  }
	  */
	  switch (dataType.type.toLowerCase()) {
        case "json":
		  return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
		  }
          break;
        case 'bit':
        case 'boolean':
		  return (col,idx) => {
            return YadamuLibrary.toBoolean(col)
		  }
          break;
        default :
		  return null
      }
    })

  }
  
  cacheRow(row) {
	  
	/*
	**
	** The default implimentation is shown below. It applies any transformation functions that have were defiend in setTableInfo andt
	** pushes the row into an array or rows waiting to fed to a batch insert mechanism
	**
	** If your override this function you must ensure that this.metrics.cached is incremented once for each call to cache row.
	** 
	** Also if your solution does not cache one row in this.batch for each row processed you will probably need to override the following 
	** functions in addtion to cache row.
	**
	**  batchComplete() : returns true when it it time to perform a bulk insert.
	**   
	**  handleBatchException(): creates an exception containing a summary of the records being inserted if an error occurs during a batch insert.
    **	

	this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	})
	
    this.batch.push(row);
	
	this.metrics.cached++
	return this.skipTable;
	
	**
	*/
	
	super.cacheRow(row)

  }
    
  reportBatchError(batch,operation,cause) {
   	super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
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
		this.metrics.written += rowCount;
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
		this.handleBatchException(batch,`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        
      }
    }

    for (const row in batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row]);
   	    this.metrics.written++
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

module.exports = ExampleWriter;