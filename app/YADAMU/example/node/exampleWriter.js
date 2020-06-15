"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const {BatchInsertError} = require('../../common/yadamuError.js')

class ExampleWriter extends YadamuWriter {

  constructor(dbi,primary,status,yadamuLogger) {
    super({objectMode: true},dbi,primary,status,yadamuLogger)
  }
  
  setTableInfo(tableInfo) {
	super.setTableInfo(tableInfo)

    this.transformations = this.tableInfo.dataTypes.map((dataType,idx) => {
      switch (dataType.type) {
		/*
		**
		** Add conversion functions for specific data types here..
        FOO:
		   return (col,idx) => { conversion code for the data type of foo col }
		**
	    */
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
	** If your override this function you must ensure that this.rowCounters.cached is incremented once for each call to cache row.
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
	
	this.rowCounters.cached++
	return this.skipTable;
	
	**
	*/
	
	super.cacheRow(row)

  }
    
      
  async writeBatch() {
	  
	/*
	**
	** Establish a Savepoint
	** Attempt a batch insert operation
	** If the batch insert fails restore to save point and attempt an iterative (row by row) insert
	**
	** The code for a simplified implementation is shown below. Is is extremely unlikely that this code would be sufficient
	** for a the real-world. It is important that any implementation handles incrementing and resetting counters correctly.
	**
		
    if (this.tableInfo.insertMode === 'Batch') {
      try {    
        await this.dbi.createSavePoint();
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
		this.batch.length = 0;  
        this.rowCounters.written += this.rowCounters.cached;
		this.rowCounters.cached = 0;
        return this.skipTable
      } catch (e) {
        await this.dbi.restoreSavePoint(e);
		this.handleBatchException(e,'Batch Insert')
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        
      }
    }

    for (const row in batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row]);
   	    this.rowCounters.written++
      } catch (e) {
        const errInfo = [this.tableInfo.dml]
        await this.handleInsertError(`INSERT ONE`,this.rowCounters.cached,row,batch[row],e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }     
   
    this.endTime = performance.now();
    this.batch.length = 0;
    this.rowCounters.cached = 0;
    return this.skipTable     
	
	*/
	
	return super.writeBatch)()
  }
}

module.exports = ExampleWriter;