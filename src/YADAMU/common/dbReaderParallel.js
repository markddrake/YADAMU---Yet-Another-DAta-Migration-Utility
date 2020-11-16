"use strict";

const { performance } = require('perf_hooks');

const Yadamu = require('../common/yadamu.js')
const YadamuLibrary = require('../common/yadamuLibrary.js')
const DBReader = require('../common/dbReader.js');									 

class DBReaderParallel extends DBReader {  

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
    this.dbi.sqlTraceTag = `/* Manager */`;	
  }
  
  async pipelineTables(readerManagerDBI,writerManagerDBI) {
	 
    if (this.schemaInfo.length > 0) {

      const maxWorkerCount = parseInt(this.dbi.yadamu.PARALLEL)
  	  const workerCount = this.schemaInfo.length < maxWorkerCount ? this.schemaInfo.length : maxWorkerCount
      const tasks = [...this.schemaInfo]
      this.yadamuLogger.info(['PIPELINE','PARALLEL',readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,tasks.length,workerCount],`Processing Tables`);

	  const workers = new Array(workerCount).fill(0).map((x,idx) => { 
		const worker = {
		  readerDBI : readerManagerDBI.workerDBI(idx)
        , writerDBI : writerManagerDBI.workerDBI(idx)			
		}
        return worker
	  })
		  	  
	  const results = workers.map((worker,idx) => { 
	    return new Promise(async (resolve,reject) => {
          // ### Await inside a Promise is an anti-pattern ???
		  const writerDBI = await worker.writerDBI
		  const readerDBI = await worker.readerDBI
          let result
          try {
     	    while (tasks.length > 0) {
	          const task = tasks.shift();
              if (task.INCLUDE_TABLE === true) {
  		          result = await this.pipelineTable(task,readerDBI,writerDBI)
			  }
            }
		  } catch (cause) {
		    result = cause
    	    this.yadamuLogger.handleException(['PARALLEL','PIPELINE',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
    	    // this.yadamuLogger.trace(['PARALLEL','PIPELINE',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
		  }		  
		  worker.writerDBI  = writerDBI
		  worker.readerDBI  = readerDBI
		  resolve(result) 
		})
      })
  
	  try {
        // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,tasks.length,workerCount],`Executing`);
	    await Promise.all(workers.map(async(worker,idx) => { 
	      await results[idx]
		  await worker.readerDBI.releaseWorkerConnection()
		  await worker.writerDBI.releaseWorkerConnection()
	    }))
	    // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,tasks.length,workerCount],`Processing Complete`);
   	  } catch (cause) {
	    // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,tasks.length,workerCount],`Processing Failed`);
	    await Promise.all(workers.map(async(worker,idx) => { 
		  const writerDBI = await worker.writerDBI
		  await writerDBI.releaseWorkerConnection()
		  const readerDBI = await worker.readerDBI
		  await readerDBI.releaseWorkerConnection()
	    }))
	  }
    }
  }       
}

module.exports = DBReaderParallel;