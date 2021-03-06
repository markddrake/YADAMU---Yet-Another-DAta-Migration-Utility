"use strict";

const { performance } = require('perf_hooks');

const Yadamu = require('../common/yadamu.js')
const YadamuLibrary = require('../common/yadamuLibrary.js')
const {YadamuError} = require('../common/yadamuException.js')
const DBReader = require('../common/dbReader.js');									 

class DBReaderParallel extends DBReader {  

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
    this.dbi.sqlTraceTag = `/* Manager */`;	
  }
  
  async pipelineTables(taskList,readerManagerDBI,writerManagerDBI) {
	 
    this.activeWorkers = new Set()

    const maxWorkerCount = parseInt(this.dbi.yadamu.PARALLEL)
    const workerCount = taskList.length < maxWorkerCount ? taskList.length : maxWorkerCount

	const workers = new Array(workerCount).fill(0).map((x,idx) => { 
	  const worker = {
		readerDBI : readerManagerDBI.workerDBI(idx)
      , writerDBI : writerManagerDBI.workerDBI(idx)			
	  }
      return worker
	})
	  
	let operationAborted = false;
	let fatalError = undefined
		  	  
	const copyOperations = workers.map((worker,idx) => { 
	  return new Promise(async (resolve,reject) => {
        // ### Await inside a Promise is an anti-pattern ???
	    const writerDBI = await worker.writerDBI
		const readerDBI = await worker.readerDBI
		worker.writerDBI  = writerDBI
		worker.readerDBI  = readerDBI
        let result = undefined
        try {
     	  while (taskList.length > 0) {
	        const task = taskList.shift();
		    await this.pipelineTable(task,readerDBI,writerDBI)
          }
		} catch (cause) {
		  result = cause
		  // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
		  this.yadamuLogger.handleException(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
		  if ((this.dbi.ON_ERROR === 'ABORT') && !operationAborted) {
			fatalError = result
    	    operationAborted = true
			if (taskList.length > 0) {
		      this.yadamuLogger.error(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR],`Operation failed: Skipping ${taskList.length} Tables`);
			}
			else {
		      this.yadamuLogger.warning(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR],`Operation failed.`);
			}		
            taskList.length = 0;
  			this.activeWorkers.forEach(async (reader) => {
			  this.activeWorkers.delete(reader)
    	      // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,this.dbi.getWorkerNumber()],`Killing instance of ${readerDBI.constructor.name}`)
              const readerAbort = new YadamuError('ABORT: Worker terminated following sibling failure.')
	          reader.destroy(readerAbort)
			})
          }
		}		  
		await readerDBI.releaseWorkerConnection()
		await writerDBI.releaseWorkerConnection()
		resolve(result) 
      })
    })
  
    this.yadamuLogger.info(['PIPELINE','PARALLEL',workerCount,readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR],`Processing ${taskList.length} Tables`);
    const results = await Promise.allSettled(copyOperations)
	if (operationAborted) throw fatalError
    // this.yadamuLogger.trace(['PIPELINE','PARALLEL',workerCount,readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,taskList.length],`Processing Complete`);
  }       
}

module.exports = DBReaderParallel;