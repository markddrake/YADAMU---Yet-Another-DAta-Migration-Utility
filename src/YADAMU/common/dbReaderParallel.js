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
      this.yadamuLogger.info(['PIPELINE','PARALLEL',readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,this.schemaInfo.length,workerCount],`Processing Tables`);
      const tasks = [...this.schemaInfo]
      const workers = new Array(workerCount).fill(0).map(async (x,idx) => { 
	    // ### Await inside a Promise is an anti-pattern ???
        const readerDBI = await readerManagerDBI.workerDBI(idx);
        const writerDBI = await writerManagerDBI.workerDBI(idx)		 
	    return new Promise(async (resolve,reject) => {
		  try {
			let writerComplete
	        while (tasks.length > 0) {
	          const task = tasks.shift();
              if (task.INCLUDE_TABLE === true) {
  		        writerComplete = await this.pipelineTable(task,readerDBI,writerDBI)
              }
            }
			// Make sure the writer has finished before closing the connection.
			await writerComplete
			await readerDBI.releaseWorkerConnection()
            await writerDBI.releaseWorkerConnection()
		    resolve(idx)
		  } catch (cause) {
	        this.yadamuLogger.handleException(['PARALLEL','PIPELINES',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
			await readerDBI.releaseWorkerConnection()
            await writerDBI.releaseWorkerConnection()
		    resolve(cause)
		  }
		})
      })
      // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,this.schemaInfo.length,workerCount],`Processing`);
	  const results = await Promise.all(workers);
      // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerManagerDBI.DATABASE_VENDOR,writerManagerDBI.DATABASE_VENDOR,this.schemaInfo.length,workerCount],`Processing Complete`);
    }
  }       
}

module.exports = DBReaderParallel;