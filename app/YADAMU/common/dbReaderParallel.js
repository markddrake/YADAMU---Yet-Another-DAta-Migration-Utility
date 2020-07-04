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
  
  async pipelineTables(primaryReaderDBI,primaryWriterDBI) {
	 
    if (this.schemaInfo.length > 0) {
      const maxWorkerCount = parseInt(this.dbi.parameters.PARALLEL)
  	  const workerCount = this.schemaInfo.length < maxWorkerCount ? this.schemaInfo.length : maxWorkerCount
      this.yadamuLogger.info(['PARALLEL',workerCount,this.schemaInfo.length,primaryReaderDBI.DATABASE_VENDOR,primaryWriterDBI.DATABASE_VENDOR],`Processing Tables`);
      const tasks = [...this.schemaInfo]
      const workers = new Array(workerCount).fill(0).map(async (x,idx) => { 
	    // ### Await inside a Promise is an anti-pattern ???
        const readerDBI = await primaryReaderDBI.workerDBI(idx);
        const writerDBI = await primaryWriterDBI.workerDBI(idx)		 
	    return new Promise(async (resolve,reject) => {
		  try {
	        while (tasks.length > 0) {
	          const task = tasks.shift();
  		      await this.pipelineTable(task,readerDBI,writerDBI)
            }
		    await readerDBI.releaseWorkerConnection()
            await writerDBI.releaseWorkerConnection()
		    resolve(idx)
		  } catch(e) {
			await readerDBI.releaseWorkerConnection()
            await writerDBI.releaseWorkerConnection()
		    resolve(e)
		  }
		})
      })
	  const results = await Promise.all(workers);
      // this.yadamuLogger.trace([this.constructor.name,'PARALLEL',workerCount,this.schemaInfo.length,primaryReaderDBI.DATABASE_VENDOR,primaryWriterDBI.DATABASE_VENDOR],`Processing Complete`);
    }
  }       
}

module.exports = DBReaderParallel;