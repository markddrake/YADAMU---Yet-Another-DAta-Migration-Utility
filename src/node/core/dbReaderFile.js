
import { performance }            from 'perf_hooks';
import { pipeline }               from 'stream/promises';

import YadamuLibrary              from '../lib/yadamuLibrary.js'
import YadamuConstants            from '../lib/yadamuConstants.js'

import Yadamu                     from './yadamu.js'
import {YadamuError}              from './yadamuException.js'
import DBReader                   from './dbReader.js';									 

class DBReaderFile extends DBReader {  

  // Use when writing to an Export File... 

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
  }
     
  async pipelineTable(readerDBI,writerDBI,task) {
      
    // this.yadamuLogger.trace(['PIPELINE','SERIAL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,task.TABLE_NAME],`Processing Table`);
	
	const queryInfo = readerDBI.generateSQLQuery(task)
	 
	// Get the Table Readers 
	const inputStreams = await readerDBI.getInputStreams(queryInfo)
    const pipelineMetrics = inputStreams[0].COPY_METRICS
    const outputStreams = await writerDBI.getOutputStreams(queryInfo.MAPPED_TABLE_NAME,pipelineMetrics)
    const yadamuPipeline = new Array(...inputStreams,...outputStreams)

    // this.traceStreamEvents(yadamuPipeline,task.TABLE_NAME)	  
    // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((s) => { return s.constructor.name }).join(' => ')}`)

    pipelineMetrics.pipeStartTime = performance.now();
    await pipeline(yadamuPipeline,{end:false})
	pipelineMetrics.pipeEndTime = performance.now();
	  
  }

  async pipelineTables(taskList,readerDBI,writerDBI) {
	
  	  await this.dbWriter.dbi.ddlComplete
      this.yadamuLogger.info(['PIPELINE','SERIAL',this.dbi.DATABASE_VENDOR,this.dbWriter.dbi.DATABASE_VENDOR],`Processing ${taskList.length} Tables`);
	  
	  while (taskList.length > 0) {
	    const task = taskList.shift()
		try {
  	      await this.pipelineTable(readerDBI,writerDBI,task)
		} catch (cause) {
		  this.yadamuLogger.handleException(['PIPELINE','SERIAL',task.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],cause)
		  taskList.length = 0
		}
	  }
  }

  
}

export { DBReaderFile as default}