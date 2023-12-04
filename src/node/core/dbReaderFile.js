
import { performance }            from 'perf_hooks';
import { pipeline }               from 'stream/promises';

import YadamuLibrary              from '../lib/yadamuLibrary.js'
import YadamuConstants            from '../lib/yadamuConstants.js'

import Yadamu                     from './yadamu.js'
import {YadamuError}              from './yadamuException.js'
import DBReader                   from './dbReader.js';									 

class DBReaderFile extends DBReader {  

  // Use when writing to an Export File... 

  get PIPELINE_MODE()               { return 'SERIAL' }

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
  }
    
  removeLeakedListeners(stream,initialListeners,listener) {
    
	const leakedListeners =  stream.listeners(listener).filter((f)  => {return !initialListeners.includes(f)});
	leakedListeners.forEach((f) => {stream.removeListener(listener,f)})
	
  } 	  
	
  async pipelineTable(task,readerDBI,writerDBI) {
      
    // this.yadamuLogger.trace(['PIPELINE',this.CONTROLLER_MODE,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,task.TABLE_NAME],`Processing Table`);
	
	const queryInfo = readerDBI.generateSQLQuery(task)
	 
	// Get the Table Readers 
	const inputStreams = await readerDBI.getInputStreams(queryInfo)
    const pipelineMetrics = inputStreams[0].COPY_METRICS
    const outputStreams = await writerDBI.getOutputStreams(queryInfo.MAPPED_TABLE_NAME,pipelineMetrics)
    const yadamuPipeline = new Array(...inputStreams,...outputStreams)

    // this.traceStreamEvents(yadamuPipeline,task.TABLE_NAME)	  
    // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((s) => { return s.constructor.name }).join(' => ')}`)



    const terminalStream = yadamuPipeline[yadamuPipeline.length-1]
    const initialListeners = {
	  errorListeners : terminalStream.listeners('error')
	, closeListeners : terminalStream.listeners('close')
	, finishListeners : terminalStream.listeners('finsih')
	}	 

	pipelineMetrics.pipeStartTime = performance.now();
    await pipeline(yadamuPipeline,{end:false})
	pipelineMetrics.pipeEndTime = performance.now();
	
	this.removeLeakedListeners(terminalStream,initialListeners.errorListeners,'error')
	this.removeLeakedListeners(terminalStream,initialListeners.closeListeners,'close')
	this.removeLeakedListeners(terminalStream,initialListeners.finishListeners,'finish')
	
  }

}

export { DBReaderFile as default}