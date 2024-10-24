
import { performance }            from 'perf_hooks';
import { pipeline }               from 'stream/promises';

import YadamuLibrary              from '../lib/yadamuLibrary.js'
import YadamuConstants            from '../lib/yadamuConstants.js'

import Yadamu                     from './yadamu.js'
import {YadamuError}              from './yadamuException.js'
import DBReader                   from './dbReader.js';									 

import DBIConstants               from '../dbi/base/dbiConstants.js'

class DBReaderFile extends DBReader {  

  // Use when writing to an Export File... 

  get PIPELINE_MODE()               { return 'SERIAL' }

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
  }
    
  removePipleineListeners(event,stream,initialListeners) {
	const additionalListeners =  stream.listeners(event).filter((f)  => {return !initialListeners.includes(f)});
	additionalListeners.forEach((f) => {stream.removeListener(event,f)})	
  } 	  
	
  async pipelineTable(task,readerDBI,writerDBI,retryOnError) {
	 
	retryOnError = retryOnError && (readerDBI.ON_ERROR === 'RETRY')
      
    // this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,task.TABLE_NAME],`Processing Table`);
	
    const pipelineState = DBIConstants.PIPELINE_STATE

	const queryInfo = readerDBI.generateSQLQuery(task)
	 
	// Get the Table Readers 
    const inputStreams = await readerDBI.getInputStreams(queryInfo,pipelineState)
    const outputStreams = await writerDBI.getOutputStreams(queryInfo.MAPPED_TABLE_NAME,pipelineState)
    const yadamuPipeline = new Array(...inputStreams,...outputStreams)
	const outputStreamState = yadamuPipeline[yadamuPipeline.length-1].STREAM_STATE

    // this.traceStreamEvents(yadamuPipeline,task.TABLE_NAME)	  
    
	// this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((s) => { return s.constructor.name }).join(' => ')}`)

    const persistentStream = yadamuPipeline[yadamuPipeline.length-1]
    const initialListeners = {
	  errorListeners : persistentStream.listeners('error')
	, closeListeners : persistentStream.listeners('close')
	, finishListeners : persistentStream.listeners('finsih')
	, endListeners : persistentStream.listeners('end')
	}	 
    
    try {	
	  pipelineState.startTime = performance.now();
      await pipeline(yadamuPipeline,{end:false})
	  pipelineState.endTime = performance.now();
	  // 'Finish' event suppressed by end:false - Need to explicitly set endTime 
	  outputStreamState.endTime = pipelineState.endTime
	  writerDBI.reportPipelineStatus(pipelineState)	  
	} catch (err) {		
	  pipelineState.endTime = performance.now();
	  // 'Finish' event suppressed by end:false - Need to explicitly set endTime 
	  outputStreamState.endTime = pipelineState.endTime

	  let cause = writerDBI.reportPipelineStatus(pipelineState,err)	  

      if (readerDBI.ON_ERROR === 'ABORT') {
  	    // Throw the underlying cause if ON_ERROR handling is ABORT
        throw cause;
      }
	  
      if (YadamuError.lostConnection(pipelineState[DBIConstants.INPUT_STREAM_ID].error) || YadamuError.lostConnection(pipelineState[DBIConstants.PARSER_STREAM_ID].error)) {
        // If the reader or parser failed with a lost connection error re-establish the input stream connection 
  	    await readerDBI.reconnect(cause,'READER')

      }
	  else {
        this.yadamuLogger.handleException(['PIPELINE',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,queryInfo.MAPPED_TABLE_NAME],cause)
	  }

	  if (retryOnError) {
		await this.retryPipelineTable(task,readerDBI,writerDBI,pipelineState)
	  }

	} finally {
	  this.removePipleineListeners('error',persistentStream,initialListeners.errorListeners,)
	  this.removePipleineListeners('close',persistentStream,initialListeners.closeListeners,)
	  this.removePipleineListeners('finish',persistentStream,initialListeners.finishListeners)
	  this.removePipleineListeners('end',persistentStream,initialListeners.finishListeners)
	}
    
	
    return pipelineState  
	
  }

}

export { DBReaderFile as default}