
import assert                 from 'assert'

import {
  performance 
}                             from 'perf_hooks';

import {
  CommandLineError, 
  ConfigurationFileError
}                             from '../core/yadamuException.js';

import YadamuCLI              from './yadamuCLI.js'
import Yadamu                 from '../core/yadamu.js'
import StringWriter           from '../util/stringWriter.js'
import YadamuLogger           from '../core/yadamuLogger.js'
import YadamuLibrary          from '../lib/yadamuLibrary.js'

class Batch extends YadamuCLI {
  
  async runBatch(configurationFilePath,batchName) {
	  
	this.command = 'COPY'
    this.yadamu = this.createYadamu()
    this.yadamu.updateParameters({CONFIG: configurationFilePath})
	
	this.CONFIGURATION = this.loadConfigurationFile()
	const startTime = performance.now()
    const results = {
	  startTime : startTime
	}
      
	try {
  	  assert(this.BATCH_NAMES.includes(batchName),new ConfigurationFileError(`Batch "${batchName}" not found. Valid connections: "${this.BATCH_NAMES}".`))	
	} catch(e) {
	  const endTime = performance.now()
	  return {
		startTime : startTime
	  , endTime : endTime
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
	  , metrics     : this.yadamu.METRICS
	  , error       : YadamuLibrary.serializeException(e)
	  }
	}
	
	const batch = this.CONFIGURATION.batchOperations[batchName]

    for (let jobName of batch) {
	  const stringWriter = new StringWriter()
	  const stringLogger = YadamuLogger.streamLogger(stringWriter,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      this.yadamu.LOGGER = stringLogger
		
	  try {
  	    assert(this.JOB_NAMES.includes(jobName),new ConfigurationFileError(`Job "${jobName}" not found. Valid job names: "${this.JOB_NAMES}".`))	
        const job = this.CONFIGURATION.jobs[jobName]
	    const startTime = performance.now()
        const summary = await this.executeJob(this.yadamu,this.CONFIGURATION,job)
        const endTime = performance.now()
	    results[jobName] = {
		  startTime   : startTime
        , endTime     : endTime
	    , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
	    , results     : summary
	    , log         : stringWriter.toJSON()
	    }
      } catch (e) {
	    const endTime = performance.now()
	    results[jobName] = {
		  startTime   : startTime
        , endTime     : endTime
	    , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
        , metrics     : this.yadamu.METRICS
	    , log         : stringWriter.toJSON()
	    , error       : YadamuLibrary.serializeException(e)
	    }
      }
	  this.yadamu.close()
	  this.yadamu.reset()
	}
	results.endTime = performance.now()
	results.elapsedTime = YadamuLibrary.stringifyDuration(results.endTime - startTime)
	return results
  }

}

export { Batch as default}