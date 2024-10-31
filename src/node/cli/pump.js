
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

class Pump extends YadamuCLI {
    
  async pump(configurationFilePath,jobName) {
	 
	this.command = 'COPY'
    this.yadamu = this.createYadamu()
	const stringWriter = new StringWriter()
	const stringLogger = YadamuLogger.streamLogger(stringWriter,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
    this.yadamu.LOGGER = stringLogger

	this.yadamu.updateParameters({CONFIG: configurationFilePath})
	this.CONFIGURATION = this.loadConfigurationFile()
	
    const startTime = performance.now()
	try {
  	  assert(this.JOB_NAMES.includes(jobName),new ConfigurationFileError(`Job "${jobName}" not found. Valid Job names: "${this.JOB_NAMES}".`))	
      const job = this.CONFIGURATION.jobs[jobName]
	  const startTime = performance.now()
      const summary = await this.executeJob(this.yadamu,this.CONFIGURATION,job)
	  const endTime = performance.now()
	  return {
		startTime   : startTime
      , endTime     : endTime
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
	  , results     : summary
	  , log         : stringWriter.toJSON()
	  }
    } catch (e) {
	  const endTime = performance.now()
	  return {
		startTime   : startTime
      , endTime     : endTime
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
      , metrics     : this.yadamu.METRICS
	  , log         : stringWriter.toJSON()
	  , error       : YadamuLibrary.serializeException(e)
	  }
    }
  }

}

async function main() {
  
  try {
	const yadamuPump = new Pump();
    try {
      const summary = await yadamuPump.performJob()
    } catch (e) {
	  yadamuPump.reportError(e)
    }
    await yadamuPump.close();
  } catch (e) {
    YadamuLibrary.reportError(e)
  }

}

main()

export { Pump as default}