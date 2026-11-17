
import fs                             from 'fs';
import path                           from 'path';
import {Readable}                     from 'stream'
import {pipeline}                     from 'stream/promises'

import ArrayReadable                  from '../util/arrayReadable.js'
import ErrorDBI                       from '../dbi/file/errorDBI.js';

import DBWriter                       from './dbWriter.js';
import YadamuLogger                   from './yadamuLogger.js'
import YadamuLibrary                  from '../lib/yadamuLibrary.js'

class YadamuRejectManager {
  
  constructor(yadamu,usage,filename) {

    this.yadamu = yadamu
    this.usage = usage
    this.filename = filename;
	
	this.dbi = new ErrorDBI(yadamu,filename)
    // Moved to initialize
	// this.dbi.initialize();
	
	// Use a NULL Logger in production.
    // this.logger =  YadamuLogger.consoleLogger();
	this.logger = YadamuLogger.NULL_LOGGER;
	
    const errorFolderPath = path.dirname(this.filename);
    fs.mkdirSync(errorFolderPath, { recursive: true });

	this.recordCount = 0
	this.pendingRejects = new Set()
	this.tableName = undefined
	this.outputStreams = undefined
	this.currentOperation = new Promise((resolve,reject) => { resolve() })

    this.is = new ArrayReadable()	  
    this.initialziationCoomplete = new Promise((resolve,reject) => {
	  this.setInitializationComplete = resolve
	  this.setInitializationFailed = reject
    })
	// Only the rejects waiting on it see a failure. The reject that opens the file reports its own.
	this.initialziationCoomplete.catch(() => {})

  }	
  
  setSystemInformation(systemInformation) {
    this.dbi.setSystemInformation(systemInformation)
  
  }
  
  setMetadata(metadata) {
    this.dbi.setMetadata(metadata)
  }
  
  sendEndOfData() {
    this.is.addContent([{eod:{startTime:this.tableStartTime,endTime:performance.now()}}])
  }
  
  async initialize(tableName) {
	this.initialize = async () => { this.sendEndOfData() }
	await this.dbi.initialize()
    await this.dbi.initializeImport();
	await this.dbi.initializeData()
	this.pipeline = pipeline(this.is,...this.dbi.getOutputStreams(tableName,{}),{end:false})
  }
	
  /*
  **
  ** Many callers (writers and output managers handling an iterative error) do not await rejectRow(),
  ** and a record is only counted once the rejection file is open. Track every reject in flight so
  ** close() can wait for them. Otherwise a row rejected just before the operation ends is counted
  ** after close() has checked recordCount, and the file is never finalized.
  **
  */

  rejectData(tableName,data) {
	const reject = this.writeRejectedData(tableName,data)
	// The tracked copy never rejects: a failure still reaches the caller through the returned promise.
	const tracked = reject.then(() => {},() => {})
	this.pendingRejects.add(tracked)
	tracked.then(() => { this.pendingRejects.delete(tracked) })
	return reject
  }

  async writeRejectedData(tableName,data) {
	if (this.tableName !== tableName) {
	  this.tableName = tableName
	  this.tableStartTime = performance.now()
	  try {
        await this.initialize(tableName)
	  } catch (e) {
		// Fail the rejects waiting for the file, so close() does not wait for them forever.
		this.setInitializationFailed(e)
		throw e
	  }
      this.is.addContent([{table:tableName}])
	  this.setInitializationComplete()
	  this.setInitializationComplete = YadamuLibrary.NOOP
    }
	await this.initialziationCoomplete
    this.recordCount+= data.length
	this.is.addContent(data.map((d) => {return {data:d}}))
  }
	
  async rejectRow(tableName,data) {
	await this.rejectData(tableName,[data])
  }
  
  async rejectRows(tableName,data) {
	await this.rejectData(tableName,data)
  }
  
  async close() {
	while (this.pendingRejects.size > 0) {
	  await Promise.all(Array.from(this.pendingRejects))
	}
    if (this.recordCount > 0) {
	  // this.sendEndOfData() 
	  this.is.addContent([null])
	  await this.pipeline
	  await this.dbi.finalizeData();
	  await this.dbi.finalizeImport();
	  await this.logger.close()
      this.yadamu.LOGGER.info([this.usage],`${this.recordCount} records written to "${this.filename}"`)
	}
  }
}
    
export { YadamuRejectManager as default}


