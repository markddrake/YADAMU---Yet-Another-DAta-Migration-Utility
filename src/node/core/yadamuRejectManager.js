
import fs            from 'fs';
import path          from 'path';
import {Readable}    from 'stream'
import {pipeline}    from 'stream/promises'

import ArrayReadable from '../util/arrayReadable.js'
import ErrorDBI      from '../dbi/file/errorDBI.js';

import DBWriter      from './dbWriter.js';
import YadamuLogger  from './yadamuLogger.js'

class YadamuRejectManager {
  
  constructor(yadamu,usage,filename) {

    this.yadamu = yadamu
    this.usage = usage
    this.filename = filename;
	
	this.dbi = new ErrorDBI(yadamu,filename)
	this.dbi.initialize()

	// Use a NULL Logger in production.
    // this.logger =  YadamuLogger.consoleLogger();
	this.logger = YadamuLogger.NULL_LOGGER;
	
    const errorFolderPath = path.dirname(this.filename);
    fs.mkdirSync(errorFolderPath, { recursive: true });

	this.recordCount = 0
	this.tableName = undefined
	this.outputStreams = undefined
	this.currentOperation = new Promise((resolve,reject) => { resolve() })

    this.is = new ArrayReadable()	  
    this.initialziationCoomplete = new Promise((resolve,reject) => {
	  this.setInitializationComplete = resolve
    })

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
    await this.dbi.initializeImport();
	await this.dbi.initializeData()
	this.pipeline = pipeline(this.is,...this.dbi.getOutputStreams(tableName,{}),{end:false})
  }
	
  async rejectData(tableName,data) {	
	if (this.tableName !== tableName) {
	  this.tableName = tableName
	  this.tableStartTime = performance.now()
      await this.initialize(tableName)
      this.is.addContent([{table:tableName}])
	  this.setInitializationComplete()
	  this.setInitializationComplete = () => {}
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


