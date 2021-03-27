"use strict"   

const fs = require('fs');
const path = require('path');
const util = require('util');

const {pipeline, finished} = require('stream')

const DBWriter = require('./dbWriter.js');
const YadamuLogger = require('./yadamuLogger.js');

const Pushable = require('./pushable.js');
const ErrorDBI = require('../file/node/errorDBI.js');

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
    this.currentPipeline = []
	this.currentTable = undefined

	this.dataStream = new Pushable({objectMode: true},true);
  }	
  
  setSystemInformation(systemInformation) {
    this.dbi.setSystemInformation(systemInformation)
  
  }
  
  setMetadata(metadata) {
    this.dbi.setMetadata(metadata)
  }

  async initializePipeline(tableName) {
    await this.dbi.initializeImport();
	await this.dbi.initializeData()
	this.currentPipeline = new Array(this.dataStream,...this.dbi.getOutputStreams(tableName))
    // console.log(this.currentPipeline.map((s) => { return s.constructor.name }).join(' ==> '))
    pipeline(this.currentPipeline,(err) => {
	  if (err && (err.code === 'ERR_STREAM_PREMATURE_CLOSE')) {
	    this.currentPipeline.forEach((stream) => {
	      if (stream.underlyingError instanceof Error) {
	        console.log(stream.constructor.name,stream.underlyingError)
	      }
	    })
	  }
	})
  }
  
  async checkTableName(tableName) {
	if (this.currentTable !== tableName) {
      this.dataStream.pump({table:tableName})
	  this.currentTable = tableName
	  if (this.recordCount === 0) {
        await this.initializePipeline(tableName)
      }
    }
  }
  
  async rejectRow(tableName,data) {
	
	await this.checkTableName(tableName)
    this.recordCount++;
    this.dataStream.pump({data:data})
  
  }
  
  async rejectRows(tableName,data) {

	await this.checkTableName(tableName)
	data.forEach((row) => {
      this.recordCount++;
      this.dataStream.pump({data:row})
	})
  }
  
  async close() {

	if (this.recordCount > 0) {
 	  const tableSwitcher = this.currentPipeline[2]
	  const tableComplete = new Promise((resolve,reject) => {
	    finished(tableSwitcher,() => {
          resolve()
         })
      })
	  
      this.dataStream.pump(null)	  
  	  await tableComplete;
	  await this.dbi.finalizeData();
	  await this.dbi.finalizeImport();
	  await this.dbi.finalize();
      await this.logger.close()
      this.yadamu.LOGGER.info([this.usage],`${this.recordCount} records written to "${this.filename}"`)
	}
  }
}
    
module.exports = YadamuRejectManager;


