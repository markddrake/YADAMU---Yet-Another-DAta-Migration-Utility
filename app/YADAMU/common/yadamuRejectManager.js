"use strict"   

const fs = require('fs');
const path = require('path');
const util = require('util');

const {pipeline} = require('stream')

const DBWriter = require('./dbWriter.js');
const YadamuLogger = require('./yadamuLogger.js');
const Pushable = require('./pushable.js');
const PassThrough = require('./yadamuPassThrough.js');
const FileDBI = require('../file/node/fileDBI.js');
const JSONWriter = require('../file/node/jsonWriter.js');

class YadamuRejectManager {
  
  constructor(yadamu,usage,filename) {

    this.yadamu = yadamu
    this.usage = usage
    this.filename = filename;
	this.dbi = new FileDBI(yadamu,filename)
	this.dbi.initialize()
	
	// Use a NULL Logger in production.
    // this.logger =  YadamuLogger.consoleLogger();
	this.logger = YadamuLogger.NUL_LOGGER;
    this.fileWriter = new DBWriter(this.dbi,this.logger);
	
	this.recordCount = 0
	this.jsonWriter = undefined
	this.dataStream = new Pushable({objectMode: true },true);
	this.tableIdx = 0;
  }
  
  setSystemInformation(systemInformation) {
    this.dbi.setSystemInformation(systemInformation)
  }
  
  setMetadata(metadata) {
    this.dbi.setMetadata(metadata)
  }

  buildPipeline(tableName) {
	// console.log('buildPipeline()',tableName,this.dbi.PIPELINE_ENTRY_POINT.writableEnded)
   	this.dataStream = new Pushable({objectMode: true },true);
    this.jsonWriter = new JSONWriter(this.dbi,tableName,undefined,this.tableIdx,{},this.logger)
    // Disable the columnCountCheck when writing an error report
    this.jsonWriter.checkColumnCount = () => {}
	this.tableIdx++
    const passThrough = new PassThrough({objectMode: false},false);
    this.currentPipeline = new Promise((resolve,reject) => {
	  passThrough.on('end',async () => {
  	    // console.log('PassThrough.end()',this.jsonWriter.tableName,this.dbi.PIPELINE_ENTRY_POINT.writableEnded)
   	    passThrough.unpipe(this.dbi.PIPELINE_ENTRY_POINT);
	    this.dataStream.destroy();
	    this.jsonWriter.destroy();
	    passThrough.destroy();
		resolve()
	  })
	}) 	
    const newPipeline = [this.dataStream,this.jsonWriter,passThrough,this.dbi.PIPELINE_ENTRY_POINT]
    pipeline(newPipeline,(err,data) => {
	  if (err) reject(err)
    })
    this.dataStream.pump({table: tableName})
  }
  
  async demolishPipeline() {
    // console.log('demolishPipeline()',this.jsonWriter.tableName,this.dbi.PIPELINE_ENTRY_POINT.writableEnded)
   	this.dataStream.pump(null);
	await this.currentPipeline;
  }
  
  async rejectRow(tableName,data) {
	 
	if (this.recordCount === 0) {
      const errorFolderPath = path.dirname(this.filename);
      fs.mkdirSync(errorFolderPath, { recursive: true });
      await this.fileWriter.initialize()  
   	  await this.dbi.initializeData()
	  this.buildPipeline(tableName)
    }
	
	if (this.jsonWriter && (tableName !== this.jsonWriter.tableName)) {
	  await this.demolishPipeline();
	  this.buildPipeline(tableName)
	}
	
    this.dataStream.pump({data:data})
    this.recordCount++;

  }
  
  async close() {
	
	if (this.recordCount > 0) {
	  this.demolishPipeline();
	  await this.currentPipeline;
	  await new Promise((resolve,reject) => {
	    pipeline([this.dbi.END_EXPORT_FILE,this.dbi.PIPELINE_ENTRY_POINT],(err,data) => {
		  if (err) reject(err)
		  resolve()
        })
	  })
      await this.dbi.finalize()    
      await this.logger.close()
      this.yadamu.LOGGER.info([this.usage],`${this.recordCount} records written to "${this.filename}"`)
	}
  }
}
    
module.exports = YadamuRejectManager;


