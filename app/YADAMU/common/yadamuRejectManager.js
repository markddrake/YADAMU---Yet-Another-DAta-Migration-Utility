"use strict"   

const fs = require('fs');
const path = require('path');
const util = require('util');

const {pipeline} = require('stream')

const DBWriter = require('./dbWriter.js');
const YadamuLogger = require('./yadamuLogger.js');

const Pushable = require('./pushable.js');
const ErrorDBI = require('../file/node/errorDBI.js');
const PassThrough = require('./yadamuPassThrough.js');

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
    this.fileWriter = new DBWriter(this.dbi,this.logger);
	
	this.initialzied = false;
	this.recordCount = 0
  	this.dataStream = new Pushable({objectMode: true},true);
	
    const errorWriter = this.dbi.getOutputStream()
	const passThrough = new PassThrough({objectMode: false},false);
    passThrough.on('end',async () => {
   	  // console.log('JSON Writer: finish',dbi.PIPELINE_ENTRY_POINT.writableEnded)
      passThrough.unpipe();
	  this.dataStream.destroy();
	  errorWriter.destroy();
	  passThrough.destroy();
      pipeline([this.dbi.END_EXPORT_FILE,this.dbi.PIPELINE_ENTRY_POINT],(err) => {
   	    // console.log('EOF Pipeline: Finish',this.dbi.PIPELINE_ENTRY_POINT.writableEnded,err)
        if (err) {throw(err)}
      })
	})
    this.errorPipeline = [this.dataStream,errorWriter,passThrough]
 	  
    this.currentTable = undefined
    const errorFolderPath = path.dirname(this.filename);
    fs.mkdirSync(errorFolderPath, { recursive: true });
  }	
  
  setSystemInformation(systemInformation) {
    this.dbi.setSystemInformation(systemInformation)
  }
  
  setMetadata(metadata) {
    this.dbi.setMetadata(metadata)
  }

  async rejectRow(tableName,data) {
	 
	if (this.currentTable === undefined) {
	  this.currentTable = tableName
      this.dataStream.pump({table:tableName})
      await this.fileWriter.initialize()  
   	  await this.dbi.initializeData()
      this.errorPipeline.push(this.dbi.PIPELINE_ENTRY_POINT)
      pipeline(this.errorPipeline,(err) => {
        if (err) { console.log(err) } else { console.log('Pipe operation #1 complete') }
      })  
    }
	
	if (this.currentTable !== tableName) {
      this.dataStream.pump({table:tableName})
	  this.currentTable = tableName
	}

    this.recordCount++;
    this.dataStream.pump({data:data})

  }
  
  async close() {
	if (this.recordCount > 0) {
      this.dataStream.pump(null)
  	  await this.dbi.pipelineComplete;
	  await this.dbi.finalize()    
      await this.logger.close()
      this.yadamu.LOGGER.info([this.usage],`${this.recordCount} records written to "${this.filename}"`)
	}
  }
}
    
module.exports = YadamuRejectManager;


