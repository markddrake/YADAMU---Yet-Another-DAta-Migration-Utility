"use strict"   

const fs = require('fs');
const path = require('path');

const DBWriter = require('./dbWriter.js');
const YadamuLogger = require('./yadamuLogger.js');
const FileDBI = require('../file/node/fileDBI.js');

class YadamuRejectManager {
  
  constructor(yadamu,usage,filename) {

    this.usage = usage
    this.filename = filename;
	this.yadamuLogger =  yadamu.getYadamuLogger()
	this.dbi = new FileDBI(yadamu,filename)
	this.dbi.initialize()
	// Use a NULL Logger in production
    // const logger = this.yadamuLogger
	const logger = new YadamuLogger(fs.createWriteStream("\\\\.\\NUL"),{});
    this.writer = new DBWriter(this.dbi,logger);  
	this.ddlComplete = new Promise((resolve,reject) => {
	  this.writer.once('ddlComplete',() => {
 	    resolve(true);
	  })
	})
	this.recordCount = 0
	this.tableWriter = undefined
  }
  
  setSystemInformation(systemInformation) {
	this.systemInformation = systemInformation
  }
  
  setMetadata(metadata) {
	this.metadata = metadata
  }
  
  async rejectRow(tableName,data) {

	if (this.recordCount === 0) {
      const errorFolderPath = path.dirname(this.filename);
      fs.mkdirSync(errorFolderPath, { recursive: true });
      await this.writer.initialize()    
      this.writer.write({systemInformation: this.systemInformation})
	  this.writer.write({metadata: this.metadata})
	  this.writer.write({pause:true})
	  await this.ddlComplete
	  this.tableWriter = this.dbi.getOutputStream(tableName)
	}
	else {
	  if (tableName !== this.tableWriter.tableName) {
    	await new Promise((resolve,reject) => {
		  this.tableWriter.end(null,null,() => {
            resolve()
          })
        })		
		this.tableWriter = this.dbi.getOutputStream(tableName)
	  }
	}
    this.tableWriter.write({data: data})
    this.recordCount++;
  }
  
  async close() {
	if (this.recordCount > 0) {
      await new Promise((resolve,reject) => {
		this.tableWriter.end(null,null,() => {
          resolve()
        })
      })		
	  this.writer.deferredCallback();	
	  await new Promise((resolve,reject) => {
	    this.writer.end(null,null,() => {
          resolve()
        })
      })		
	  await this.dbi.finalize()    
      this.yadamuLogger.info([this.usage],`${this.recordCount} records written to "${this.filename}"`)
	}
  }
}
    
module.exports = YadamuRejectManager;


