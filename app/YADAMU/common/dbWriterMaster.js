"use strict";

const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js')
const DBWriter = require('./dbWriter.js');									 
const DBWriterSlave = require('./dbWriterSlave.js');									 

class DBWriterMaster extends DBWriter {

  constructor(dbi,mode,status,yadamuLogger,options) {
    super(dbi,mode,status,yadamuLogger,options);
    const self = this;
    this.dbi.sqlTraceTag = `/* Master */`;	
	this.slaveCount = 0;
  }      
  
  async initialize() {
    await this.dbi.initializeImport();
	this.targetSchemaInfo = await this.getTargetSchemaInfo()
  }
 
  async _write(obj, encoding, callback) {
    // console.log(new Date().toISOString(),`${this.constructor.name}._write`,Object.keys(obj)[0]);	
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.dbi.setSystemInformation(obj.systemInformation)
          break;
        case 'ddl':
          if ((this.ddlRequired) && (obj.ddl.length > 0) && (this.dbi.isValidDDL())) { 
            await this.dbi.executeDDL(obj.ddl);
            this.ddlComplete = true;
          }
          break;
        case 'metadata':
          await this.setMetadata(obj.metadata);
		  await this.dbi.initializeData();
		  this.emit('ReadyForData');
          break;
	    case 'slaveReleased':
		  // Parallel Master only
          // Slave Counting mechanism is Fragile. 
          // Need a better mechanism to detect that all slaves have terminated.
	      // If slave throws an exception we may not get a slaveReleased message, causing Yadamu to Hang.
          this.slaveCount--
          if (this.slaveCount === 0) {
            this.emit('AllDataWritten');
	      }
		  break;     	
		default:
      }    
	  callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._write()`,`"${this.tableName}"`],e);
	  await this.dbi.abort();
      process.nextTick(() => this.emit('error',e));
      callback(e);
    }3
  }
 
  async _final(callback) {
    try {
	  if (this.timings.length === 0) {
        this.yadamuLogger.info([`${this.constructor.name}`],`DDL only export. No data written.`);
	  }
      else {
        await this.dbi.finalizeData();
		if (Object.keys(this.timings).length === 0) {
		  this.yadamuLogger.info([`${this.constructor.name}`],`No tables found.`);
		}
      }		
      await this.dbi.finalizeImport();
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._final()`,`"${this.currentTable}"`],e);
	  await this.dbi.abort();
      process.nextTick(() => this.emit('error',e));
      callback(e);
    } 
  } 
  
  setTimings(timings) {
	Object.assign(this.timings,timings)
  }
  
  async newSlave(slaveNumber) {
	const dbi = await this.dbi.newSlaveInterface(slaveNumber);
	const slave = new DBWriterSlave(dbi,this.mode,this.status,this.yadamuLogger)
	this.slaveCount++ 
	slave.setMaster(this);
	return slave
  }
  
}

module.exports = DBWriterMaster;
