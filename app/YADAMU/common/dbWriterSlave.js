"use strict";

const { performance } = require('perf_hooks');

const YadamuLibrary = require('../common/yadamuLibrary.js')
const DBWriter = require('../common/dbWriter.js');									 

class DBWriterSlave extends DBWriter {

  constructor(dbi,mode,status,yadamuLogger,options) {
    super(dbi,mode,status,yadamuLogger,options);
    const self = this;
  }      
  
  async initialize() {
  }
 
  async _write(obj, encoding, callback) {
    // console.log(new Date().toISOString(),`${this.constructor.name}._write`,Object.keys(obj)[0]);	
    try {
      switch (Object.keys(obj)[0]) {
		case 'table':
          this.rowCount = 0;
          this.skipTable = false;
          this.tableName = obj.table;
          this.currentTable = this.dbi.getTableWriter(this.tableName);
          await this.currentTable.initialize();
          break;
        case 'data': 
          await this.currentTable.appendRow(obj.data);
          this.rowCount++;
          if ((this.rowCount % this.feedbackInterval === 0) & !this.currentTable.batchComplete()) {
            this.yadamuLogger.info([`${this.constructor.name}`,`${this.tableName}`],`Rows buffered: ${this.currentTable.batchRowCount()}.`);
          }
          if (this.currentTable.batchComplete()) {
            this.skipTable = await this.currentTable.writeBatch(this.status);
            if (this.skipTable) {
               this.dbi.rollbackTransaction();
            }
            if (this.reportBatchWrites && this.currentTable.reportBatchWrites() && !this.currentTable.commitWork(this.rowCount)) {
              this.yadamuLogger.info([`${this.constructor.name}`,`${this.tableName}`],`Rows written:  ${this.rowCount}.`);
            }                    
          }  
          if (this.currentTable.commitWork(this.rowCount)) {
            await this.dbi.commitTransaction(this.rowCount)
            if (this.reportCommits) {
              this.yadamuLogger.info([`${this.constructor.name}`,`${this.tableName}`],`Rows commited: ${this.rowCount}.`);
            }          
            await this.dbi.beginTransaction();            
          }
		  break;
		case 'eod':
          const results = await this.currentTable.finalize();
          this.skipTable = results.skipTable;
          if (!this.skipTable) {
            const elapsedTime = results.endTime - results.startTime;            
            this.reportTableStatistics(elapsedTime,results);
		    this.master.setTimings(this.timings)
          }
		  this.currentTable = undefined
  		  break;
	    case 'releaseSlave':
		  await this.dbi.releaseConnection()
		  this.master.write({slaveReleased:null});
		  break;     	
		default:
      }    
	  callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._write()`,`"${this.tableName}"`],e);
	  await this.dbi.abort();
      process.nextTick(() => this.emit('error',e));
      callback(e);
    }
  }

  setMaster(dbWriterMaster) {
	this.master = dbWriterMaster
  }
  
}

module.exports = DBWriterSlave;
