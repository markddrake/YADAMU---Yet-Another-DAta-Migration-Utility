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
          this.tableName = obj.table;
          this.currentTable = this.dbi.getTableWriter(this.tableName);
          await this.currentTable.initialize();
          break;
        case 'data': 
          if (this.currentTable.skipTable !== true) {
		    await this.currentTable.appendRow(obj.data);
            this.rowCount++;
		  }
          if ((this.rowCount % this.feedbackInterval === 0) & !this.currentTable.batchComplete()) {
            this.yadamuLogger.info([`${this.tableName}`],`Rows buffered: ${this.currentTable.batchRowCount()}.`);
          }
          if ((this.currentTable.skipTable !== true) && (this.currentTable.batchComplete())) {
            await this.currentTable.writeBatch(this.status);
            if (this.currentTable.skipTable) {
               this.currentTable.rollbackTransaction();
            }
            if (this.reportBatchWrites && this.currentTable.reportBatchWrites() && !this.currentTable.commitWork(this.rowCount)) {
              this.yadamuLogger.info([`${this.tableName}`],`Rows written:  ${this.rowCount}.`);
            }                    
          }  
          if ((this.currentTable.skipTable !== true) && (this.currentTable.commitWork(this.rowCount))) {
            await this.currentTable.commitTransaction(this.rowCount)
            if (this.reportCommits) {
              this.yadamuLogger.info([`${this.tableName}`],`Rows commited: ${this.rowCount}.`);
            }          
            await this.dbi.beginTransaction();            
          }
		  break;
		case 'eod':
          await this.currentTable.finalize();
          this.reportTableComplete(obj.eod);
		  this.master.setTimings(this.timings)
		  this.currentTable = undefined
  		  break;
	    case 'releaseSlave':
		  await this.dbi.releaseConnection()
		  break;     	
		default:
      }    
	  callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}[${this.dbi.slaveNumber}]._write()`,`"${this.tableName}"`],e);
	  this.currentTable.skipTable = true;
	  try {
        await this.currentTable.rollbackTransaction(e)
        callback();
	  } catch (e) {
        // Passing the exception to callback triggers the onError() event
        callback(e); 
      }

    }
  }

  setMaster(dbWriterMaster) {
	this.master = dbWriterMaster
  }
  
}

module.exports = DBWriterSlave;
