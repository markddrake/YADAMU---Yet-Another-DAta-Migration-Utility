"use strict";

const { performance } = require('perf_hooks');

const YadamuLibrary = require('../common/yadamuLibrary.js')
const DBWriter = require('../common/dbWriter.js');									 

class DBWriterSlave extends DBWriter {

  constructor(dbi,mode,status,yadamuLogger,options) {
    super(dbi,mode,status,yadamuLogger,options);
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
		  this.transactionManager = this.currentTable;
          await this.currentTable.initialize()
		  // Add table specific error handler to output stream
		  const reject = this.rejectionHandlers[this.tableName]
		  this.on('error',function(err){
		    this.yadamuLogger.logException([`${this.constructor.name}.onError()`,`${this.tableName}`],err)
		    reject(err)
	      });
          break;
        case 'data': 
          if (this.currentTable.skipTable === false) {
		    await this.currentTable.appendRow(obj.data);
            this.rowCount++;
		    if ((this.rowCount % this.feedbackInterval === 0) & !this.currentTable.batchComplete()) {
              this.yadamuLogger.info([`${this.tableName}`],`Rows buffered: ${this.currentTable.batchRowCount()}.`);
            }
            if (this.currentTable.batchComplete()) {
              await this.currentTable.writeBatch(this.status);
              if (this.currentTable.skipTable) {
                 this.transactionManager.rollbackTransaction();
              }
              if (this.reportBatchWrites && this.currentTable.reportBatchWrites() && !this.currentTable.commitWork(this.rowCount)) {
                this.yadamuLogger.info([`${this.tableName}`],`Rows written:  ${this.rowCount}.`);
              }                    
            }  
            if (this.currentTable.commitWork(this.rowCount)) {
              await this.currentTable.commitTransaction(this.rowCount)
              if (this.reportCommits) {
                this.yadamuLogger.info([`${this.tableName}`],`Rows commited: ${this.rowCount}.`);
              }          
              await this.dbi.beginTransaction();            
			}
          }
		  break;
		case 'eod':
          await this.currentTable.finalize();
          this.reportTableComplete(obj.eod);
		  this.master.setTimings(this.timings)
		  // Remove Table Specific Error Handler from output stream
   	      this.removeListener('error',this.listeners('error').pop())
		  delete this.rejectionHandlers[this.tableName]
		  this.transactionManager = this.dbi
   	      this.currentTable = undefined
		  break;
		default:
      }    
	  callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}[${this.dbi.slaveNumber}]._write()`,`"${this.tableName}"`],e);
	  this.currentTable.skipTable = true;
	  try {
        await this.transactionManager.rollbackTransaction(e)
        callback();
	  } catch (e) {
        // Passing the exception to callback triggers the onError() event
        callback(e); 
      }
    }
  }
  
  async _final() {
    // this.yadamuLogger.trace([`${this.dbi.DATABASE_VENDOR}`,`Writer`,`${this.dbi.slaveNumber}`],`Finished.`)
	await this.dbi.releaseSlaveConnection()
	this.master.slaveComplete()
  }

  setMaster(dbWriterMaster) {
	this.master = dbWriterMaster
  }
  
}

module.exports = DBWriterSlave;
