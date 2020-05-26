"use strict";
const Writable = require('stream').Writable
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js');

class DBWriter extends Writable {
  
  constructor(dbi,mode,status,yadamuLogger,options) {

    super({objectMode: true});
    
    this.dbi = dbi;
    this.mode = mode;
    this.ddlRequired = (mode !== 'DATA_ONLY');    
    this.status = status;
    this.yadamuLogger = yadamuLogger;
    this.yadamuLogger.info([`Writer`,`${dbi.DATABASE_VENDOR}`,`${this.mode}`,`${this.dbi.slaveNumber !== undefined ? this.dbi.slaveNumber : 'Master'}`],`Ready.`)
        
    this.transactionManager = this.dbi
	this.currentTable   = undefined;
    this.rowCount       = undefined;
    this.ddlComplete    = false;

    this.configureFeedback(this.dbi.parameters.FEEDBACK); 
	this.tableCount = 0;
    this.timings = {}
	
    this.rejectionHandlers = {}

  }      
  
  configureFeedback(feedbackModel) {
      
    this.reportCommits      = false;
    this.reportBatchWrites  = false;
    this.feedbackCounter    = 0;
    
    if (feedbackModel !== undefined) {
        
      if (feedbackModel === 'COMMIT') {
        this.reportCommits = true;
        return;
      }
  
      if (feedbackModel === 'BATCH') {
        this.reportCommits = true;
        this.reportBatchWrites = true;
        return;
      }
    
      if (!isNaN(feedbackModel)) {
        this.reportCommits = true;
        this.reportBatchWrites = true;
        this.feedbackInterval = parseInt(feedbackModel)
      }
    }      
  }
  
  objectMode() {
    return this.dbi.objectMode(); 
  }

  setInputStreamType(ist) {
	this.inputStreamType = ist
  }
	
  getTimings() {
    return this.timings
  }
  
  generateMetadata(schemaInfo) {
    const metadata = this.dbi.generateMetadata(schemaInfo,false)
    Object.keys(metadata).forEach(function(table) {
       metadata[table].vendor = this.dbi.DATABASE_VENDOR;
    },this)
    return metadata
  }
  
  async generateStatementCache(metadata,ddlRequired) {
    const startTime = performance.now()
    this.dbi.setMetadata(metadata)      
    await this.dbi.generateStatementCache(this.dbi.parameters.TO_USER,!this.ddlComplete)
	let ddlStatementCount = 0
	let dmlStatementCount = 0
	Object.keys(this.dbi.statementCache).forEach(function(tableName) {
	  if (this.dbi.statementCache[tableName].ddl !== null) {
		ddlStatementCount++;
	  }
	  if (this.dbi.statementCache[tableName].dml !== null) {
		dmlStatementCount++;
      }
    },this)	  
	this.yadamuLogger.ddl([`${this.dbi.DATABASE_VENDOR}`],`Generated ${ddlStatementCount === 0 ? 'no' : ddlStatementCount} DDL and ${dmlStatementCount === 0 ? 'no' : dmlStatementCount} DML statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
  }   

  async getTargetSchemaInfo() {
	  
    // Fetch metadata for tables that already exist in the target schema.
       
    const schemaInfo = await this.dbi.getSchemaInfo('TO_USER');
	return schemaInfo

  }
  
  async setMetadata(metadata) {
    
    /*
    **
    ** Match tables in target schema with the metadata from the export source
    **
    ** Determine which tables already exist in the target schema. Process incoming rows based on the metadata from the existing tables.
    ** 
    ** Tables which do not exist in the target schema need to be created.
    **
    */ 
	
    if (this.targetSchemaInfo === null) {
      this.dbi.setMetadata(metadata)      
    }
    else {    
    
       // Copy the source metadata 
    
      Object.keys(metadata).forEach(function(table) {
        const tableMetadata = metadata[table]
        if (!tableMetadata.hasOwnProperty('vendor')) {
           tableMetadata.vendor = this.dbi.systemInformation.vendor;   
        }
        tableMetadata.source = {
          vendor          : tableMetadata.vendor
         ,columns         : tableMetadata.columns
         ,dataTypes       : tableMetadata.dataTypes
         ,sizeConstraints : tableMetadata.sizeConstraints
        }
      },this)
      	  
	
      if (this.targetSchemaInfo.length > 0) {
    
        // Merge metadata for existing table with metadata from export source

        const exportTableNames = Object.keys(metadata)
        const targetMetadata = this.generateMetadata(this.targetSchemaInfo,false)
      
        // Transform tablenames based on TABLE_MATCHING parameter.    

        let targetNamesTransformed = this.targetSchemaInfo.map(function(tableInfo) {
          return tableInfo.TABLE_NAME;
        },this)
          
        let exportNamesTransformed = exportTableNames.map(function(tableName) {
          return tableName;
        },this)

        switch ( this.dbi.parameters.TABLE_MATCHING ) {
          case 'UPPERCASE' :
            exportNamesTransformed = exportNamesTransformed.map(function(tableName) {
              return tableName.toUpperCase();
            },this)
            break;
          case 'LOWERCASE' :
            exportNamesTransformed = exportTableNames.map(function(tableName) {
              return tableName.toLowerCase();
            },this)
            break;
          case 'INSENSITIVE' :
            exportNamesTransformed = exportTableNames.map(function(tableName) {
              return tableName.toLowerCase();
            },this)
            targetNamesTransformed = targetNamesTransformed.map(function(tableName) {
              return tableName.toLowerCase();
            },this)
            break;
          default:
        }
           
        targetNamesTransformed.forEach(function(targetName, idx){
          const tableIdx = exportNamesTransformed.findIndex(function(member){return member === targetName})
          if ( tableIdx > -1)    {
            // Overwrite metadata from source with metadata from target.
            targetMetadata[this.targetSchemaInfo[idx].TABLE_NAME].source = metadata[exportTableNames[tableIdx]].source
            metadata[exportTableNames[tableIdx]] = targetMetadata[this.targetSchemaInfo[idx].TABLE_NAME]
          }
        },this)
      }    
      
      await this.generateStatementCache(metadata,!this.ddlComplete)
    }
  }      
  
  reportTableComplete(readerStatistics) {
	  	  
	const writerStatistics = this.currentTable.getStatistics();
	const readerElapsedTime = readerStatistics.readerEndTime - readerStatistics.pipeStartTime;
    const writerElapsedTime = writerStatistics.endTime - writerStatistics.startTime;        
	const pipeElapsedTime = writerStatistics.endTime - readerStatistics.pipeStartTime;
	const readerThroughput = isNaN(readerElapsedTime) ? 'N/A' : Math.round((readerStatistics.rowsRead/readerElapsedTime) * 1000)
    const writerThroughput = isNaN(writerElapsedTime) ? 'N/A' : Math.round((writerStatistics.counters.committed/writerElapsedTime) * 1000)
	
	let readerStatus = ''
	let rowCountSummary = ''
	
    const readerTimings = `Reader Elapsed Time: ${YadamuLibrary.stringifyDuration(readerElapsedTime)}s. Throughput ${Math.round(readerThroughput)} rows/s.`
	const writerTimings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(writerElapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(writerStatistics.sqlTime))}s. Throughput: ${Math.round(writerThroughput)} rows/s.`
    
	if ((readerStatistics.rowsRead === 0) || (readerStatistics.rowsRead === writerStatistics.counters.committed)) {
      rowCountSummary = `Rows ${readerStatistics.rowsRead}.`
    }
	else {
      rowCountSummary = `Read ${readerStatistics.rowsRead}. Written ${writerStatistics.counters.committed}.`
    }

	rowCountSummary = writerStatistics.counters.skipped > 0 ? `${rowCountSummary} Skipped ${writerStatistics.counters.skipped}.` : rowCountSummary
   
	if (readerStatistics.copyFailed) {
	  rowCountSummary = readerStatistics.tableNotFound === true ? `Table not found.` : `Read operation failed. ${rowCountSummary} `  
      this.yadamuLogger.error([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
	}
	else {
	  if (readerStatistics.rowsRead !== writerStatistics.counters.committed) {
        this.yadamuLogger.error([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
      }
	  else {
        this.yadamuLogger.info([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
	  }
    }	  
		 
    this.timings[this.tableName] = {rowCount: writerStatistics.counters.committed, insertMode: writerStatistics.insertMode,  rowsSkipped: writerStatistics.counters.skipped, elapsedTime: Math.round(writerElapsedTime).toString() + "ms", throughput: Math.round(writerThroughput).toString() + "/s", sqlExecutionTime: Math.round(writerStatistics.sqlTime)};
  }
  
  registerRejectionHandler(tableName,reject) {
	 this.rejectionHandlers[tableName] = reject
  }
  
  async initialize() {
    await this.dbi.initializeImport();
	this.targetSchemaInfo = await this.getTargetSchemaInfo()
  }
  
  abortTable() {

    // this.yadamuLogger.trace([`${this.constructor.name}.abortTable`],``);
	  
	// Set skipTable TRUE. No More rows will be cached for writing. No more batches will be written. Batches that have been written but not commited will be rollbed back.
	// ### Not this may set it on the DBI rather than than the current table is the abort is raised when the current table is not defined (eg before the first table, or between tables, or after the last table.

    this.transactionManager.skipTable = true;
  }
 
  async _write(obj, encoding, callback) {
    const action = Object.keys(obj)[0]
    // console.log(new Date().toISOString(),`${this.constructor.name}._write`,action);
    try {
      switch (action) {
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
          break;
        case 'table':
		  this.tableCount++;
          this.rowCount = 0;
          this.tableName = obj.table;
          this.currentTable = this.dbi.getTableWriter(this.tableName);
		  this.transactionManager = this.currentTable;
		  // Add table specific error handler to output stream
		  this.on('error',function(err){
			this.yadamuLogger.handleException([`${this.constructor.name}.onError()`,`${this.tableName}`],err)
		    this.rejectionHandlers[this.tableName](err)
	      });
	      await this.currentTable.initialize();
          break;
        case 'data':
          if (this.currentTable.skipTable === false) {
            // console.log(new Date().toISOString(),`${this.constructor.name}._write`,action,this.currentTable.skipTable);
  	        await this.currentTable.appendRow(obj.data);
            this.rowCount++;
            if ((this.rowCount % this.feedbackInterval === 0) & !this.currentTable.batchComplete()) {
              this.yadamuLogger.info([`${this.tableName}`,this.currentTable.insertMode],`Rows buffered: ${this.currentTable.batchRowCount()}.`);
            }
            if (this.currentTable.batchComplete()) {
  			  await this.currentTable.writeBatch(this.status);
			  if (this.currentTable.skipTable) {
                await this.transactionManager.rollbackTransaction();
              }
              if (this.reportBatchWrites && this.currentTable.reportBatchWrites() && !this.currentTable.commitWork(this.rowCount)) {
                this.yadamuLogger.info([`${this.tableName}`,this.currentTable.insertMode],`Rows written:  ${this.rowCount}.`);
              }                    
            }  
            if (this.currentTable.commitWork(this.rowCount)) {
              await this.transactionManager.commitTransaction(this.rowCount)
              if (this.reportCommits) {
                this.yadamuLogger.info([`${this.tableName}`,this.currentTable.insertMode],`Rows commited: ${this.rowCount}.`);
              }          
              await this.dbi.beginTransaction();            
			}
          }
          break;
		case 'eod':
          await this.currentTable.finalize();
          this.reportTableComplete(obj.eod);
		  // Remove Table Specific Error Handler from output stream.
   	      this.removeListener('error',this.listeners('error').pop())
		  delete this.rejectionHandlers[this.tableName]
		  this.transactionManager = this.dbi
          this.currentTable = undefined
		  break;
        default:
      }    
      callback();
    } catch (e) {
	this.yadamuLogger.handleException([`Writer`,`${this.dbi.DATABASE_VENDOR}`,`"${this.tableName === undefined ? action : this.tableName}"`],e);
	  this.transactionManager.skipTable = true;
	  try {
        await this.transactionManager.rollbackTransaction(e)
		if ((['systemInformation','ddl','metadata'].indexOf(action) > -1) || this.dbi.abortOnError()){
	      // Errors prior to processing rows are considered fatal
		  callback(e);
		}
		else {
          callback();
		}
	  } catch (e) {
        // Passing the exception to callback triggers the onError() event
        callback(e); 
      }
    }
  }
 
  async _final(callback) {
    try {
	  if (this.mode === "DDL_ONLY") {
        this.yadamuLogger.info([`${this.dbi.DATABASE_VENDOR}`],`DDL only export. No data written.`);
      }
      else {
		if (Object.keys(this.timings).length === 0) {
		  this.yadamuLogger.info([`${this.dbi.DATABASE_VENDOR}`],`No tables found.`);
		}
        await this.dbi.finalizeData();
	  }
      await this.dbi.finalizeImport();
      await this.dbi.releaseMasterConnection()
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.dbi.DATABASE_VENDOR}`,`"${this.currentTable}"`],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 
   
}

module.exports = DBWriter;
