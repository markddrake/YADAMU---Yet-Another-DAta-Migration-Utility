"use strict";
const Writable = require('stream').Writable
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js');

class DBWriter extends Writable {
  
  constructor(dbi,mode,status,yadamuLogger,options) {

    super({objectMode: true });
    const self = this;
    
    this.dbi = dbi;
    this.mode = mode;
    this.ddlRequired = (mode !== 'DATA_ONLY');    
    this.status = status;
    this.yadamuLogger = yadamuLogger;
    this.yadamuLogger.info([`${this.constructor.name}`,`${dbi.DATABASE_VENDOR}`],`Ready. Mode: ${this.mode}.`)
        
    this.errorHandler   = undefined;
	this.currentTable   = undefined;
    this.rowCount       = undefined;
    this.ddlComplete    = false;

    this.configureFeedback(this.dbi.parameters.FEEDBACK); 
	this.tableCount = 0;
    this.timings = {}
	
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
    
  setErrorHandler(errorHandler) {
	 this.errorHandler = errorHandler
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
	this.yadamuLogger.info([`${this.constructor.name}.generateStatementCache()`],`Generated ${this.dbi.statementCache ? Object.keys(this.dbi.statementCache).length : 'no'} DDL and DML statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
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
    const writerThroughput = isNaN(writerElapsedTime) ? 'N/A' : Math.round((writerStatistics.rowsCommitted/writerElapsedTime) * 1000)
	
	let readerStatus = ''
	let rowCountSummary = ''
	
    const readerTimings = `Reader Elapsed Time: ${YadamuLibrary.stringifyDuration(readerElapsedTime)}s. Throughput ${Math.round(readerThroughput)} rows/s.`
	const writerTimings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(writerElapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(writerStatistics.sqlTime))}s. Throughput: ${Math.round(writerThroughput)} rows/s.`
    
	if ((readerStatistics.rowsRead === 0) || (readerStatistics.rowsRead === writerStatistics.rowsCommitted)) {
      rowCountSummary = `Rows ${readerStatistics.rowsRead}.`
    }
	else {
      rowCountSummary = `Read ${readerStatistics.rowsRead}. Written ${writerStatistics.rowsCommitted}.`
    }

	rowCountSummary = writerStatistics.rowsSkipped > 0 ? `${rowCountSummary} Skipped ${writerStatistics.rowsSkipped}.` : rowCountSummary
   
	if (readerStatistics.copyFailed) {
	  rowCountSummary = readerStatistics.tableNotFound === true ? `Table not found.` : `Read operation failed. ${rowCountSummary} `  
      this.yadamuLogger.error([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
	}
	else {
	  if (readerStatistics.rowsRead !== writerStatistics.rowsCommitted) {
        this.yadamuLogger.error([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
      }
	  else {
        this.yadamuLogger.info([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
	  }
    }	  
		 
    this.timings[this.tableName] = {rowCount: writerStatistics.rowsCommitted, insertMode: writerStatistics.insertMode,  rowsSkipped: writerStatistics.rowsSkipped, elapsedTime: Math.round(writerElapsedTime).toString() + "ms", throughput: Math.round(writerThroughput).toString() + "/s", sqlExecutionTime: Math.round(writerStatistics.sqlTime)};
  }
  
  
  async initialize() {
    await this.dbi.initializeImport();
	this.targetSchemaInfo = await this.getTargetSchemaInfo()
  }
  
  abortTable() {
	  
	// Set skipTable TRUE. No More rows will be cached for writing. No more batches will be written. Batches that have been written but not commited will be rollbed back.

    // this.yadamuLogger.trace([`${this.constructor.name}.abortTable`],``);
	if (this.currentTable !== undefined) {
	  this.currentTable.skipTable = true;
	}
	else {
	  this.currentTable = { skipTable: true}
	}
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
          await this.currentTable.initialize();
          break;
        case 'data': 
          // console.log(new Date().toISOString(),`${this.constructor.name}._write`,action,this.currentTable.skipTable);
          if (this.currentTable.skipTable !== true) {
		    await this.currentTable.appendRow(obj.data);
            this.rowCount++;
          }
          if ((this.rowCount % this.feedbackInterval === 0) & !this.currentTable.batchComplete()) {
            this.yadamuLogger.info([`${this.tableName}`,this.currentTable.insertMode],`Rows buffered: ${this.currentTable.batchRowCount()}.`);
          }
          if ((this.currentTable.skipTable !== true) && (this.currentTable.batchComplete())) {
			await this.currentTable.writeBatch(this.status);
			if (this.currentTable.skipTable) {
              await this.currentTable.rollbackTransaction();
            }
            if (this.reportBatchWrites && this.currentTable.reportBatchWrites() && !this.currentTable.commitWork(this.rowCount)) {
              this.yadamuLogger.info([`${this.tableName}`,this.currentTable.insertMode],`Rows written:  ${this.rowCount}.`);
            }                    
          }  
          if ((this.currentTable.skipTable !== true) && (this.currentTable.commitWork(this.rowCount))) {
            await this.currentTable.commitTransaction(this.rowCount)
            if (this.reportCommits) {
              this.yadamuLogger.info([`${this.tableName}`,this.currentTable.insertMode],`Rows commited: ${this.rowCount}.`);
            }          
            await this.dbi.beginTransaction();            
          }
          break;
		case 'eod':
          await this.currentTable.finalize();
          this.reportTableComplete(obj.eod);
          this.currentTable = undefined
		  // Remove Table Specific Error Handlers
		  if (this.errorHandler) {
		    this.removeListener('error',this.errorHandler)		  
			this.errorHandler = undefined
		  }
		  break;
        default:
      }    
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._write()`,`"${this.tableName}"`],e);
	  this.currentTable.skipTable = true;
	  try {
        await this.currentTable.rollbackTransaction(e)
		if (['systemInformation','ddl','metadata'].indexOf(action) > -1) {
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
        this.yadamuLogger.info([`${this.constructor.name}`],`DDL only export. No data written.`);
      }
      else {
		if (Object.keys(this.timings).length === 0) {
		  this.yadamuLogger.info([`${this.constructor.name}`],`No tables found.`);
		}
        await this.dbi.finalizeData();
	  }
      await this.dbi.finalizeImport();
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._final()`,`"${this.currentTable}"`],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 
}

module.exports = DBWriter;
