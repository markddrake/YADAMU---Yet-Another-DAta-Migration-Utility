"use strict" 
const Writable = require('stream').Writable
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js');

class DBWriter extends Writable {
  
  /*
  **
  ** The DB Writer should always be invoked with the option {end: false}. 
  ** 
  ** If one or more workers are used to process data these should be registered with the DBWriter by calling the DBWriter's registerWorker()
  ** function. Once a worker has completed all of it's tasks it must send a 'workerComplete' message to the DBWriter. Thw worker complete message
  ** must be sent via the same pipe that is sending information to the DBWriter. Typically this is done by invoking the DBWriter's write()  method.
  ** When all workers have trasnmitted "workerComplete" messages the DBWriter sends itsef a 'dataComplete' message. This allows the worker to work 
  ** independantly of each other, and ensures the writer does not receive the 'dataCompete' message until all workers have terminated.
  **
  ** If no workers are instantiated then the DBReader is repsonsible for sending the 'dataComplete' message directory to the DBWriter.
  **
  ** When the DBWriter receieves the 'dataComplete' message it emits a 'dataComplete' event'. The DBReader listens for this event, performs 
  ** any necessary clean-up, such as closing database connections or releasing other resources, and then sends an 'exportComplete' message
  ** back to the DBWriter.
  **
  ** When the DBWriter receives an 'exportComplete' message the writer will invoke it's end() method which causes the _finish() method to execute and
  ** a 'finish' or 'close' event to be emitted.
  */
 
  constructor(dbi,mode,status,yadamuLogger,options) {

    super({objectMode: true});
    
	/*
	this.theEnd = this.end
	this.end = (chunk,encoding,callback) => {
      console.log(new Error().stack)
	  return this.theEnd(chunk,encoding,callback)
	}
	*/
	
    this.dbi = dbi;
    this.mode = mode;
    this.ddlRequired = (mode !== 'DATA_ONLY');    
    this.status = status;
    this.yadamuLogger = yadamuLogger;
    this.yadamuLogger.info([`Writer`,`${dbi.DATABASE_VENDOR}`,`${this.mode}`,`${this.dbi.workerNumber !== undefined ? this.dbi.workerNumber : 'Primary'}`],`Ready.`)
        
    this.transactionManager = this.dbi
	this.currentTable   = undefined;
    this.rowCount       = undefined;
    this.ddlComplete    = false;

    this.configureFeedback(this.dbi.parameters.FEEDBACK); 
	this.tableCount = 0;

	this.workerStatus = [];
  	this.workerException = undefined;
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
  
  setInputStreamType(ist) {
	this.inputStreamType = ist
  }

  initializeWorkers(workerCount) {
	this.workerStatus = new Array(workerCount).fill('WAITING');
  }
  
  registerWorker(worker) {
	this.setWorkerStatus(worker.dbi.isPrimary() ? 0 : worker.dbi.getWorkerNumber(),'ACTIVE')
  }

  setWorkerException(workerException) {
    // Cache the exception raised by the first worker to fail to that it can be passed to callback in _final	 
    this.workerException = this.workerException === undefined ? workerException : this.workerException;
  }
  
  setWorkerStatus(workerId,workerStatus) {
    this.workerStatus[workerId] = workerStatus
	// this.yadamuLogger.trace([`${this.constructor.name}.setWorkerStatus()`],`${workerId}. Status: ${this.workerStatus}`);
  }
  
  workersComplete() {
    return this.workerStatus.every( v => v === 'COMPLETE')
  }
  
  getTimings() {
    return this.timings
  }
  
  generateMetadata(schemaInfo) {
    const metadata = this.dbi.generateMetadata(schemaInfo,false)
    Object.keys(metadata).forEach((table) => {
       metadata[table].vendor = this.dbi.DATABASE_VENDOR;
    })
    return metadata
  }
  
  async generateStatementCache(metadata,ddlRequired) {
    const startTime = performance.now()
    this.dbi.setMetadata(metadata)      
    await this.dbi.generateStatementCache(this.dbi.parameters.TO_USER,!this.ddlComplete)
	let ddlStatementCount = 0
	let dmlStatementCount = 0
	Object.keys(this.dbi.statementCache).forEach((tableName) => {
	  if (this.dbi.statementCache[tableName].ddl !== null) {
		ddlStatementCount++;
	  }
	  if (this.dbi.statementCache[tableName].dml !== null) {
		dmlStatementCount++;
      }
    })	  
	this.yadamuLogger.ddl([`${this.dbi.DATABASE_VENDOR}`],`Generated ${ddlStatementCount === 0 ? 'no' : ddlStatementCount} DDL and ${dmlStatementCount === 0 ? 'no' : dmlStatementCount} DML statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
  }   

  async getTargetSchemaInfo() {
	  
    // Fetch metadata for tables that already exist in the target schema.
       
    const schemaInfo = await this.dbi.getSchemaInfo('TO_USER');
	return schemaInfo

  }
  
  async setMetadata(sourceMetadata) {
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
      this.dbi.setMetadata(sourceMetadata)      
    }
    else {    
    
       // Copy the source metadata 	   
	   
      Object.keys(sourceMetadata).forEach((table) => {
        const tableMetadata = sourceMetadata[table]
        if (!tableMetadata.hasOwnProperty('vendor')) {
           tableMetadata.vendor = this.dbi.systemInformation.vendor;   
        }
        tableMetadata.source = {
          vendor          : tableMetadata.vendor
         ,columns         : tableMetadata.columns
         ,dataTypes       : tableMetadata.dataTypes
         ,sizeConstraints : tableMetadata.sizeConstraints
        }
      })
    
      if (this.targetSchemaInfo.length > 0) {
        const targetMetadata = this.generateMetadata(this.targetSchemaInfo,false)
    
        // Apply table Mappings 

  	    if (this.dbi.tableMappings !== undefined)  {
          sourceMetadata = this.dbi.applyTableMappings(sourceMetadata)	  
	    }
	 
        // Get source and target Tablenames. Apply name transformations based on TABLE_MATCHING parameter.    

        let targetTableNames = this.targetSchemaInfo.map((tableInfo) => {
          return tableInfo.TABLE_NAME;
        })

        const sourceKeyNames = Object.keys(sourceMetadata)
        let sourceTableNames = sourceKeyNames.map((key) => {
          return sourceMetadata[key].tableName;
        })

        switch ( this.dbi.parameters.TABLE_MATCHING ) {
          case 'UPPERCASE' :
            sourceTableNames = sourceTableNames.map((tableName) => {
              return tableName.toUpperCase();
            })
            break;
          case 'LOWERCASE' :
            sourceTableNames = sourceTableNames.map((tableName) => {
              return tableName.toLowerCase();
            })
            break;
          case 'INSENSITIVE' :
            sourceTableNames = sourceTableNames.map((tableName) => {
              return tableName.toLowerCase();
            })
            targetTableNames = targetTableNames.map((tableName) => {
              return tableName.toLowerCase();
            })
            break;
          default:
        }
           
        // Merge metadata for existing table with metadata from export source

        targetTableNames.forEach((targetName, idx) => {
          const tableIdx = sourceTableNames.findIndex((sourceName) => {return sourceName === targetName})
          if ( tableIdx > -1)    {
            // Copy the source metadata to the source object in the target meteadata. 
            targetMetadata[this.targetSchemaInfo[idx].TABLE_NAME].source = sourceMetadata[sourceKeyNames[tableIdx]].source
			// Overwrite source metadata with target Metadata
            sourceMetadata[sourceKeyNames[tableIdx]] = targetMetadata[this.targetSchemaInfo[idx].TABLE_NAME]
          }
        })
      }    
	  await this.generateStatementCache(sourceMetadata,!this.ddlComplete)
    }
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
  
  recordTimings(timings) {
	Object.assign(this.timings,timings)
  }
 
  async _write(obj, encoding, callback) {
	const messageType = Object.keys(obj)[0]
	try {
	  // this.yadamuLogger.trace([this.constructor.name,`WRITE`,this.dbi.DATABASE_VENDOR],messageType)
      switch (messageType) {
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
		  this.emit('ddlComplete');
          break;
		case 'timings':
		  this.recordTimings(obj.timings);
		  break;
		case 'workerComplete':
		  this.setWorkerStatus(obj.workerComplete === 'Primary' ? 0 : obj.workerComplete,'COMPLETE')
	      // Be careful to ensure that workers allways have appropriate error handling attached.
		  // There is a danger that if worker throws an unexpected exception / rejection the dbWriter will not get a "workerComplete" message, causing YADAMU to hang
	      // this.yadamuLogger.trace([this.constructor.name,`WRITE`,this.dbi.DATABASE_VENDOR,messageType,obj.workerComplete],`${this.workerStatus}`)
          if (this.workersComplete()) {
	        this.write({dataComplete: null})
          }
          break;
  	    case 'eof':		 
		   // 'eof' is generated by the JSON Parser (Text or HTML Parser) when the parsing operation detects the end of the outermost object or array.
		   // It is the equivilant of dataComplete for a Database based reader. The DBWriter will recieve an EOF when the parser is processing 
		   // a DDL_ONLY file which contains no 'data' object.
		case 'dataComplete':
          this.emit('dataComplete',this.workerException);
		  break;
		case 'exportComplete':
		  this.end();
          break;
        default:
      }    
      callback();
    } catch (e) {
	  this.yadamuLogger.handleException([this.constructor.name,`WRITE`,this.dbi.DATABASE_VENDOR,messageType],e);
	  this.transactionManager.skipTable = true;
	  try {
        await this.transactionManager.rollbackTransaction(e)
		if ((['systemInformation','ddl','metadata'].indexOf(messageType) > -1) || this.dbi.abortOnError()){
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
    // this.yadamuLogger.trace([this.constructor.name],'final()')
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
      await this.dbi.releasePrimaryConnection()
      callback(this.workerException);
    } catch (e) {
      this.yadamuLogger.logException([`${this.dbi.DATABASE_VENDOR}`,`"${this.currentTable}"`],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 
	     
}

module.exports = DBWriter;
