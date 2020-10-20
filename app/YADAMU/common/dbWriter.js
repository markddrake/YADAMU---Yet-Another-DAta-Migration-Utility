"use strict" 
const Writable = require('stream').Writable
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
 
  constructor(dbi,yadamuLogger,options) {

    super({objectMode: true});
    this.dbi = dbi;
    this.ddlRequired = (this.dbi.MODE !== 'DATA_ONLY');    
    this.status = dbi.yadamu.STATUS
    this.yadamuLogger = yadamuLogger;
	// console.log(yadamuLogger.os.constructor.name)
    this.yadamuLogger.info([`Writer`,dbi.DATABASE_VENDOR,dbi.DB_VERSION,this.dbi.MODE,this.dbi.getWorkerNumber()],`Ready.`)
        
    this.transactionManager = this.dbi
	this.currentTable   = undefined;
    this.ddlCompleted   = false;

    this.configureFeedback(this.dbi.parameters.FEEDBACK); 

	this.ddlComplete = new Promise((resolve,reject) => {
	  this.once('ddlComplete',(status) => {
 	    // this.yadamuLogger.trace([this.constructor.name],`DDL Complete`)
		if (status instanceof Error) resolve(status)
		resolve(true);
	  })
	})	

	this.dataComplete = new Promise((resolve,reject) => {
	  this.once('dataComplete',() => {
 	    // this.yadamuLogger.trace([this.constructor.name],`DDL Complete`)
		resolve(true);
	  })
	})	
	
  }      
  
  getOutputStream() {
	  
    return [this]

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
  
  generateMetadata(schemaInfo) {
    const metadata = this.dbi.generateMetadata(schemaInfo,false)
    Object.keys(metadata).forEach((table) => {
       metadata[table].vendor = this.dbi.DATABASE_VENDOR;
    })
    return metadata
  }
  
  async generateStatementCache(metadata,ddlRequired) {
    const startTime = performance.now()
    await this.dbi.setMetadata(metadata)     

    await this.dbi.generateStatementCache(this.dbi.parameters.TO_USER,!this.ddlCompleted)
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
      await this.dbi.setMetadata(sourceMetadata)      
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
         ,columnNames     : tableMetadata.columnNames
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

        switch ( this.dbi.TABLE_MATCHING ) {
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
	  await this.generateStatementCache(sourceMetadata,!this.ddlCompleted)
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
            this.ddlCompleted = true;
          }
          break;
        case 'metadata':
          await this.setMetadata(obj.metadata);
		  await this.dbi.initializeData();
		  // The deferred callback is invoked by the DBReader when all data has been processed.
		  // This allows the DBWriter to sleep, while the workers do the heavy lifting.
		  this.deferredCallback = callback
          // this.yadamuLogger.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType],`Emit "ddlComplete"`)  
          this.emit('ddlComplete',null);
		  return;
  	    case 'eof':		 
          this.emit('dataComplete',true);
		  break;
        default:
      }    
      callback();
    } catch (e) {
	  this.yadamuLogger.handleException([`WRITER`,this.dbi.DATABASE_VENDOR,`_write()`,messageType,this.dbi.yadamu.ON_ERROR],e);
      this.emit('ddlComplete',e);
      // Any errors that occur while processing metadata are fatal.
      // Passing the exception to callback triggers the onError() event
      try {
        await this.transactionManager.rollbackTransaction(e)
  	    callback(e);
	  } catch (rollbackError) {
		callback(e); 
      }
    }
  }
 
  async _final(callback) {                                                                   
    // this.yadamuLogger.trace([this.constructor.name],'final()')
    try {
	  if (this.dbi.MODE === "DDL_ONLY") {
        this.yadamuLogger.info([`${this.dbi.DATABASE_VENDOR}`],`DDL only export. No data written.`);
      }
      else {
        await this.dbi.finalizeData();
		if (YadamuLibrary.isEmpty(this.dbi.yadamu.metrics)) {
		  this.yadamuLogger.info([`${this.dbi.DATABASE_VENDOR}`],`No tables found.`);
		}
	  }
      await this.dbi.finalizeImport();
      await this.dbi.releasePrimaryConnection()
      callback();
    } catch (e) {
      this.yadamuLogger.handleException([`WRITER`,this.dbi.DATABASE_VENDOR,`_FINAL()`,this.dbi.yadamu.ON_ERROR],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 
	     
}

module.exports = DBWriter;
