"use strict" 
const Writable = require('stream').Writable
const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js');

class DBWriter extends Writable {
  
  /*
  **
  ** The DB Writer should always be invoked with the option {end: false}. 
  ** 
  */
      
  constructor(dbi,yadamuLogger,options) {

    super({objectMode: true});
    this.dbi = dbi;
    this.ddlRequired = (this.dbi.MODE !== 'DATA_ONLY');    
    this.status = dbi.yadamu.STATUS
    this.yadamuLogger = yadamuLogger;
    this.yadamuLogger.info([`Writer`,dbi.DATABASE_VENDOR,dbi.DB_VERSION,this.dbi.MODE,this.dbi.getWorkerNumber()],`Ready.`)
        
    this.transactionManager = this.dbi
	this.currentTable   = undefined;
    this.ddlCompleted   = false;
    this.deferredCallback = () => {}

	this.ddlComplete = new Promise((resolve,reject) => {
	  this.once('ddlComplete',(status,startTime) => {
	    try {
 	      // this.yadamuLogger.trace([this.constructor.name],`${this.constructor.name}.on(ddlComplete): (${status instanceof Error}) "${status ? `${status.constructor.name}(${status.message})` : status}"`)
		  if (status instanceof Error) {
	        this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR],`DDL Failure. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
		    status.ignoreUnhandledRejection = true;
            reject (status)
          }
		  else {
  	        this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR],`Executed ${Array.isArray(status) ? status.length : undefined} DDL operations. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
		    resolve(true);
	      }
		} catch (e) {
		  reject (e)
		}
	  })
	})		
	
  }      
                                                                                             
  callDeferredCallback() {
	this.deferredCallback();
	this.deferredCallback = undefined
  }
  
  getOutputStream() {
	  
    return [this]

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
  
  async executeDDL(ddlStatements) {
	// this.yadamuLogger.trace([this.constructor.name,`executeDDL()`,this.dbi.DATABASE_VENDOR],`Executing DLL statements)`) 
    const startTime = performance.now()
	try {
      const results = await this.dbi.executeDDL(ddlStatements) 
  	  this.emit('ddlComplete',results,startTime);	 
	} catch (e) {
  	  this.emit('ddlComplete',e,startTime);	 
    }	
  }
  
  async generateStatementCache(metadata) {
    const startTime = performance.now()
    await this.dbi.setMetadata(metadata)     
    const statementCache = await this.dbi.generateStatementCache(this.dbi.parameters.TO_USER)
	const ddlStatements = this.dbi.analyzeStatementCache(statementCache,startTime)

	if (this.ddlCompleted) {
      // this.yadamuLogger.trace([this.constructor.name,`generateStatementCache()`,this.dbi.DATABASE_VENDOR,],`DDL already completed. Emit ddlComplete(SUCCESS))`)  
	  this.emit('ddlComplete',[],performance.now());
	}
	else {
  	  // Execute DDL Statements Asynchronously - Emit dllComplete when ddl execution is finished. 
      this.executeDDL(ddlStatements)
    }
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
		 ,storageTypes    : tableMetadata.storageTypes
         ,sizeConstraints : tableMetadata.sizeConstraints
        }
      })
    
      if (this.targetSchemaInfo.length > 0) {
        const targetMetadata = this.generateMetadata(this.targetSchemaInfo,false)
    
        // Apply table Mappings 

  	    if (this.dbi.tableMappings !== undefined)  {
          sourceMetadata = this.dbi.applyTableMappings(sourceMetadata,this.dbi.tableMappings)	  
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
	  await this.generateStatementCache(sourceMetadata)
    }
  }      
    
  async initialize() {
    await this.dbi.initializeImport();
	this.targetSchemaInfo = await this.getTargetSchemaInfo()
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
		    const startTime = performance.now()
            const results = await this.dbi.executeDDL(obj.ddl);
			if (results instanceof Error) {
			  throw results;
			}
			this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR],`Executed ${results.length} DDL statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
			this.ddlCompleted = true
          }
          break;
        case 'metadata':
		  // Cache the callback. The deferred callback is invoked by the DBReader when all DDL and DML operations are complete.
		  // This allows the DBWriter to sleep, while the workers do the heavy lifting.
		  this.deferredCallback = callback
          this.setMetadata(obj.metadata);
		  await this.dbi.initializeData();
		  return
  	    case 'eof':		 
          // this.emit('dataComplete',true);
		  break;
        default:
      }    
      callback();
    } catch (e) {
	  this.yadamuLogger.handleException([`WRITER`,`_WRITE`,messageType,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],e);
      // Any errors that occur while processing metadata are fatal.Passing the exception to callback triggers the onError() event
	  // Attempt a rollback, however if the rollback fails invoke the callback with the origianal exception.
      try {
        await this.transactionManager.rollbackTransaction(e)
  	    callback(e);
	  } catch (rollbackError) {
		callback(e); 
      }
	  switch (messageType) {
        case 'ddl':
          this.emit('ddlComplete',e)
		  break;
        default:
      }		
      this.underlyingError = e;
    }
  }
 
  async _final(callback) {                                                                   
    // this.yadamuLogger.trace([this.constructor.name],'final()')
    try {
	  if (this.dbi.MODE === "DDL_ONLY") {
        this.yadamuLogger.info([`${this.dbi.DATABASE_VENDOR}`],`DDL only operation. No data written.`);
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
      this.yadamuLogger.handleException([`WRITER`,`FINAL`,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 
	     
}

module.exports = DBWriter;
