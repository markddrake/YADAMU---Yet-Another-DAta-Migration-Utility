"use strict" 

import {Writable} from 'stream'
import { performance } from 'perf_hooks';

import YadamuLibrary from './yadamuLibrary.js';
import YadamuConstants from './yadamuConstants.js';

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
  }  
  
  getOutputStream() {
	  
    return [this]

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
  	  // this.emit(YadamuConstants.DDL_COMPLETE,results,startTime);	 
	} catch (e) {
  	  // this.emit(YadamuConstants.DDL_COMPLETE,e,startTime);	 
    }	
  }
  
  async generateStatementCache(metadata) {

    const startTime = performance.now()
    await this.dbi.setMetadata(metadata)     
    const statementCache = await this.dbi.generateStatementCache(this.dbi.parameters.TO_USER)
	const ddlStatements = this.dbi.analyzeStatementCache(statementCache,startTime)
	
	if (this.ddlCompleted) {
      // this.yadamuLogger.trace([this.constructor.name,`generateStatementCache()`,this.dbi.DATABASE_VENDOR,],`DDL already completed. Emit ddlComplete(SUCCESS))`)  
	}
	else {
  	  // Execute DDL Statements Asynchronously - Emit dllComplete when ddl execution is finished. 
      this.executeDDL(ddlStatements)
    }
  }   

  async getTargeMetadata() {
	  
    // Fetch metadata for tables that already exist in the target schema.
       
    const targetMetadata = await this.dbi.getSchemaMetadata();
	return targetMetadata

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

    if (this.targetMetadata === null) {
      await this.dbi.setMetadata(sourceMetadata)      
    }
    else {    
    
       // Copy the source metadata 	   
	   
      Object.keys(sourceMetadata).forEach((table) => {
        const tableMetadata = sourceMetadata[table]
		tableMetadata.source = Object.assign({},sourceMetadata[table])
        if (!tableMetadata.hasOwnProperty('vendor')) {
           tableMetadata.vendor = this.dbi.systemInformation.vendor;   
        }
	    /*
          vendor          : tableMetadata.vendor
         ,columnNames     : tableMetadata.columnNames
         ,dataTypes       : tableMetadata.dataTypes
		 ,storageTypes    : tableMetadata.storageTypes
         ,sizeConstraints : tableMetadata.sizeConstraints
		 */
      })
    
      if (this.targetMetadata.length > 0) {
        const targetMetadata = this.generateMetadata(this.targetMetadata,false)
    
        // Apply table Mappings 

  	    if (this.dbi.tableMappings !== undefined)  {
          sourceMetadata = this.dbi.applyTableMappings(sourceMetadata,this.dbi.tableMappings)	  
	    }
	 
        // Get source and target Tablenames. Apply name transformations based on the DBI IDENTIFIER_TRANSFORMATION parameter.    

        let targetTableNames = this.targetMetadata.map((tableInfo) => {
          return tableInfo.TABLE_NAME;
        })

        const sourceKeyNames = Object.keys(sourceMetadata)
        let sourceTableNames = sourceKeyNames.map((key) => {
          return sourceMetadata[key].tableName;
        })
        
		switch (this.dbi.IDENTIFIER_TRANSFORMATION ) {
          case 'LOWERCASE':
            sourceTableNames = sourceTableNames.map((tableName) => {
              return tableName.toLowerCase();
            })
            targetTableNames = targetTableNames.map((tableName) => {
               return tableName.toLowerCase();
            })
			break;
          case 'UPPERRCASE':
            sourceTableNames = sourceTableNames.map((tableName) => {
              return tableName.toUpperCase();
            })
            targetTableNames = targetTableNames.map((tableName) => {
               return tableName.toUpperCase();
            })
			break;
		}	
           
        // Merge metadata for existing table with metadata from export source

        targetTableNames.forEach((targetName, idx) => {
          const tableIdx = sourceTableNames.findIndex((sourceName) => {return sourceName === targetName})
          if ( tableIdx > -1)    {
            // Copy the source metadata to the source object in the target meteadata. 
            targetMetadata[this.targetMetadata[idx].TABLE_NAME].source = sourceMetadata[sourceKeyNames[tableIdx]].source
			// Overwrite source metadata with target Metadata
            sourceMetadata[sourceKeyNames[tableIdx]] = targetMetadata[this.targetMetadata[idx].TABLE_NAME]
          }
        })
      }    
	  await this.generateStatementCache(sourceMetadata)
    }
  }      
  
  async initialize() {
    await this.dbi.initializeImport();
	this.targetMetadata = await this.getTargeMetadata()
  }
  
  async doConstruct() {
  }
   
  _construct(callback) {
	this.doConstruct().then(() => { 
	  callback() 
    }).catch((e) => { 
      this.yadamuLogger.handleException([`WRITER`,`INITIALIZE`,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],e);
	  callback(e)
    })
  }
  
 
  async doWrite(obj) {
	   
    const messageType = Object.keys(obj)[0]
    // this.yadamuLogger.trace([this.constructor.name,`WRITE`,this.dbi.DATABASE_VENDOR],messageType)
	try {
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
		    this.ddlCompleted = true
          }
		  else {
			this.dbi.skipDDLOperations()
		  }
          break;
        case 'metadata':
          await this.setMetadata(obj.metadata);
		  await this.dbi.initializeData();
		  return true
  	    case 'eof':		 
		  break;
		case 'table':
          /*
		  **
	      **
		  ** The following code will not work as by the time the table message has been received and the pipes have been switched around 'data' message are already flowing to the dbWriter.
		  ** Stream re-direction must take place before any messages for the new target enter the pipeline. Hence the need for the eventStream class, which ensures that correct target is 
		  ** attached before pushing data.
		  **
		  **
          
          if (this.dbi.metadata.hasOwnProperty(obj.table)) {
             // Table is in the list of tables to be processed.
    		 this.dataSource.unpipe(this)
			 console.log(obj.table,this.dataSource.constructor.name,this.dataSource.COPY_METRICS)
			 const outputStreams = await this.dbi.getOutputStreams(obj.table,this.dataSource.COPY_METRICS)
			 const outputManager = outputStreams[0]
			 console.log(outputManager.constructor.name)
			 outputManager.once('eod',() => {
			   outputManager.unpipe(this.dataSource)
			   outputManager.pipe(this)
			 })
			 this.dataSource.pipe(outputManager)
		   }
		   
		   **
		   */
		   break;		   
        default:
      }    
	  return false
	} catch (err) {
      this.yadamuLogger.handleException([`WRITER`,`WRITE`,messageType,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],err)
      this.underlyingError = err;
      // Attempt a rollback, however if the rollback fails throw the err that let to the rollback operation
	  try {
        await this.transactionManager.rollbackTransaction(e)
	  } catch (rollbackError) {}	  
	  throw err
	}
	
  }
 
  _write(obj, encoding, callback) {
	  
	 // Deferred Callback is true when the reader will be redirected to another stream, such as a table writer. The callback will be invoked when the intermediate copies have completed.
	  
	 this.doWrite(obj).then(() => { callback() }).catch((e) => { callback(e) })
       
  }
  
  async doFinal() {                                                                   

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
	await this.dbi.doFinal()
  } 
  
  _final(callback) {
	 
	this.doFinal().then(() => { 
	  callback() 
    }).catch((e) => { 
      this.yadamuLogger.handleException([`WRITER`,`FINAL`,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],e);
	  callback(e)
    })
  }
  
  async doDestroy(err) {
    // this.yadamuLogger.trace([this.constructor.name,`DESTORY`,this.dbi.DATABASE_VENDOR],``)
	await this.dbi.doDestroy()
  }
  
  _destroy(err,callback) {
	this.doDestroy(err).then((err) => {
	  callback(err) 
	}).catch((e) => { 
 	  this.yadamuLogger.handleException([`WRITER`,`DESTROY`,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],e);
 	  callback(e)
 	})
  }    
}

export { DBWriter as default}
