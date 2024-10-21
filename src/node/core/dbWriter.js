
import {Writable}                 from 'stream'
import { performance }            from 'perf_hooks';

import YadamuLibrary              from '../lib/yadamuLibrary.js'
import YadamuConstants            from '../lib/yadamuConstants.js'

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
    this.yadamuLogger.info([YadamuConstants.WRITER_ROLE,dbi.DATABASE_VENDOR,dbi.DATABASE_VERSION,this.dbi.MODE,this.dbi.getWorkerNumber()],`Ready.`)
        
    this.transactionManager = this.dbi
    this.currentTable   = undefined;
    this.ddlCompleted   = false;
    this.deferredCallback = YadamuLibrary.NOOP
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
    if (!this.ddlCompleted) {
      // Execute DDL Statements Asynchronously - Emit dllComplete when ddl execution is finished. 
      this.executeDDL(ddlStatements).then(YadamuLibrary.NOOP).catch((e) => {console.log(e)})
    }
  }   

  compareMetadata(source,target) {
	  
	if (source.skipColumnReordering) {
	  return {}
	}
	  
    let columnMappings = source.columnNames.reduce((columnMappings,columnName,idx) => {
      if (target.columnNames[idx] !== source.columnNames[idx]) {
        const targetIdx = target.columnNames.indexOf(columnName) 
          columnMappings[columnName] = {
            source : idx
          , target : targetIdx 
          } 
      }
      return columnMappings
    },{})
    columnMappings = target.columnNames.reduce((columnMappings,columnName,idx) => {
      const sourceIdx = source.columnNames.indexOf(columnName) 
      if (sourceIdx < 0) {
        columnMappings[columnName] = {
          source : sourceIdx
        , target : idx 
        } 
      }
      return columnMappings
    },columnMappings)
    return columnMappings
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

    // Apply table Mappings 

    if (this.dbi.tableMappings !== undefined)  {
      sourceMetadata = this.dbi.applyTableMappings(sourceMetadata,this.dbi.tableMappings)     
    }
        	
    const targetSchemaInfo = await this.dbi.getSchemaMetadata()
    
    if (targetSchemaInfo.length > 0) {

      // Generate the target metadata
      const targetMetadata = this.generateMetadata(targetSchemaInfo,false)
	  // Apply table Mappings 
      if (this.dbi.tableMappings !== undefined)  {
        targetMetadata = this.dbi.applyTableMappings(targetMetadata,this.dbi.tableMappings)     
      }
        
      // Get source and target Tablenames. Apply name transformations based on the DBI IDENTIFIER_TRANSFORMATION parameter.    

      const sourceMetadataArray = Object.values(sourceMetadata)
      let sourceTableNames = sourceMetadataArray.map((tableMetadata) => {
         return tableMetadata.tableName
      })

      const targetMetadataArray = Object.values(targetMetadata)
      let targetTableNames = targetMetadataArray.map((tableMetadata) => {
         return tableMetadata.tableName
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
	    
	  sourceTableNames.forEach((sourceTableName,sourceIdx) => {
	    if (targetTableNames.includes(sourceTableName)) {
          const tableMetadata = {...sourceMetadataArray[sourceIdx]}
          tableMetadata.columnMappings = this.compareMetadata(sourceMetadataArray[sourceIdx],targetMetadataArray[targetTableNames.indexOf(sourceTableName)])
          Object.assign(sourceMetadataArray[sourceIdx],targetMetadataArray[targetTableNames.indexOf(sourceTableName)])
          sourceMetadataArray[sourceIdx].source = tableMetadata
		  sourceMetadataArray.vendor = this.DATABASE_VENDOR
		}
      })
    }
    await this.generateStatementCache(sourceMetadata)
  }      
  
  async initialize() {
    await this.dbi.initializeImport();
  }
  
  async doConstruct() {
  }
   
  _construct(callback) {
    this.doConstruct().then(() => { 
      callback() 
    }).catch((e) => { 
      this.yadamuLogger.handleException([`WRITER`,`INITIALIZE`,this.dbi.DATABASE_VENDOR,this.dbi.ON_ERROR],e);
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
          if (!this.ddlRequired) {
            this.dbi.skipDDLOperations()
          }
          else {
            if ((obj.ddl.length > 0) && (this.dbi.isValidDDL())) { 
              const startTime = performance.now()
              const results = await this.dbi.executeDDL(obj.ddl);
              if (results instanceof Error) {
                throw results;
              }
              this.ddlCompleted = true
            }
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
             console.log(obj.table,this.dataSource.constructor.name,this.dataSource.PIPELINE_STATE)
             const outputStreams = await this.dbi.getOutputStreams(obj.table,this.dataSource.PIPELINE_STATE)
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
      this.yadamuLogger.handleException([`WRITER`,`WRITE`,messageType,this.dbi.DATABASE_VENDOR,this.dbi.ON_ERROR],err)
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
    await this.dbi.final()
  } 
  
  _final(callback) {
     
    this.doFinal().then(() => { 
      callback() 
    }).catch((e) => { 
      this.yadamuLogger.handleException([`WRITER`,`FINAL`,this.dbi.DATABASE_VENDOR,this.dbi.ON_ERROR],e);
      callback(e)
    })
  }
  
  async doDestroy(err) {
    // this.yadamuLogger.trace([this.constructor.name,`DESTORY`,this.dbi.DATABASE_VENDOR],``)
    // Forced clean-up of the DBI
    await this.dbi.destroy(err)
  }
  
  _destroy(err,callback) {
    this.doDestroy(err).then((err) => {
      callback(err) 
    }).catch((e) => { 
      this.yadamuLogger.handleException([`WRITER`,`DESTROY`,this.dbi.DATABASE_VENDOR,this.dbi.ON_ERROR],e);
      callback(e)
    })
  }    
}

export { DBWriter as default}
