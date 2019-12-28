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
    this.yadamuLogger.log([`${this.constructor.name}`,`${dbi.DATABASE_VENDOR}`],`Ready. Mode: ${this.mode}.`)
        
    this.currentTable = undefined;
    this.rowCount     = undefined;
    this.ddlComplete  = false;
    this.skipTable    = false;

    this.configureFeedback(this.dbi.parameters.FEEDBACK); 
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
  
  setOptions(options) {
    OPTIONS = options
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
    this.dbi.setMetadata(metadata)      
    await this.dbi.generateStatementCache(this.dbi.parameters.TO_USER,!this.ddlComplete)
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
  
  reportTableStatistics(elapsedTime,results) {
      
    const skipCount = this.dbi.skipCount;
    const rowsRead = this.rowCount;
    const rowsWritten = rowsRead - skipCount;
    const throughput = isNaN(elapsedTime) ? 'N/A' : Math.round((rowsWritten/elapsedTime) * 1000)
    
    this.yadamuLogger.log([`${this.constructor.name}`,`${this.tableName}`,`${results.insertMode}`],`Rows written ${rowsWritten}${skipCount !== 0 ? ', skipped ' + skipCount : ''}. DB Time: ${YadamuLibrary.stringifyDuration(Math.round(results.sqlTime))}s. Elaspsed Time ${YadamuLibrary.stringifyDuration(Math.round(elapsedTime))}s. Throughput ${throughput} rows/s.`);
    this.timings[this.tableName] = {rowCount: this.rowCount, insertMode: results.insertMode,  rowsSkipped: skipCount, elapsedTime: Math.round(elapsedTime).toString() + "ms", throughput: Math.round(throughput).toString() + "/s", sqlExecutionTime: Math.round(elapsedTime)};
  }
  
  async initialize() {
    await this.dbi.initializeImport();
	this.targetSchemaInfo = await this.getTargetSchemaInfo()
  }
 
  async _write(obj, encoding, callback) {
    // console.log(new Date().toISOString(),`${this.constructor.name}._write`,Object.keys(obj)[0]);
    try {
      switch (Object.keys(obj)[0]) {
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
          this.skipTable = false;
          this.tableName = obj.table;
          this.currentTable = this.dbi.getTableWriter(this.tableName);
          await this.currentTable.initialize();
          break;
        case 'data': 
          if (this.skipTable === true) {
            break;
          }
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
          }
		  this.currentTable = undefined
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
 
  async _final(callback) {
    try {
	  if (this.timings.length === 0) {
        this.yadamuLogger.info([`${this.constructor.name}`],`DDL only export. No data written.`);
      }
      else {
        await this.dbi.finalizeData();
		if (Object.keys(this.timings).length === 0) {
		  this.yadamuLogger.info([`${this.constructor.name}`],`No tables found.`);
		}
	  }
      await this.dbi.finalizeImport();
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._final()`,`"${this.currentTable}"`],e);
	  await this.dbi.abort();
      process.nextTick(() => this.emit('error',e));
      callback(e);
    } 
  } 
}

module.exports = DBWriter;
