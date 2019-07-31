"use strict";
const Writable = require('stream').Writable
const Readable = require('stream').Readable;

const Yadamu = require('./yadamu.js');

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
    
    this.timings = {}
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
    await this.dbi.generateStatementCache(this.dbi.parameters.TOUSER,!this.ddlComplete)
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


    
    // Fetch metadata for tables that already exist in the target schema.
       
    const targetSchemaInfo = await this.dbi.getSchemaInfo('TOUSER');
    
    if (targetSchemaInfo === null) {
      this.dbi.setMetadata(metadata)      
    }
    else {    
    
       // Copy the source metadata 
    
      Object.keys(metadata).forEach(function(table) {
        if (!metadata[table].hasOwnProperty('vendor')) {
           metadata[table].vendor = this.dbi.systemInformation.vendor;   
        }
        metadata[table].source = {
          vendor          : metadata[table].vendor
         ,dataTypes       : metadata[table].dataTypes
         ,sizeConstraints : metadata[table].sizeConstraints
        }
      },this)
    
      if (targetSchemaInfo.length > 0) {
    
        // Merge metadata for existing table with metadata from export source

        const exportTableNames = Object.keys(metadata)
        const targetMetadata = this.generateMetadata(targetSchemaInfo,false)
      
        // Transform tablenames based on TABLE_MATCHING parameter.    

        let targetNamesTransformed = targetSchemaInfo.map(function(tableInfo) {
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
            targetMetadata[targetSchemaInfo[idx].TABLE_NAME].source = metadata[exportTableNames[tableIdx]].source
            metadata[exportTableNames[tableIdx]] = targetMetadata[targetSchemaInfo[idx].TABLE_NAME]
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
    const throughput = (rowsWritten/Math.round(elapsedTime)) * 1000
    this.yadamuLogger.log([`${this.constructor.name}`,`${this.tableName}`,`${results.insertMode}`],`Rows written ${rowsWritten}${skipCount !== 0 ? ', skipped ' + skipCount : ''}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round(throughput)} rows/s.`);
    this.timings[this.tableName] = {rowCount: this.rowCount, insertMode: results.insertMode,  rowsSkipped: skipCount, elapsedTime: Math.round(elapsedTime).toString() + "ms", throughput: Math.round(throughput).toString() + "/s"};
  }
 
  async _write(obj, encoding, callback) {
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
          break;
        case 'table':
          if (this.currentTable === undefined) {
            await this.dbi.initializeDataLoad();
          }
          else {
            const results = await this.currentTable.finalize();
            this.skipTable = results.skipTable;
            if (this.skipTable === false) {
              const elapsedTime = results.endTime - results.startTime;
              this.reportTableStatistics(elapsedTime,results);
            }
          }
          // this.setTableName(obj.table)
          this.tableName = obj.table;
          this.currentTable = this.dbi.getTableWriter(this.tableName);
          // await this.dbi.beginTransaction();
          await this.currentTable.initialize();
          this.rowCount = 0;
          this.skipTable = false;
          break;
        case 'data': 
          if (this.skipTable === true) {
            break;
          }
          await this.currentTable.appendRow(obj.data);
          this.rowCount++;
          if (this.currentTable.batchComplete()) {
            this.skipTable = await this.currentTable.writeBatch(this.status);
          }  
          if (this.currentTable.commitWork(this.rowCount)) {
             await this.dbi.commitTransaction()
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._write()`,`"${this.tableName}"`],e);
      process.nextTick(() => this.emit('error',e));
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.currentTable !== undefined) {
        const results = await this.currentTable.finalize();
        this.skipTable = results.skipTable;
        if (!this.skipTable) {
          const elapsedTime = results.endTime - results.startTime;            
          this.reportTableStatistics(elapsedTime,results);
        }
        await this.dbi.finalizeDataLoad();
      }
      else {
        this.yadamuLogger.info([`${this.constructor.name}`],`No tables found.`);
      }
      await this.dbi.importComplete();
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._final()`,`"${this.currentTable}"`],e);
      process.nextTick(() => this.emit('error',e));
      callback(e);
    } 
  } 
}

module.exports = DBWriter;