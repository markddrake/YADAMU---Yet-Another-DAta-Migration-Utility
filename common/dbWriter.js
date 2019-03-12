"use strict";
const Writable = require('stream').Writable
const Readable = require('stream').Readable;

const Yadamu = require('./yadamu.js');

class DBWriter extends Writable {
  
  constructor(dbi,schema,mode,status,logWriter,options) {

    super({objectMode: true });
    const self = this;
    
    this.dbi = dbi;
    this.schema = schema;
    this.mode = mode;
    this.ddlRequired = (mode !== 'DATA_ONLY');    
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[DBWriter ${dbi.DATABASE_VENDOR}]: Ready. Mode: ${this.mode}.\n`)
        
    this.currentTable = undefined;
    this.rowCount     = undefined;
    this.ddlComplete  = false;
  }      
  
  objectMode() {
    return this.dbi.objectMode(); 
  }
  
  setOptions(options) {
    OPTIONS = options
  }
  
  generateMetadata(schemaInfo) {
    const metadata = this.dbi.generateMetadata(schemaInfo,false)
    Object.keys(metadata).forEach(function(table) {
       metadata[table].vendor = this.dbi.DATABASE_VENDOR;
    },this)
    return metadata
  }
  
  async generateStatementCache(metadata,ddlRequired) {
    if (Object.keys(metadata).length > 0) {   
      // ### if the import already processed a DDL object do not execute DDL when generating statements.
      Object.keys(metadata).forEach(function(table) {
        if (!metadata[table].hasOwnProperty('vendor')) {
           metadata[table].vendor = this.dbi.systemInformation.vendor;
        }
      },this)
      this.dbi.setMetadata(metadata)      
      await this.dbi.generateStatementCache(this.schema,!this.ddlComplete)
    }
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
    
    const targetSchemaInfo = await this.dbi.getSchemaInfo(this.schema);
    
    if (targetSchemaInfo === null) {
      this.dbi.setMetadata(metadata)      
    }
    else {    
   
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
            metadata[exportTableNames[tableIdx]] = targetMetadata[targetSchemaInfo[idx].TABLE_NAME]
          }
        },this)
      }    

      await this.generateStatementCache(metadata,!this.ddlComplete)
    }
  }      
 
  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.dbi.setSystemInformation(obj.systemInformation)
          break;
        case 'ddl':
          if ((this.ddlRequired) && (obj.ddl.length > 0) && (this.dbi.isValidDDL())) { 
            await this.dbi.executeDDL(this.schema,obj.ddl);
            this.ddlComplete = true;
          }
          break;
        case 'metadata':
          await this.setMetadata(obj.metadata);
          break;
        case 'table':
          if (this.currentTable === undefined) {
            await this.dbi.initializeDataLoad(this.schema);
          
          }
          else {
            const results = await this.currentTable.finalize();
            if (!this.skipTable) {
              const elapsedTime = results.endTime - results.startTime;            
              this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${results.insertMode}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          // this.setTableName(obj.table)
          this.tableName = obj.table;
          this.currentTable = this.dbi.getTableWriter(this.schema,this.tableName);
          await this.currentTable.initialize();
          this.rowCount = 0;
          break;
        case 'data': 
          if (this.skipTable) {
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
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._write() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.currentTable !== undefined) {
        const results = await this.currentTable.finalize();
        if (!this.skipTable) {
          const elapsedTime = results.endTime - results.startTime;            
          this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${results.insertMode}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
        }
        await this.dbi.finalizeDataLoad();
      }
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      } 
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._final() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    } 
  } 
}

module.exports = DBWriter;