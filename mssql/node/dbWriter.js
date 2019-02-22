"use strict";
const sql = require('mssql');
const Writable = require('stream').Writable

const Yadamu = require('../../common/yadamuCore.js');
const MsSQLCore = require('./mssqlCore.js');
const StatementGenerator = require('./statementGenerator');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'MSSQLSERVER';

let OPTIONS = {
  IDENTIFIER_CASE : null
}

class DBWriter extends Writable {
  
  constructor(pool,database,schema,batchSize,commitSize,mode,status,logWriter,options) {
    super({objectMode: true });
    const self = this;
    
    this.pool = pool;
    // this.transaction = undefined
  
    this.database = database;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[DBWriter ${DATABASE_VENDOR}]: Ready. Mode: ${this.mode}.\n`)

    this.metadata = undefined;
    this.statementCache = undefined;
    
    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;

    this.logDDLIssues = (status.loglevel && (status.loglevel > 2));
    // this.logDDLIssues = true;          

    this.statementGenerator = new StatementGenerator(this.pool, status, logWriter);    
  }      
  
  objectMode() {
    return true;
  }
   
  setOptions(options) {
    OPTIONS = options
  }

  async setTable(tableName) {
      
    switch (OPTIONS.IDENTIFIER_CASE) {
       case 'UPPER':
         this.tableName = tableName.toUpperCase();
         break;
       case 'LOWER':
         this.tableName = tableName.toLowerCase();
         break;         
      default: 
        this.tableName = tableName;
    }                
       
    this.tableInfo = this.statementCache[this.tableName]
    this.tableInfo.bulkOperation.rows.length = 0;
    this.rowCount = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
  async writeBatch(status) {
      
    if (this.tableInfo.bulkSupported) {
      try {
        
        const results = await this.pool.request().bulk(this.tableInfo.bulkOperation);
        const endTime = new Date().getTime();
        // console.log(`Bulk(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Success`);
        this.tableInfo.bulkOperation.rows.length = 0;
        return endTime
      } catch (e) {
        if (this.logDDLIssues) {
          this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"]: Bulk Operation failed. Reason: ${e.message}\n`)
        }
        this.tableInfo.bulkSupported = false;
        // console.log(this.tableInfo.bulkOperation.columns);
        if (this.logDDLIssues) {
          this.logWriter.write(`${e.stack}\n`);
          this.logWriter.write(`{${JSON.stringify(this.tableInfo.bulkOperation.columns)}`);
        }      
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.

    if (this.tableInfo.preparedStatement === undefined) {
      this.tableInfo.preparedStatement = await this.statementGenerator.createPreparedStatement(this.pool, this.tableInfo.dml, this.tableInfo.targetDataTypes) 
    }

    try {
      for (const r in this.tableInfo.bulkOperation.rows) {
        const args = {}
        for (const c in this.tableInfo.bulkOperation.rows[0]){
          args['C'+c] = this.tableInfo.bulkOperation.rows[r][c]
        }
        const results = await this.tableInfo.preparedStatement.execute(args);
      }
            
      const endTime = new Date().getTime();
      // console.log(`Conventional(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Success`);
      this.tableInfo.bulkOperation.rows.length = 0;
      return endTime
    } catch (e) {
      // console.log(`Conventional(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Failed`);
      this.tableInfo.bulkOperation.rows.length = 0;
      this.skipTable = true;
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"]: Skipping table. Reason: ${e.message}\n${e.stack}\n`)
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.bulkOperation.columns}\n`);
        this.logWriter.write(`${this.tableInfo.bulkOperation.rows}\n`);
      }      
    }
  }

  async _write(obj, encoding, callback) {
    
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'metadata':
          this.metadata = Yadamu.convertIdentifierCase(OPTIONS.IDENTIFIER_CASE,obj.metadata);
          const targetTableInfo = await MsSQLCore.getTableInfo(this.pool.request(),this.schema,this.status);
          if (targetTableInfo.length > 0) {
             this.metadata = Yadamu.mergeMetadata(MsSQLCore.generateMetadata(targetTableInfo,false),this.metadata);
          }
          if (Object.keys(this.metadata).length > 0) {
            this.statementCache = await this.statementGenerator.generateStatementCache(this.database, this.schema, this.systemInformation, this.metadata, );
          }
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName) {
            if (this.tableInfo.bulkOperation.rows.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
            }  
            if (!this.skipTable) {
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${this.tableInfo.bulkSupported ? 'Bulk' : 'Conventional'}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
            if (!this.tableInfo.bulkSupported) {
              await this.tableInfo.preparedStatement.unprepare();
            }
          }
          this.setTable(obj.table);
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          if (this.rowCount === 0) {
            // await this.transaction.begin();
          }
          // Perform SQL Server specific data type conversions before pushing row to bulkOperation row cache
          this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
                                                   const dataType = Yadamu.decomposeDataType(targetDataType);
                                                   if (obj.data[idx] !== null) {
                                                     switch (dataType.type) {
                                                       case "image" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "varbinary":
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "json":
                                                         if (typeof obj.data[idx] === 'object') {
                                                           obj.data[idx] = JSON.stringify(obj.data[idx]);
                                                         }
                                                         break;
                                                       case "time":
                                                       case "date":
                                                       case "datetime":
                                                       case "datetime2":
                                                       case "datetimeoffset":
                                                         if (typeof obj.data[idx] === 'string') {
                                                           obj.data[idx] = obj.data[idx].endsWith('Z') ? obj.data[idx] : `${obj.data[idx]}Z`
                                                         }
                                                         else {
                                                           // Alternative is to rebuild the table with these data types mapped to date objects ....
                                                           obj.data[idx] = obj.data[idx].toISOString();
                                                         }
                                                         break;
                                                       default :
                                                     }
                                                   }
          },this)
          this.tableInfo.bulkOperation.rows.add(...obj.data);
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
          if (this.tableInfo.bulkOperation.rows.length  === this.batchSize) {
              //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             // await this.transaction.commit();
             // await this.transaction.begin();       
             const elapsedTime = this.endTime - this.startTime;
             // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
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
      if (this.tableName) {        
        if (!this.skipTable) {
          if (this.tableInfo.bulkOperation.rows.length > 0) {
            // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
            this.endTime = await this.writeBatch();
          }  
          // await this.transaction.commit();
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${this.tableInfo.bulkSupported ? 'Bulk' : 'Conventional'}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          // this.transaction.commit();
        }
        if (!this.tableInfo.bulkSupported) {
          await this.tableInfo.preparedStatement.unprepare();
        }
      }          
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }
      // this.conn.close();
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._final() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    } 
  } 
}

module.exports = DBWriter;