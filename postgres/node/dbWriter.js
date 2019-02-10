"use strict";
const Writable = require('stream').Writable

const Yadamu = require('../../common/yadamuCore.js');
const StatementGenerator = require('./statementGenerator');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'Postgres';

class DBWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,mode,status,logWriter,options) {
    super({objectMode: true });
    const self = this;
    
    this.conn = conn;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBWriter ready. Mode: ${this.mode}.\n`)

    this.batch = [];
    this.batchRowCount = 0;

    this.systemInformation = undefined;
    this.metadata = undefined;

    this.statementCache = undefined;
    
    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;
    
    this.logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    // this.logDDLIssues   = true;    
    
    this.statementGenerator = new StatementGenerator(conn,status,logWriter);
  }      
  
  objectMode() {
    
    return true;
  
  }
  
  async setTable(tableName) {
       
    this.tableName = tableName
    this.tableInfo =  this.statementCache[tableName];
    this.rowCount = 0;
    this.batch.length = 0;
    this.tableLobIndex = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
   async writeBatch(status) {
    try {
      // Slice removes the unwanted last comma from the replicated args list.
      let argNumber = 1;
      const args = Array(this.batchRowCount).fill(0).map(function() {return `(${Array(this.tableInfo.targetDataTypes.length).fill(0).map(function(){return `$${argNumber++}`}).join(',')})`},this).join(',');
      const statement = this.tableInfo.dml + args
      const results = await this.conn.query(statement,this.batch);
      const endTime = new Date().getTime();
      this.batch.length = 0;
      this.batchRowCount = 0;
      return endTime
    } catch (e) {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`rollback transaction;\n--\n`);
      }
      await this.conn.query(`rollback transaction`);
      this.batch.length = 0;
      this.batchRowCount = 0;
      this.skipTable = true;
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}DBWriter "${this.tableName}"]: Skipping table. Reason: ${e.message}\n`)
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.dml}\n`);
        this.logWriter.write(`${JSON.stringify(this.args)}\n`);
        this.logWriter.write(`${this.batch}\n`);
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
          this.metadata = obj.metadata;
          if (Object.keys(this.metadata).length > 0) {
            this.statementCache = await this.statementGenerator.generateStatementCache(this.schema, this.systemInformation, this.metadata);
          }
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName !== undefined) {
            if (this.batchRowCount > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batchRowCount} rows.`);
              this.endTime = await this.writeBatch(this.status);
              if (this.status.sqlTrace) {
                this.status.sqlTrace.write(`commit transaction;\n--\n`);
              }
              await this.conn.query(`commit transaction`);
            }  
            if (!this.skipTable) {
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"]: Rows witten ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTable(obj.table);
          if (this.status.sqlTrace) {
             this.status.sqlTrace.write(`${this.tableInfo.dml};\n--\n`);
          }
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          this.tableInfo.targetDataTypes.forEach(async function(targetDataType,idx) {
                                                   const dataType = Yadamu.decomposeDataType(targetDataType);
                                                   if (obj.data[idx] !== null) {
                                                     switch (dataType.type) {
                                                       case "bit" :
                                                         if (obj.data[idx] === true) {
                                                           obj.data[idx] = 1
                                                         }
                                                         else {
                                                           obj.data[idx] = 0
                                                         }  
                                                         break;
                                                       case "time" :
                                                         let components = obj.data[idx].split('T')
                                                         obj.data[idx] = components.length === 1 ? components[0] : components[1]
                                                         obj.data[idx] = obj.data[idx].split('Z')[0]
                                                         break;
                                                       case "bytea" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       default :
                                                     }
                                                   }
          },this)
          this.batch.push(...obj.data);
          this.batchRowCount++;
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batchRowCount} rows.`);
          if ((this.batchRowCount  === this.batchSize) || ( this.batch.length > 32768)) {
              //  this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"]: Completed Batch contains ${this.batchRowCount} rows.`);
              this.endTime = await this.writeBatch(this.status);
          }  
          this.rowCount++;
          if (this.rowCount === 1) {
            if (this.status.sqlTrace) {
              this.status.sqlTrace.write(`begin transaction;\n--\n`);
            }
            await this.conn.query(`begin transaction`);
          }
          if ((this.rowCount % this.commitSize) === 0) {
            if (this.status.sqlTrace) {
              this.status.sqlTrace.write(`commit transaction;\n--\n`);
            }
            await this.conn.query(`commit transaction`);
            const elapsedTime = this.endTime - this.startTime;
            if (this.status.sqlTrace) {
              this.status.sqlTrace.write(`begin transaction;\n--\n`);
            }
            await this.conn.query(`begin transaction`);
            // this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"]: Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.tableName) {        
        if (!this.skipTable) {
          if (this.batchRowCount > 0) {
            // this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"]: Final Batch contains ${this.batchRowCount} rows.`);
            this.endTime = await this.writeBatch();
          }  
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          if (this.status.sqlTrace) {
            this.status.sqlTrace.write(`commit transaction;\n--\n`);
          }
          await this.conn.query(`commit transaction`);
        }
      }          
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }
      callback();
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      callback(e);
    } 
  } 
}

module.exports = DBWriter;