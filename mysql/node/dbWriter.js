"use strict";
const Writable = require('stream').Writable

const Yadamu = require('../../common/yadamuCore.js');
const StatementGenerator80 = require('./statementGenerator.js');
const StatementGenerator57 = require('../../common/mysql/statementGenerator57.js');
const MySQLCore = require('./mysqlCore.js');

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


    this.batch = [];
    
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
    
    this.setSQLInterface();
    
  }      
  
  async setSQLInterface() {
    const sqlGetVersion = `SELECT @@version`
    const results = await MySQLCore.query(this.conn,this.status,sqlGetVersion);
    if (results[0]['@@version'] > '6.0') {
       this.statementGenerator = new StatementGenerator80(this.conn,this.status,this.logWriter);
    }
    else {
       this.statementGenerator = new StatementGenerator57(this,this.status,this.logWriter);
    }
  
    // Force 5.7 Code Path
    // this.statementGenerator = new StatementGenerator57(this,this.status this.logWriter);
  } 
  
  async executeDDL(ddlStatements) {
    await Promise.all(ddlStatements.map(async function(ddlStatement) {
                                          try {
                                            return await MySQLCore.query(this.conn,this.status,ddlStatement) 
                                          } catch (e) {
                                            this.logWriter.write(`${e}\n${statementCache[table].ddl}\n`)
                                          }
    },this))
  }

  async setTable(tableName) {
       
    this.tableName = tableName
    this.tableInfo =  this.statementCache[tableName];
    this.rowCount = 0;
    this.batch.length = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
   async writeBatch(status) {
    
     try {
      if (this.tableInfo.useSetClause) {
        for (const i in this.batch) {
          try {
            const results = await MySQLCore.query(this.conn,this.status,this.tableInfo.dml,this.batch[i]);
          } catch(e) {
            if (e.errno && ((e.errno === 3616) || (e.errno === 3617))) {
              this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping Row Reason: ${e.message}\n`)
              this.rowCount--;
            }
            else {
              throw e;
            }
          }    
        }
      }
      else {  
        const results = await MySQLCore.query(this.conn,this.status,this.tableInfo.dml,[this.batch]);
      }
      const endTime = new Date().getTime();
      this.batch.length = 0;
      return endTime
    } catch (e) {
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Reason: ${e.message}\n`)
      this.logWriter.write(`${this.tableInfo.dml}[${this.batch.length}]...\n`);
      this.batch.length = 0;
      this.skipTable = true;
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.dml}\n`);
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
            if (this.batch.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
              await this.conn.commit();
            }  
            if (!this.skipTable) {
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTable(obj.table);
          await this.conn.beginTransaction();
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
                                                   const dataType = Yadamu.decomposeDataType(targetDataType);
                                                   if (obj.data[idx] !== null) {
                                                     switch (dataType.type) {
                                                       case "tinyblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "blob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "mediumblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "longblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "varbinary" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "binary" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "geometry":
                                                         obj.data[idx] = JSON.stringify(obj.data[idx]);
                                                         break;
                                                       case "json" :
                                                         obj.data[idx] = JSON.stringify(obj.data[idx]);
                                                         break;
                                                       default :
                                                     }
                                                   }
                                                 },this)

          this.batch.push(obj.data);
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batch.length} rows.`);
          if (this.batch.length  === this.batchSize) {
              //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             await this.conn.commit();
             const elapsedTime = this.endTime - this.startTime;
             // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
             await this.conn.beginTransaction();
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.tableName) {        
        if (!this.skipTable) {
          if (this.batch.length > 0) {
            // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
           this.endTime = await this.writeBatch();
          }  
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          await this.conn.commit();
        }
      }          
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n`);
      callback(e);
    } 
  } 
}

module.exports = DBWriter;