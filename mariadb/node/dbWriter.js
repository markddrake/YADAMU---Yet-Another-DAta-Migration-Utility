"use strict";
const Writable = require('stream').Writable

const Yadamu = require('../../common/yadamuCore.js');
const StatementGenerator = require('../../common/mysql/statementGenerator57.js');

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
    this.batchRowCount = 0;

    this.systemInformation = undefined;
    this.metadata = undefined;

    this.statementCache = undefined;

    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined;
    this.startTime = undefined;
    this.skipTable = true;

    this.statementGenerator = new StatementGenerator(this, status, logWriter) 
    
    this.logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    // this.logDDLIssues   = true;
  }

  async executeDDL(ddlStatements) {
    await Promise.all(ddlStatements.map(async function(ddlStatement) {
                                          try {
                                            if (this.status.sqlTrace) {
                                              this.status.sqlTrace.write(`${statementCache[table].ddl};\n--\n`);
                                            }
                                            return await this.conn.query(ddlStatement) 
                                          } catch (e) {
                                            this.logWriter.write(`${e}\n${statementCache[table].ddl}\n`)
                                          }
    },this))
  }
  
  async setTable(tableName) {

    this.tableName = tableName
    this.tableInfo =  this.statementCache[tableName];
    this.tableInfo.args =  '(' + Array(this.tableInfo.targetDataTypes.length).fill('?').join(',')  + '),';
    this.rowCount = 0;
    this.batch.length = 0;
    this.batchRowCount = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }

   async writeBatch(status) {

    try {
      if (this.tableInfo.useSetClause) {
        for (const i in this.batch) {
          try {
            const results = await this.conn.query(this.tableInfo.dml,this.batch[i]);
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
        // Slice removes the unwanted last comma from the replicated args list.
        const args = this.tableInfo.args.repeat(this.batchRowCount).slice(0,-1);
        const results = await this.conn.query(this.tableInfo.dml.slice(0,-1) + args, this.batch);
      }
      const endTime = new Date().getTime();
      this.batch.length = 0;
      this.batchRowCount = 0;
      return endTime
    } catch (e) {
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Reason: ${e.message}\n`)
      this.logWriter.write(`${this.tableInfo.dml}[${this.batchRowCount}]...\n`);
      this.batch.length = 0;
      this.batchRowCount = 0;
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
            if (this.batchRowCount > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batchRowCount} rows.`);
              this.endTime = await this.writeBatch(this.status);
              await this.conn.commit();
            }
            if (!this.skipTable) {
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTable(obj.table);
          await this.conn.commit();
          await this.conn.beginTransaction();
          if (this.status.sqlTrace) {
             this.status.sqlTrace.write(`${this.tableInfo.dml};\n--\n`);
          }
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
                                                       case "timestamp" :
                                                         obj.data[idx] = new Date(Date.parse(obj.data[idx]));
                                                         break;
                                                       default :
                                                     }
                                                   }
                                                 },this)
          this.batch.push(...obj.data);
          this.batchRowCount++;
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batchRowCount} rows.`);
          if (this.batchRowCount  === this.batchSize) {
              //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batchRowCount} rows.`);
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
          if (this.batchRowCount > 0) {
            // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batchRowCount} rows.`);
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