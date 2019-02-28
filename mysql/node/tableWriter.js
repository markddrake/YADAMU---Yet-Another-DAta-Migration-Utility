"use strict"

const Yadamu = require('../../common/yadamu.js');

class TableWriter {

  constructor(dbi,schema,tableName,tableInfo,status,logWriter) {
    this.dbi = dbi;
    this.schema = schema;
    this.tableName = tableName
    this.tableInfo = tableInfo;
    this.status = status;
    this.logWriter = logWriter;    

    this.batch = [];
    
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.insertMode = 'Batch';

    this.skipTable = false;

    this.logDDLIssues   = (this.status.loglevel && (this.status.loglevel > 2));
    this.logDDLIssues   = true;
  }

  async initialize() {
  }

  batchComplete() {
    return (this.batch.length === this.tableInfo.batchSize)
  }
  
  commitWork(rowCount) {
    return (rowCount % this.tableInfo.commitSize) === 0;
  }

  async appendRow(row) {
    this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
      const dataType = Yadamu.decomposeDataType(targetDataType);
      if (row[idx] !== null) {
        switch (dataType.type) {
          case "tinyblob" :
            row[idx] = Buffer.from(row[idx],'hex');
            break;
          case "blob" :
            row[idx] = Buffer.from(row[idx],'hex');
            break;
          case "mediumblob" :
            row[idx] = Buffer.from(row[idx],'hex');
            break;
          case "longblob" :
            row[idx] = Buffer.from(row[idx],'hex');
            break;
          case "varbinary" :
            row[idx] = Buffer.from(row[idx],'hex');
            break;
          case "binary" :
            row[idx] = Buffer.from(row[idx],'hex');
            break;
          case "json" :
            if (typeof row[idx] === 'object') {
              row[idx] = JSON.stringify(row[idx]);
            }
            break;
          default :
        }
      }
    },this)
    this.batch.push(row);
  }

  hasPendingRows() {
    return this.batch.length > 0;
  }
      
  async writeBatch() {
     this.insertMode = 'Batch';
     try {
      if (this.tableInfo.useSetClause) {
        this.insertMode = 'Iterative';
        for (const i in this.batch) {
          try {
            const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[i]);
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
        const results = await this.dbi.executeSQL(this.tableInfo.dml,[this.batch]);
      }
      this.endTime = new Date().getTime();
      this.batch.length = 0;
      return this.skipTable
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
    return this.skipTable
  }

  async finalize() {
    if (this.hasPendingRows()) {
      this.skipTable = await this.writeBatch();   
    }
    await this.dbi.commitTransaction();
    return {
      startTime    : this.startTime
    , endTime      : this.endTime
    , insertMode   : this.insertMode
    , skipTable    : this.skipTable
    }    
  }

}

module.exports = TableWriter;