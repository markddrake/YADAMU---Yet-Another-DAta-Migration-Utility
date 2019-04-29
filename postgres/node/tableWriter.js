"use strict"

const Yadamu = require('../../common/yadamu.js');

class TableWriter {

  constructor(dbi,tableName,tableInfo,status,logWriter) {
    this.dbi = dbi;
    this.tableName = tableName
    this.tableInfo = tableInfo;
    this.status = status;
    this.logWriter = logWriter;    

    this.batch = [];
    this.lobList = [];

    this.lobUsage = 0;
    this.batchRowCount = 0;
    
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.insertMode = 'Insert';

    this.skipTable = false;

    this.logDDLIssues   = (this.status.loglevel && (this.status.loglevel > 2));
    this.logDDLIssues   = true;
  }

  async initialize() {
    this.dbi.beginTransaction();
  }

  batchComplete() {
    return (this.batchRowCount  === this.tableInfo.batchSize)
  }
  
  commitWork(rowCount) {
    return (rowCount % this.tableInfo.commitSize) === 0;
  }

  async appendRow(row) {
    this.tableInfo.targetDataTypes.forEach(async function(targetDataType,idx) {
      const dataType = Yadamu.decomposeDataType(targetDataType);
      if (row[idx] !== null) {
        switch (dataType.type) {
          case "bit" :
            if (row[idx] === true) {
              row[idx] = 1
            }
            else {
              row[idx] = 0
            }  
            break;
          case "bytea" :
            row[idx] = Buffer.from(row[idx],'hex');
            break;
          case "time" :
            if (typeof row[idx] === 'string') {
              let components = row[idx].split('T')
              row[idx] = components.length === 1 ? components[0] : components[1]
              row[idx] = row[idx].split('Z')[0]
            }
            else {
              row[idx] = row[idx].getUTCHours() + ':' + row[idx].getUTCMinutes() + ':' + row[idx].getUTCSeconds() + '.' + row[idx].getUTCMilliseconds();  
            }
            break;
          case 'date':
          case 'datetime':
          case 'timestamp':
            if (typeof row[idx] !== 'string') {
              // Avoid unexpected Time Zone Conversions when inserting from a Javascript Date object 
              row[idx] = row[idx].toISOString();
            }
            break;
          default :
        }
      }
    },this)
    this.batch.push(...row);
    this.batchRowCount++
  }

  hasPendingRows() {
    return this.batch.length > 0;
  }
      
  async writeBatch() {
    try {
      // Slice removes the unwanted last comma from the replicated args list.
      let argNumber = 1;
      const args = Array(this.batchRowCount).fill(0).map(function() {return `(${Array(this.tableInfo.targetDataTypes.length).fill(0).map(function(){return `$${argNumber++}`}).join(',')})`},this).join(',');
      const sqlStatement = this.tableInfo.dml + args
      const results = await this.dbi.insertBatch(sqlStatement,this.batch);
      this.endTime = new Date().getTime();
      this.batch.length = 0;
      this.batchRowCount = 0;
    } catch (e) {
      await this.dbi.rollbackTransaction();
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