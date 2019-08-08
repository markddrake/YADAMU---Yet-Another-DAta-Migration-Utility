"use strict"

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.batchRowCount = 0;
  }

  async initialize() {
    this.dbi.beginTransaction();
  }

  batchComplete() {
    return (this.batchRowCount  === this.tableInfo.batchSize)
  }
  
  batchRowCount() {
    return this.batchRowCount
  }
  
  async appendRow(row) {
    this.tableInfo.targetDataTypes.forEach(async function(targetDataType,idx) {
      const dataType = this.dbi.decomposeDataType(targetDataType);
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
          case "boolean" :
            switch (row[idx]) {
              case "00" :
                row[idx] = false;
                break;
              case "01" :
                row[idx] = true;
                break;
              default:
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
            if (typeof row[idx] === 'string') {
              if (row[idx].endsWith('Z') && row[idx].length === 28) {
                row[idx] = row[idx].slice(0,-2) + 'Z'
              }
              else {
                if (!row[idx].endsWith('+00:00')) {
                  if (row[idx].length === 27) {                                
                    row[idx] = row[idx].slice(0,-1) 
                  }
                }
              }               
            }
            else {
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

    this.repackBatch = false;
    this.batchCount++;

    if (this.insertMode === 'Batch') {
      try {
        // Slice removes the unwanted last comma from the replicated args list.
        let argNumber = 1;
        const args = Array(this.batchRowCount).fill(0).map(function() {return `(${Array(this.tableInfo.targetDataTypes.length).fill(0).map(function(){return `$${argNumber++}`}).join(',')})`},this).join(',');
        const sqlStatement = this.tableInfo.dml + args
        const results = await this.dbi.insertBatch(sqlStatement,this.batch);
        this.endTime = new Date().getTime();
        this.batch.length = 0;
        this.batchRowCount = 0;
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"}`,`Batch size: ${this.tableInfo.bulkOperation.rows.length}`],`Bulk Operation raised:\n${e.message}`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          this.yadamuLogger.writeDirect(`{${JSON.stringify(this.tableInfo.bulkOperation.columns)}`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.bulkOperation.rows[0]}\n...\n${this.tableInfo.bulkOperation.rows[this.tableInfo.bulkOperation.rows.length-1]}\n`)
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"}`],`Switching to Iterative operations.`);          
        }
        await this.dbi.rollbackTransaction();
        this.tableInfo.insertMode = 'Iterative' 
        repackBatch = true;
      }
    } 
     
    let argNumber = 1;
    const args = Array(1).fill(0).map(function() {return `(${Array(this.tableInfo.targetDataTypes.length).fill(0).map(function(){return `$${argNumber++}`}).join(',')})`},this).join(',');
    const sqlStatement = this.tableInfo.dml + args
    for (let row =0; row < this.batchRowCount; row++) {
      const nextRow = repackBatch ?  this.batch.splice(0,this.tableInfo.columnCount) : this.batch[row]
      try {
        const results = await this.dbi.insertBatch(sqlStatement,nextRow);
      } catch(e) {
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml] : []
        const abort = this.dbi.handleInsertError('writeBatch()',this.tableName,this.batchRowCount,nextRow,e,errInfo);
        if (abort) {
          await this.dbi.rollbackTransaction();
          this.skipTable = true;
          break;
        }
      }
    }

    this.endTime = new Date().getTime();
    this.batchRowCount = 0;
    this.batch.length = 0;
    return this.skipTable   
  }
}

module.exports = TableWriter;