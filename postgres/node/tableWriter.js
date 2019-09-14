"use strict"

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.tableInfo.columnCount = this.tableInfo.targetDataTypes.length;
    this.batchRowCount = 0;
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
      
  async writeBatch() {

    this.batchCount++;
    let repackBatch = false;

    if (this.insertMode === 'Batch') {
               
      try {
        await this.dbi.createSavePoint();
        let argNumber = 1;
        const args = Array(this.batchRowCount).fill(0).map(function() {return `(${this.tableInfo.insertOperators.map(function(operator) {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`},this).join(',');
        const sqlStatement = this.tableInfo.dml + args
        const results = await this.dbi.insertBatch(sqlStatement,this.batch);
        this.endTime = new Date().getTime();
        await this.dbi.releaseSavePoint();
        this.batch.length = 0;
        this.batchRowCount = 0;
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.batchRowCount}].  Batch Insert raised:\n${e}.`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          // Use Slice to print first and last row, rather than first and last value.
          this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch.slice(0,this.tableInfo.columnCount))}\n...\n${JSON.stringify(this.batch.slice(this.batch.length-this.tableInfo.columnCount,this.batch.length))}\n`);
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
        }
        await this.dbi.restoreSavePoint();
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
        this.dbi.createSavePoint();
        const results = await this.dbi.insertBatch(sqlStatement,nextRow);
        this.dbi.releaseSavePoint();
      } catch(e) {
        this.dbi.restoreSavePoint();
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml] : []
        this.skipTable = await this.dbi.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.batchRowCount,row,nextRow,e,errInfo);
        if (this.skipTable) {
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