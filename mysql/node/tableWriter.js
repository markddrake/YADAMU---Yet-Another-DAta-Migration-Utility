"use strict"

const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.tableInfo.columnCount = this.tableInfo.targetDataTypes.length;
    this.tableInfo.args =  '(' + Array(this.tableInfo.columnCount).fill('?').join(',')  + ')';    
  }

  async finalize() {
    const results = await super.finalize()
    results.insertMode = this.tableInfo.insertMode
    return results;
  }

  async appendRow(row) {
    this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
      const dataType = this.dbi.decomposeDataType(targetDataType);
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
          case "date":
          case "time":
          case "datetime":
          case "timestamp":
            // If the the input is a string, assume 8601 Format with "T" seperating Date and Time and Timezone specified as 'Z' or +00:00
            // Neeed to convert it into a format that avoiods use of convert_tz and str_to_date, since using these operators prevents the use of Bulk Insert.
            // Session is already in UTC so we safely strip UTC markers from timestamps
            if (typeof row[idx] !== 'string') {
              row[idx] = row[idx].toISOString();
            }             
            row[idx] = row[idx].substring(0,10) + ' '  + (row[idx].endsWith('Z') ? row[idx].substring(11).slice(0,-1) : (row[idx].endsWith('+00:00') ? row[idx].substring(11).slice(0,-6) : row[idx].substring(11)))
            // Truncate fractional values to 6 digit precision
            // ### Consider rounding, but what happens at '9999-12-31 23:59:59.999999
            row[idx] = row[idx].substring(0,26);
          default :
        }
      }
    },this)
    this.batch.push(row);
  }

  async processWarnings(results) {
    // ### Output Records that generate warnings
    if (results.warningCount >  0) {
      const warnings = await this.dbi.executeSQL('show warnings');
      warnings.forEach(function(warning,idx) {
        if (warning.Level === 'Warning') {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Warnings reported by bulk insert operation. Details: ${JSON.stringify(warning)}`)
          this.yadamuLogger.writeDirect(`${this.batch[idx]}\n`)
        }
      },this)
    }
  }
  
  async writeBatch() {     

    this.batchCount++;
   
    if (this.tableInfo.insertMode === 'Batch') {
      try {
        await this.dbi.createSavePoint();
        const results = await this.dbi.executeSQL(this.tableInfo.dml,[this.batch]);
        await this.processWarnings(results);
        this.endTime = new Date().getTime();
        await this.dbi.releaseSavePoint();
        this.batch.length = 0;  
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.batch.length}]. Batch Insert raised:\n${e}.`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch[0])}\n...\n${JSON.stringify(this.batch[this.batch.length-1])}\n`);
          this.yadamuLogger.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative mode.`);          
        }
        await this.dbi.restoreSavePoint();
        this.tableInfo.insertMode = 'Iterative'   
        this.tableInfo.dml = this.tableInfo.dml.slice(0,-1) + this.tableInfo.args
      }
    }
          
    for (const row in this.batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[row])
        await this.processWarnings(results);
      } catch (e) {
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml] : []
        this.skipTable = await this.dbi.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.batch.length,row,this.batch[row],e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }     
    
    this.endTime = new Date().getTime();
    this.batch.length = 0;  
    return this.skipTable 
  }
}

module.exports = TableWriter;