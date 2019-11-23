"use strict"

const WKX = require('wkx');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
  }

  async appendRow(row) {
    this.tableInfo.sourceDataTypes.forEach(function(sourceDataType,idx) {
      const dataType = this.dbi.decomposeDataType(sourceDataType);
      if (row[idx] !== null) {
        switch (dataType.type) {
          case '"MDSYS"."SDO_GEOMETRY"':
          case 'geography':
          case 'geometry':
            switch (this.dbi.systemInformation.spatialFormat) {
              case "WKB":
                row[idx]  = JSON.stringify(WKX.Geometry.parse(Buffer.from(row[idx],'hex')).toGeoJSON())
                break;
              case "EWKB":
                break;
              case "WKT":
                break;
              case "EWKT":
                break;
              default:
            }
            break;
          default:
        }
      }
    },this)
    this.batch.push(row);
    return this.batch.length;
  }

  async writeBatch() {
    this.batchCount++;

   
    if (this.tableInfo.insertMode === 'Batch') {
      try {
        const result = await this.dbi.executeSQL(dmlStatement,this.batch);
        this.endTime = new Date().getTime();
        this.batch.length = 0;  
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.batch.length}]. Batch Insert raised:\n${e}.`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch[0])}\n...\n${JSON.stringify(this.batch[this.batch.length-1])}\n`);
          this.yadamuLogger.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative mode.`);          
        }
        this.tableInfo.insertMode = 'Iterative'   
      }
    }
          
    for (const row in this.batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[row])
        await this.processWarnings(results);
      } catch (e) {
        const errInfo = this.status.showInfoMsgs === true ? [this.tableInfo.dml,this.batch[row]] : []
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