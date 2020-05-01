"use strict"

const { performance } = require('perf_hooks');

const WKX = require('wkx');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
  }

  async appendRow(row) {
    this.tableInfo.dataTypes.forEach(function(dataType,idx) {
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
	
	this.rowsCached++;
    return this.skipTable;
  }

  async writeBatch() {

    this.batchCount++;
   
    if (this.tableInfo.insertMode === 'Batch') {
      try {
        const result = await this.dbi.executeSQL(this.tableInfo.dml,this.batch);
        this.endTime = performance.now();
        this.batch.length = 0;  
		this.rowsWritten += this.rowsCached;
		this.rowsCached = 0;
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.batch.length}]. Batch Insert raised:\n${e}.`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch[0])}\n...\n${JSON.stringify(this.batch[this.batch.length-1])}\n`);
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative mode.`);          
        }
        this.tableInfo.insertMode = 'Iterative'   
      }
    }

    // Suppress SQL Trace for Iterative Inserts
	
    const sqlTrace = this.status.sqlTrace

    for (const row in this.batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[row])
		this.status.sqlTrace = undefined
		this.rowsWritten++;
      } catch (e) {
        const errInfo = this.status.showInfoMsgs === true ? [this.tableInfo.dml,this.batch[row]] : []
        this.skipTable = await this.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.batch.length,row,this.batch[row],e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }   
	
	this.status.sqlTrace = sqlTrace

    this.endTime = performance.now();
    this.batch.length = 0;
    this.rowsCached = 0;
    return this.skipTable
  }
}

module.exports = TableWriter;