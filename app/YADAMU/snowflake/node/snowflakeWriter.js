"use strict"

const { performance } = require('perf_hooks');

const WKX = require('wkx');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const {BatchInsertError} = require('../../common/yadamuError.js')

class SnowflakeWriter extends YadamuWriter {

  constructor(dbi,tableName,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,status,yadamuLogger)
  }
  
  setTableInfo(tableInfo) {
	super.setTableInfo(tableInfo)

    this.transformations = this.tableInfo.dataTypes.map((dataType,idx) => {
      switch (dataType.type) {
		/*
		**
		** Add conversion functions for specific data types here..
        FOO:
		   return (col,idx) => { conversion code for the data type of foo col }
		**
	    */
        case '"MDSYS"."SDO_GEOMETRY"':
        case 'geography':
        case 'geometry':
          switch (this.dbi.systemInformation.spatialFormat) {
            case "WKB":
              return (col,idx) => {
			    return JSON.stringify(WKX.Geometry.parse(Buffer.from(col,'hex')).toGeoJSON())
			  }
            case "EWKB":
 			  return null
            case "WKT":
 			  return null
            case "EWKT":
 			  return null
            default:
          }
		  return null
        default :
		  return null
      }
    })

  }
        
  async writeBatch() {
	  
    this.rowCounters.batchCount++;
   
    if (this.tableInfo.insertMode === 'Batch') {
      try {
        const result = await this.dbi.executeSQL(this.tableInfo.dml,this.batch);
        this.endTime = performance.now();
        this.batch.length = 0;
        this.rowCounters.written += this.rowCounters.cached;
        this.rowCounters.cached = 0;
        return this.skipTable
      } catch (e) {
        await this.dbi.restoreSavePoint(e);
		this.handleBatchException(e,'Batch Insert')
		this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative'   
      }
    }

    // Suppress SQL Trace for Iterative Inserts
	
    const sqlTrace = this.status.sqlTrace

    for (const row in this.batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[row])
		this.status.sqlTrace = undefined
		this.rowCounters.written++;
      } catch (e) {
        const errInfo = this.status.showInfoMsgs === true ? [this.tableInfo.dml,this.batch[row]] : []
        await this.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.batch.length,row,this.batch[row],e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }   
	
	this.status.sqlTrace = sqlTrace

    this.endTime = performance.now();
    this.batch.length = 0;
    this.rowCounters.cached = 0;
	return this.skipTable
  }
}

module.exports = SnowflakeWriter;