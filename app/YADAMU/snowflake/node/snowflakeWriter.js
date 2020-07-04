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
        
  handleBatchError(operation,cause) {
   	super.handleBatchError(operation,cause,this.batch[0],this.batch[this.batch.length-1])
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
      } catch (cause) {
		this.handleBatchError(`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
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
      } catch (cause) {
        await this.handleInsertError(`INSERT ONE`,cause,this.batch.length,row,this.batch[row]);
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