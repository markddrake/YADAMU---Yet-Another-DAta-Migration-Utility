"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const {BatchInsertError} = require('../../common/yadamuError.js')

class PostgresWriter extends YadamuWriter {

  constructor(dbi,primary,status,yadamuLogger) {
    super({objectMode: true},dbi,primary,status,yadamuLogger)
  }
  
  setTableInfo(tableInfo) {
	super.setTableInfo(tableInfo)
    this.tableInfo.columnCount = this.tableInfo.targetDataTypes.length;

    this.transformations = this.tableInfo.dataTypes.map((dataType,idx) => {
      switch (dataType.type) {
        case "bit" :
		  return (col,idx) => {
            if (col === true) {
              return 1
            }
            else {
              return 0
            }  
		  }
          break;
        case "boolean" :
 		  return (col,idx) => {
           switch (col) {
              case "00" :
                return false;
                break;
              case "01" :
                return true;
                break;
              default:
			    return col
            }
          }
          break;
        case "bytea" :
		  return (col,idx) => {
            return Buffer.from(col,'hex');
		  }
          break;
        case "time" :
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              col = components.length === 1 ? components[0] : components[1]
              return col.split('Z')[0]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds();  
            }
		  }
          break;
        case 'date':
        case 'datetime':
        case 'timestamp':
		  return (col,idx) => {
            if (typeof col === 'string') {
              if (col.endsWith('Z') && col.length === 28) {
                col = col.slice(0,-2) + 'Z'
              }
              else {
                if (!col.endsWith('+00:00')) {
                  if (col.length === 27) {                                
                    col = col.slice(0,-1) 
                  }
                }
              }               
            }
            else {
              // Avoid unexpected Time Zone Conversions when inserting from a Javascript Date object 
              col = col.toISOString();
            }
			return col
		  }
          break;
        default :
		  return null
      }
    })

  }
  
  cacheRow(row) {
	  
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	  
	this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	})
	
    this.batch.push(...row);
	
    this.rowCounters.cached++
	return this.skipTable
  }
    
  handleBatchException(cause,message) {
   
    // Use Slice to add first and last row, rather than first and last value.
    const batchException = new BatchInsertError(cause,this.tableInfo.tableName,this.tableInfo.dml,this.rowCounters.cached,this.batch.slice(0,this.tableInfo.columnCount),this.batch.slice(this.batch.length-this.tableInfo.columnCount,this.batch.length))
    this.yadamuLogger.handleWarning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],batchException)

  }
      
  async writeBatch() {

    this.rowCounters.batchCount++;
    let repackBatch = false;

    if (this.insertMode === 'Batch') {
               
      try {
        await this.dbi.createSavePoint();
        let argNumber = 1;
        const args = Array(this.rowCounters.cached).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`}).join(',');
        const sqlStatement = this.tableInfo.dml + args
        const results = await this.dbi.insertBatch(sqlStatement,this.batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.batch.length = 0;
		this.rowCounters.written += this.rowCounters.cached;
        this.rowCounters.cached = 0;
        return this.skipTable
      } catch (e) {
        await this.dbi.restoreSavePoint(e);
		this.handleBatchException(e,'Batch Insert')
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        repackBatch = true;
      }
    } 
     
    let argNumber = 1;
    const args = Array(1).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`}).join(',');
    const sqlStatement = this.tableInfo.dml + args
    for (let row =0; row < this.rowCounters.cached; row++) {
      const nextRow = repackBatch ?  this.batch.splice(0,this.tableInfo.columnCount) : this.batch[row]
      try {
        this.dbi.createSavePoint();
        const results = await this.dbi.insertBatch(sqlStatement,nextRow);
        this.dbi.releaseSavePoint();
		this.rowCounters.written++;
      } catch(e) {
        this.dbi.restoreSavePoint(e);
        const errInfo = [this.tableInfo.dml,JSON.stringify(nextRow)]
        await this.handleInsertError(`INSERT ONE`,this.rowCounters.cached,row,nextRow,e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }

    this.endTime = performance.now();
    this.batch.length = 0;
	this.rowCounters.cached = 0;
    return this.skipTable   
  }
}

module.exports = PostgresWriter;