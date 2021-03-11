"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class PostgresWriter extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  setTableInfo(tableInfo) {
	super.setTableInfo(tableInfo)
    this.tableInfo.columnCount = this.tableInfo.columnNames.length;
    
	this.transformations = this.tableInfo.targetDataTypes.map((targetDataType,idx) => {
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
      switch (dataType.type.toLowerCase()) {
		case "tsvector":
        case "json" :
		case "jsonb":
	      // https://github.com/brianc/node-postgres/issues/442
	      return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
          }
        case "boolean" :
 		  return (col,idx) => {
             return YadamuLibrary.toBoolean(col)
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
                if (col.endsWith('+00:00')) {
			      if (col.length > 32) {
					col = col.slice(0,26) + '+00:00'
				  }
				}
				else {
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
	  
    // if (this.metrics.cached === 1) console.log('postgresWriter',row)
		
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
  	
	this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	})
	
    this.batch.push(...row);
	
    this.metrics.cached++
	return this.skipTable
  }
    
  reportBatchError(batch,operation,cause) {
    // Use Slice to add first and last row, rather than first and last value.
	super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
  }
      
  async _writeBatch(batch,rowCount) {
   
    this.metrics.batchCount++;
    let repackBatch = false;
	
	if (this.insertMode === 'Batch') {
               
      try {
        await this.dbi.createSavePoint();
        let argNumber = 1;
        const args = Array(rowCount).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`}).join(',');
        const sqlStatement = this.tableInfo.dml + args
		const results = await this.dbi.insertBatch(sqlStatement,batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.metrics.written += rowCount;
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
		this.reportBatchError(batch,`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
		this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        repackBatch = true;
      }
    } 
     
    let argNumber = 1;
    const args = Array(1).fill(0).map(() => {return `(${this.tableInfo.insertOperators.map((operator) => {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`}).join(',');
    const sqlStatement = this.tableInfo.dml + args
	for (let row = 0; row < rowCount; row++) {
	  const offset = row * this.tableInfo.columnCount
      const nextRow  = repackBatch ? batch.slice(offset,offset + this.tableInfo.columnCount) : batch[row]
      try {
        this.dbi.createSavePoint();
        const results = await this.dbi.insertBatch(sqlStatement,nextRow);
        this.dbi.releaseSavePoint();
		this.metrics.written++;
      } catch(cause) {
        this.dbi.restoreSavePoint(cause);
        this.handleIterativeError(`INSERT ONE`,cause,row,nextRow);
        if (this.skipTable) {
          break;
        }
      }
    }

    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable   
  }
  
}

module.exports = PostgresWriter;