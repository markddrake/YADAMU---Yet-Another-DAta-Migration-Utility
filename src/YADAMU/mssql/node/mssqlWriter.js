"use strict"

const { performance } = require('perf_hooks');
const YadamuWriter = require('../../common/yadamuWriter.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const NullWriter = require('../../common/nullWriter.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');
const {DatabaseError,RejectedColumnValue} = require('../../common/yadamuException.js');

class MsSQLWriter extends YadamuWriter {
    
  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  setTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
	const transformations = this.dataTypes.map((dataType,idx) => {      
	  switch (dataType.type.toLowerCase()) {
        case "json":
		  return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
		  }
          break;
		case 'bit':
        case 'boolean':
		  return (col,idx) => {
            return YadamuLibrary.toBoolean(col)
		  }
          break;
        case "datetime":
		  return (col,idx) => {
            if (typeof col === 'string') {
              col = col.endsWith('Z') ? col : (col.endsWith('+00:00') ? `${col.slice(0,-6)}Z` : `${col}Z`)
            }
            else {
              // Alternative is to rebuild the table with these data types mapped to date objects ....
              col = col.toISOString();
            }
            if (col.length > 23) {
               col = `${col.substr(0,23)}Z`;
            }
			return col;
		  }
          break;
		case "time":
        case "date":
        case "datetime2":
        case "datetimeoffset":
		  return (col,idx) => {
            if (typeof col === 'string') {
              col = col.endsWith('Z') ? col : (col.endsWith('+00:00') ? `${col.slice(0,-6)}Z` : `${col}Z`)
            }
            else {
              // Alternative is to rebuild the table with these data types mapped to date objects ....
              col = col.toISOString();
            }
			return col;
		  }
          break;
 		case "real":
        case "float":
		case "double":
		case "double precision":
		case "binary_float":
		case "binary_double":
		  switch (this.dbi.INFINITY_MANAGEMENT) {
		    case 'REJECT':
              return (col, idx) => {
			    if (!isFinite(col)) {
			      throw new RejectedColumnValue(this.tableInfo.columnNames[idx],col);
			    }
				return col;
		      }
		    case 'NULLIFY':
			  return (col, idx) => {
			    if (!isFinite(col)) {
                  this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],`Column "${this.tableInfo.columnNames[idx]}" contains unsupported value "${col}". Column nullified.`);
	  		      return null;
				}
			    return col
		      }   
			default:
			  return null;
	      }
		default :
		  return null
      }
    })
	
    // Use a dummy rowTransformation function if there are no transformations required.

	return transformations.every((currentValue) => { currentValue === null}) 
	? (row) => {} 
	: (row) => {
      transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
      }) 
    }
  }	  

  
  setTableInfo(tableName) {
	super.setTableInfo(tableName)
	this.tableInfo.insertMode = 'Bulk';

	this.useNext = 0;
	this.newBatch()
    	
    this.dataTypes  = YadamuLibrary.decomposeDataTypes(this.tableInfo.targetDataTypes)
    this.rowTransformation  = this.setTransformations(this.tableInfo.targetDataTypes)
		
  }
      
  newBatch() {
	super.newBatch();
	// console.log('newBatch(): Using Operation',this.useNext)
	this.batch = this.tableInfo.bulkOperations[this.useNext]
	// Exclusive OR (XOR) operator 1 becomes 0, 0 becomes 1.
	this.useNext ^= 1;
  }
  
  releaseBatch(batch) {
	if (Array.isArray(batch.rows)) {
	  batch.rows.length = 0;
	}
  }
  
  getMetrics()  {
	const results = super.getMetrics()
	results.insertMode = this.tableInfo ? (this.tableInfo.bulkSupported === true ? 'Bulk' : 'Iterative' ) : 'Bulk'
	return results;
  }
  
  cacheRow(row) {
      
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	
	try {
      this.rowTransformation(row)	
      this.batch.rows.add(...row);
 
  	  this.metrics.cached++;
	  return this.skipTable;
	} catch (e) {
  	  if (e instanceof RejectedColumnValue) {
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],e.message);
        this.dbi.yadamu.REJECTION_MANAGER.rejectRow(this.tableName,row);
		this.metrics.skipped++
        return
	  }
	  throw e
	}
  }

  reportBatchError(batch,operation,cause) {
   
    const additionalInfo = {
      columnDefinitions: batch.columns
	}

    super.reportBatchError(operation,cause,batch.rows[0],batch.rows[batch.rows.length-1],additionalInfo)
  }
  
  async _writeBatch(batch,rowCount) {
	  	  
    this.metrics.batchCount++;
    
    // console.log(this.tableInfo.bulkSupported,)
    // console.dir(batch,{depth:null})
    
    if (this.SPATIAL_FORMAT === 'GeoJSON') {
      YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch.rows,true)
    } 

    if (this.tableInfo.bulkSupported) {
      try {        
        await this.dbi.createSavePoint();
        const results = await this.dbi.bulkInsert(batch);
		this.endTime = performance.now();
        this.metrics.written += rowCount;
		this.releaseBatch(batch)
		return this.skipTable
      } catch (cause) {
	    this.reportBatchError(batch,`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
		if (this.dbi.TRANSACTION_IN_PROGRESS && this.dbi.tediousTransactionError) {
		  // this.yadamuLogger.trace([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableName}"`],`Unexpected ROLLBACK during BCP Operation. Starting new Transaction`);          
		  await this.dbi.recoverTransactionState(true)
		}	
	  	this.yadamuLogger.warning([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableName}"`],`Switching to Iterative mode.`);          
        this.tableInfo.bulkSupported = false;
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.
 
    try {
      await this.dbi.cachePreparedStatement(this.tableInfo.dml, this.dataTypes,this.SPATIAL_FORMAT) 
    } catch (cause) {
      if (this.rowsLost()) {
		throw cause
      }
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`INSERT ONE`,`"${this.tableName}"`],cause);
      this.endTime = performance.now();
      this.releaseBatch(batch)
      return this.skipTable          
    }	
	
	const sqlTrace = this.status.sqlTrace

    for (const row in batch.rows) {
      try {
        const args = {}
        for (const col in batch.rows[0]){
           args['C'+col] = batch.rows[row][col]
        }
        const results = await this.dbi.executeCachedStatement(args);
		this.metrics.written++
        this.status.sqlTrace = NullWriter.NULL_WRITER;
      } catch (cause) {
		if (this.dbi.TRANSACTION_IN_PROGRESS && this.dbi.tediousTransactionError) {
		  // this.yadamuLogger.trace([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableName}"`],`Unexpected ROLLBACK during BCP Operation. Starting new Transaction`);          
		  await this.dbi.recoverTransactionState(true)
		}	
        this.handleIterativeError(`INSERT ONE`,cause,row,batch.rows[row]);
        if (this.skipTable) {
          break;
		}
      }
    }       
      
	this.status.sqlTrace = sqlTrace
	this.status.sqlTrace.write(this.dbi.traceComment(`Previous Statement repeated ${rowCount} times.`))
	await this.dbi.clearCachedStatement();   
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable
  }

  async finalize(cause) {
	await super.finalize(cause)
    await this.dbi.clearCachedStatement()
  }
  
}

module.exports = MsSQLWriter;