  "use strict"

import { performance } from 'perf_hooks';

import YadamuWriter from '../base/yadamuWriter.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js';
import {DatabaseError,RejectedColumnValue} from '../../core/yadamuException.js';

class MySQLWriter extends YadamuWriter {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
   
  async processWarnings(results,row) {

    // ### Output Records that generate warnings

    let badRow = 0;

    if (results.warningCount >  0) {
      const warnings = await this.dbi.executeSQL('show warnings');
      // warnings.forEach(async (w1arning,idx) => {
      for (const warning of warnings) {
        if (warning.Level === 'Warning') {
          let nextBadRow = warning.Message.split('row')
          nextBadRow = parseInt(nextBadRow[nextBadRow.length-1])
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode,nextBadRow],`${warning.Code} Details: ${warning.Message}.`)
		  
		  // Only write rows to Rejection File in Iterative Mode. 
		  
          if ((this.tableInfo.insertMode === 'Iterative') && (badRow !== nextBadRow)) {
            const columnOffset = (nextBadRow-1) * this.tableInfo.columnNames.length
            this.dbi.yadamu.WARNING_MANAGER.rejectRow(this.tableName,row);
            badRow = nextBadRow;
          }
        }
      }
    }
  }
  
  recodeSpatialColumns(batch,msg) {
    this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableName,`INSERT ROWS`,this.COPY_METRICS.cached,this.SPATIAL_FORMAT],`${msg} Converting batch to "WKT".`);
    YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch,false)
  }  
  
  async retryGeoJSONAsWKT(sqlStatement,rowNumber,row) {
    YadamuSpatialLibrary.recodeSpatialColumns('GeoJSON','WKT',this.tableInfo.targetDataTypes,row,false)
    try {
	  // Create a bound row by cloning the current set of binds and adding the column value.
      sqlStatement = sqlStatement.replace(/ST_GeomFromGeoJSON\(\?\)/g,'ST_GeomFromText(?)')
      const results = await this.dbi.executeSQL(sqlStatement,row)
      this.adjustRowCounts(1)
    } catch (cause) {
	  this.handleIterativeError('INSERT ONE',cause,rowNumber,row);
    }
  }
  
  reportBatchError(batch, operation,cause) {
	if (this.tableInfo.insertMode === 'Rows') {
      super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
	}
	else {
   	  super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
	}
  }
  
  async _writeBatch(batch,rowCount) {     

    // console.log(batch[0])

	// this.yadamuLogger.trace([this.constructor.name,'_writeBatch',this.tableName,this.tableInfo.insertMode,this.COPY_METRICS.batchNumber,rowCount,batch.length],'Start')    
	
    let recodedBatch = false;
	
	switch (this.tableInfo.insertMode) {
      case 'Batch':
        try {
          await this.dbi.createSavePoint();
		  const bulkInsertStatement =  `${this.tableInfo.dml} ?`
          const results = await this.dbi.executeSQL(bulkInsertStatement,[batch]);
          await this.processWarnings(results,null);
          this.endTime = performance.now();
          await this.dbi.releaseSavePoint();
   		  this.adjustRowCounts(rowCount);
          this.releaseBatch(batch)
          return this.skipTable
        } catch (cause) {
   		  this.reportBatchError(batch,'INSERT MANY',cause)
          await this.dbi.restoreSavePoint(cause);
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Multi-row insert mode.`);     
		  this.tableInfo.insertMode = "Rows"
          batch = batch.flat()
        }
      case 'Rows':
	    while (true) {
          try {
            await this.dbi.createSavePoint();    
            const multiRowInsert = `${this.tableInfo.dml} ${new Array(rowCount).fill(this.tableInfo.rowConstructor).join(',')}`
            const results = await this.dbi.executeSQL(multiRowInsert,batch);
            await this.processWarnings(results,null);
            this.endTime = performance.now();
            await this.dbi.releaseSavePoint();
	   	    this.adjustRowCounts(rowCount);
            this.releaseBatch(batch)
            return this.skipTable
          } catch (cause) {
            await this.dbi.restoreSavePoint(cause);
			// If it's a spatial error recode the entire batch and try again.
            if ((cause instanceof DatabaseError) && cause.spatialError() && !recodedBatch) {
              recodedBatch = true;
			  this.recodeSpatialColumns(batch,cause.message)
			  this.tableInfo.rowConstructor = this.tableInfo.rowConstructor.replace(/ST_GeomFromWKB\(\?\)/g,'ST_GeomFromText(?)')
			  continue;
		    }
	   	    this.reportBatchError(batch,'INSERT ROWS',cause)
            this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);    
  		    this.tableInfo.insertMode = "Iterative"
            break;			
		  }
        }
      case 'Iterative':     
	    break;
      default:
    }     
		
    // Batch or Rows failed, or iterative was selected.
	const singleRowInsert = `${this.tableInfo.dml} ${this.tableInfo.rowConstructor}`
	for (let row = 0; row < rowCount; row++) {
	  const offset = row * this.tableInfo.columnCount
      const nextRow  = batch.length > rowCount ? batch.slice(offset,offset + this.tableInfo.columnCount) : batch[row]
      try {
		const results = await this.dbi.executeSQL(singleRowInsert,nextRow)
        await this.processWarnings(results,nextRow);
  	    this.adjustRowCounts(1);
      } catch (cause) { 
	    if (cause.spatialErrorGeoJSON()) {
		  await this.retryGeoJSONAsWKT(singleRowInsert,row,nextRow)
	    }
		else {
          this.handleIterativeError(`INSERT ONE`,cause,row,nextRow);
		}
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

export { MySQLWriter as default }