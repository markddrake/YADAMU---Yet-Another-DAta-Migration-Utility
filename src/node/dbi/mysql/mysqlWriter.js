
import { performance } from 'perf_hooks';

import YadamuWriter from '../base/yadamuWriter.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js';
import {DatabaseError,RejectedColumnValue} from '../../core/yadamuException.js';

class MySQLWriter extends YadamuWriter {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
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
          this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode,nextBadRow],`${warning.Code} Details: ${warning.Message}.`)
		  
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
    this.LOGGER.info([this.dbi.DATABASE_VENDOR,this.tableName,`INSERT ROWS`,this.PIPELINE_STATE.cached,this.SPATIAL_FORMAT],`${msg} Converting batch to "WKT".`);
    YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch,false)
    this.dbi.resetExceptionTracking()
  }  
  
  async retryGeoJSONAsWKT(sqlStatement,rowNumber,row) {
    YadamuSpatialLibrary.recodeSpatialColumns('GeoJSON','WKT',this.tableInfo.targetDataTypes,row,false)
    try {
	  // Create a bound row by cloning the current set of binds and adding the column value.
      sqlStatement = sqlStatement.replace(/ST_GeomFromGeoJSON\(\?\)/g,'ST_GeomFromText(?)')
      const results = await this.dbi.executeSQL(sqlStatement,row)
      this.adjustRowCounts(1)
	  this.dbi.resetExceptionTracking()
    } catch (cause) {
	  this.handleIterativeError('INSERT ONE',cause,rowNumber,row);
    }
  }
  
  reportBatchError(operation,cause,batch) {
	if (this.tableInfo.insertMode === 'Rows') {
      super.reportBatchError(operation,cause,batch.slice(0,this.tableInfo.columnCount),batch.slice(batch.length-this.tableInfo.columnCount,batch.length))
	}
	else {
   	  super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
	}
  }
  
  async _writeBatch(batch,rowCount) {     

    // console.log(rowCount,batch)

	// this.LOGGER.trace([this.constructor.name,'_writeBatch',this.tableName,this.tableInfo.insertMode,this.PIPELINE_STATE.batchNumber,rowCount,batch.length],'Start')    
	
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
   		  this.reportBatchError('INSERT MANY',cause,batch)
          await this.dbi.restoreSavePoint(cause);
          this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Multi-row insert mode.`);     
          this.dbi.resetExceptionTracking()
		  this.tableInfo.insertMode = "Rows"
          batch = batch.flat()
        }
      case 'Rows':
	    while (true) {
          try {
            await this.dbi.createSavePoint();    
            const multiRowInsert = `${this.tableInfo.dml} ${new Array(rowCount).fill(this.tableInfo.rowConstructor).join(',')}`
            const results = await this.dbi.executeSQL(multiRowInsert,batch.flat());
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
	   	    this.reportBatchError('INSERT ROWS',cause,batch)
            this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);    
		    this.dbi.resetExceptionTracking()
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