"use strict"

import { performance } from 'perf_hooks';

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuConstants from '../../lib/yadamuConstants.js';
import YadamuOutputManager from '../base/yadamuOutputManager.js';
import PerformanceReporter from '../../util/performanceReporter.js';

class JSONOutputManager extends YadamuOutputManager {
	     
  constructor(dbi,tableName,metrics,firstTable,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
	this.startTable = firstTable ? `"${tableName}":[` :  `,"${tableName}":[`
	this.rowSeperator = '';
	this.rowCount = 0
  }
    
  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
    return this.tableInfo.targetDataTypes.map((targetDataType,idx) => {      
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
	  if (YadamuLibrary.isBinaryType(dataType.type)) {
		return (col,idx) =>  {
          return col.toString('hex')
		}
      }
      
	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case "POINT":
        case "LSEG":
        case "BOX":
        case "PATH":
        case "POLYGON":
        case "CIRCLE":
        case "LINESTRING":
        case "MULTIPOINT":
        case "MULTILINESTRING":
        case "MULTIPOLYGON":
		case "GEOMCOLLECTION":
		case "GEOMETRYCOLLECTION":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (col,idx)  => {
			  if (Buffer.isBuffer(col)) {
			    return col.toString('hex')
			  }
			  return col
			}
          }
          if (this.SPATIAL_FORMAT.endsWith('GeoJSON')) {
            return (col,idx)  => {
			  if (typeof col === 'string') {
				try {
			      return JSON.parse(col)
				} catch(e) {}
			  }
			  return col
			}
          }
		  return null;
		case "REAL":
        case "FLOAT":
		case "DOUBLE":
		case "DOUBLE PRECISION":
		case "BINARY_FLOAT":
		case "BINARY_DOUBLE":
		   return (col,idx) => {
			 if (!isFinite(col)) {
			   switch (true) {
		         case isNaN(col): 
		   	       return "NaN"
				   break;
			     case (col === Infinity):
				   return "Infinity"
				   break;
				 case (col === -Infinity):
				   return "-Infinity"
				   break;
				 default:
			   }   
		     }
  		     return col
		   }			 
        case "JSON":
          return (col,idx) =>  {
			try {
              if (typeof col === 'string') {
                return JSON.parse(col)
              } 
			  if (Buffer.isBuffer(col)) {
			    return JSON.parse(col.toString('utf8'))
			  }
			} catch (e) { return { "YADAMU_INVALID_JSON_VALUE" : col }}
   		    return col
          }
        case "BOOLEAN":
          return (col,idx) =>  {
		    const bool = (typeof col === 'string') ? col.toUpperCase() : (Buffer.isBuffer(col)) ? col.toString('hex') : col
			switch(bool) {
              case true:
              case "TRUE":
              case "01":
              case "1":
			  case 1:
                return true;
				break;
              case false:
              case "FALSE":
              case "00":
              case "0":
			  case 0:
                return false;
				break;
              default: 
            }
            return col
          }
        case "DATE":
          return (col,idx) =>  { 
            if (col instanceof Date) {
              return col.toISOString()
            }
		    return col
          }
        case "DATETIME":
        case "DATETIME2":
        case "TIMESTAMP":
		  // YYYY-MM-DDTHH24:MI:SS.nnnnnnnnn
		  // Timestamps are truncated to a maximum of 6 digits
          // Timestamps not explicitly marked as UTC are coerced to UTC.
		  // Timestamps using a '+00:00' are converted are converted to 
		  const tsMaxLength = 20 + this.dbi.TIMESTAMP_PRECISION
		  return (col,idx) =>  { 
		    let ts
			switch (true) {
			  case (col === null):
			    return null
              case (col instanceof Date):
                return col.toISOString()
              case col.endsWith('+00:00'):
			    ts = col.slice(0,-6) 
				return `${ts.slice(0,tsMaxLength)}Z`
              case col.endsWith('Z'):
			    ts = col.slice(0,-1) 
			    return `${ts.slice(0,tsMaxLength)}Z`
			  default:
			    return `${col.slice(0,tsMaxLength)}Z`
            }
          }
		default:
		  return null
      }
    }) 
	
    // Use a dummy rowTransformation function if there are no transformations required.

  }
		
  async setTableInfo(tableName) {
	  
	await super.setTableInfo(tableName)
	if (this.dbi.tableMappings && this.dbi.tableMappings.hasOwnProperty(tableName)) {
	  this.tableName = this.dbi.tableMappings[tableName].tableName
	}
  }
  
  beginTable()  {
	this.push(this.startTable)  
  }
  
  async initializeTable(tableName) {
	this.beginTable()
    await super.initializeTable(tableName)
  }
  
  batchComplete() {
    return false
  }
  
  commitWork(rowCount) {
    return false;
  }
  
  formatRow(row) {
    return `${this.rowSeperator}${JSON.stringify(row)}`
  }
  
  _processRow(row) {
    // Be very careful about adding unecessary code here. This is executed once for each row processed by YADAMU. Keep it as lean as possible.
	this.rowCount++
	this.checkColumnCount(row)

	this.rowTransformation(row)

    this.push(this.formatRow(row));
	this.rowSeperator = ','
    this.COPY_METRICS.committed++;
    if ((this.FEEDBACK_INTERVAL > 0) && ((this.COPY_METRICS.committed % this.FEEDBACK_INTERVAL) === 0)) {
      this.yadamuLogger.info([`${this.tableName}`,this.dbi.OUTPUT_FORMAT],`Rows Written: ${this.COPY_METRICS.committed}.`);
    }
  }
  
  async doTransform(messageType,obj) {
	 
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),messageType,this.COPY_METRICS.received,this.writableLength,this.writableHighWaterMark],'doTransform()')
	
    switch (messageType) {
      case 'data':
        // processRow() becomes a No-op after calling abortTable()
	    await this.processRow(obj.data)
		break
      case 'table':
        // processRow() becomes a No-op after calling abortTable()
        await this.initializeTable(obj.table)
		break
	  case 'partition':
        await this.initializePartition(obj.partition)
	    break;  
      case 'eod':
        // Used when processing serial data sources such as files to indicate that all records have been processed by the writer
        // this.yadamuLogger.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType,this.displayName,this.rowCount],`${YadamuConstants.END_OF_DATA}`)  
        this.emit(YadamuConstants.END_OF_DATA)
      default:
    }
		
  }
  
  finalizeTable() {
	this.push(']')
  }
  
  processPendingRows() {
	 
	// Called from _flush() when there are no more rows to process for the current table.
	// Generate the required reporting based on this component having no further data, since the downstream pipeline components remain open.
	  
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.skipTable,this.dbi.TRANSACTION_IN_PROGRESS,this.writableEnded,this.writableFinished,this.destroyed,this.hasPendingRows(),this.COPY_METRICS.received,this.COPY_METRICS.committed,this.COPY_METRICS.written,this.COPY_METRICS.cached],`JSONOutputManager.processPendingRows(${this.hasPendingRows()})`)
	this.COPY_METRICS.managerEndTime = performance.now()
	this.finalizeTable()
  }
  
  async doDestroy(err) {
    // Workaround for unexpected "[ERR_STREAM_DESTROYED]: Cannot call pipe after a stream was destroyed" exceptions	 
    if (this.writableEnded && this.writableFinished && err?.code === 'ERR_STREAM_DESTROYED') {
      // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),this.COPY_METRICS.received,this.COPY_METRICS.cached,this.COPY_METRICS.written,this.COPY_METRICS.skipped,this.COPY_METRICS.lost,this.writableEnded,this.writableFinished,err.code],`JSON_Writer_destroy(): Swallowed error "${err.message}".`)
      err = undefined
	}
	
	// Defer performance reporting to here... Ensures parser 'finish' event has occurred.
	this.reportGenerator = new PerformanceReporter(this.dbi,this.tableInfo,this.COPY_METRICS,{},this.yadamuLogger)
	this.reportGenerator.reportPerformance(err)
	this.reportGenerator.end()
	await super.doDestroy(err)
  }
  

}

export {JSONOutputManager as default }