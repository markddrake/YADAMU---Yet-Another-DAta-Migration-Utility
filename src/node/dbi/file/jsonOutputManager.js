
import { 
  performance 
}                            from 'perf_hooks';

import Yadamu                from '../../core/yadamu.js';
import YadamuConstants       from '../../lib/yadamuConstants.js';

import YadamuDataTypes       from '../base/yadamuDataTypes.js';
import YadamuOutputManager   from '../base/yadamuOutputManager.js';

import DBIConstants          from '../base/dbiConstants.js'

class JSONOutputManager extends YadamuOutputManager {
	     
  constructor(dbi,tableName,pipelineState,firstTable,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
	this.PIPELINE_STATE.displayName = this.tableName
	this.PIPELINE_STATE.ddlComplete = performance.now()
	this.startTable = firstTable ? `"${tableName}":[` :  `,"${tableName}":[`
	this.rowSeperator = '';
	this.rowCount = 0
  }
    
  generateTransformations(dataTypes) {
	  
    // RDBMS based implementations are driven off metadata dericed from the database catalog
	// File based implementatins must work with the metadata contained in the export file. 
	// Mappings are defined in ../cfg/typeMapping.json
    
	// Set up Transformation functions to be applied to the incoming rows
	
	return dataTypes.map((dataType,idx) => {      
      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType);
	  switch (true) {
		case (YadamuDataTypes.isBinary(dataTypeDefinition.type)):
		  return (col,idx) =>  {
            return col.toString('hex')
		  }
		case (YadamuDataTypes.isSpatial(dataTypeDefinition.type)):
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (col,idx)  => {
			  if (Buffer.isBuffer(col)) {
			    return col.toString('hex')
			  }
			  return col
			}
          }
          if (this.SPATIAL_FORMAT === ('GeoJSON')) {
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
	    case (YadamuDataTypes.isFloatingPoint(dataTypeDefinition.type)):
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
	    case (YadamuDataTypes.isJSON(dataTypeDefinition.type)):
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
        // case (YadamuDataTypes.isBoolean(dataTypeDefinition.type,dataTypeDefinition.length,this.tableInfo.vendor)):
        case dataTypeDefinition.type.toUpperCase() === 'BOOLEAN':
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
        case (YadamuDataTypes.isDate(dataTypeDefinition.type)):
          return (col,idx) =>  { 
            if (col instanceof Date) {
              return col.toISOString()
            }
		    return col
          }
        case (YadamuDataTypes.isTimestamp(dataTypeDefinition.type)):
          // case "DATETIME":
          // case "DATETIME2":
          // case "TIMESTAMP":
		  // YYYY-MM-DDTHH24:MI:SS.nnnnnnnnn
		  // Timestamps are truncated to a maximum of 6 digits
          // Timestamps not explicitly marked as UTC are coerced to UTC.
		  // Timestamps using a '+00:00' are converted are converted to 
		  const tsMaxLength = 20 + this.dbi.INBOUND_TIMESTAMP_PRECISION
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
	// if (this.PIPELINE_STATE.committed === 0) console.log('JOM',row)
    return `${this.rowSeperator}${JSON.stringify(row)}`
  }
  
  _processRow(row) {
    // Be very careful about adding unecessary code here. This is executed once for each row processed by YADAMU. Keep it as lean as possible.
	this.rowCount++
	this.checkColumnCount(row)

	this.rowTransformation(row)

    this.push(this.formatRow(row));
	this.rowSeperator = ','
    this.PIPELINE_STATE.committed++;
    if ((this.FEEDBACK_INTERVAL > 0) && ((this.PIPELINE_STATE.committed % this.FEEDBACK_INTERVAL) === 0)) {
      this.LOGGER.info([`${this.tableName}`,this.dbi.OUTPUT_FORMAT],`Rows Written: ${this.PIPELINE_STATE.committed}.`);
    }
  }
  
  async doTransform(messageType,obj) {
	 
    // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),messageType,this.PIPELINE_STATE.received,this.writableLength,this.writableHighWaterMark],'doTransform()')
	
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
        // this.LOGGER.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType,this.PIPELINE_STATE.displayName,this.rowCount],`${YadamuConstants.END_OF_DATA}`)  
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
	  
    // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.skipTable,this.dbi.TRANSACTION_IN_PROGRESS,this.writableEnded,this.writableFinished,this.destroyed,this.hasPendingRows(),this.PIPELINE_STATE.received,this.PIPELINE_STATE.committed,this.PIPELINE_STATE.written,this.PIPELINE_STATE.cached],`JSONOutputManager.processPendingRows(${this.hasPendingRows()})`)
	this.PIPELINE_STATE[DBIConstants.TRANSFORMATION_STREAM_ID].endTime = performance.now()
	this.finalizeTable()
  }
  
  async doDestroy(err) {
    // Workaround for unexpected "[ERR_STREAM_DESTROYED]: Cannot call pipe after a stream was destroyed" exceptions	 
    this.STREAM_STATE.endTime = performance.now()
	this.PIPELINE_STATE.insertMode = this.dbi.OUTPUT_FORMAT

    if (this.writableEnded && this.writableFinished && err?.code === 'ERR_STREAM_DESTROYED') {
      // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),this.PIPELINE_STATE.received,this.PIPELINE_STATE.cached,this.PIPELINE_STATE.written,this.PIPELINE_STATE.skipped,this.PIPELINE_STATE.lost,this.writableEnded,this.writableFinished,err.code],`JSON_Writer_destroy(): Swallowed error "${err.message}".`)
      err = undefined
	}
	
	// Defer performance reporting to here... Ensures parser 'finish' event has occurred.
	await super.doDestroy(err)
  }
  

}

export {JSONOutputManager as default }