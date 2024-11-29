
import { 
  performance 
}                          from 'perf_hooks';

import { 
  Readable, 
  Transform
}                          from 'stream';

import { 
  pipeline 
}                          from 'stream/promises'

import { 
  compose
}                          from 'stream'

import {CommandLineError}  from '../../core/yadamuException.js';
import YadamuLibrary       from '../../lib/yadamuLibrary.js';
import YadamuConstants     from '../../lib/yadamuConstants.js';
import NullWritable        from '../../util/nullWritable.js';

import DBIConstants        from '../base/dbiConstants.js';
import YadamuDataTypes     from '../base/yadamuDataTypes.js';

class StreamSwitcher extends Transform {
  
  get LOGGER()               { return this._LOGGER }
  set LOGGER(v)              { this._LOGGER = v }
  get DEBUGGER()             { return this._DEBUGGER }
  set DEBUGGER(v)            { this._DEBUGGER = v }

  get UPSTREAM_STATE()       { return this._UPSTREAM_STATE }
  set UPSTREAM_STATE(v)      { this._UPSTREAM_STATE =  v }

  get PIPELINE_STATE()       { return this._PIPELINE_STATE }
  set PIPELINE_STATE(v)      { this._PIPELINE_STATE =  v }

  get STREAM_STATE()         { return this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] }
  set STREAM_STATE(v)        { this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] = v }
	  
  get TABLE_FILTER()                  { return this.dbWriter.dbi.TABLE_FILTER }
  
  get INBOUND_SPATIAL_FORMAT()        { return this.systemInformation.driverSettings.spatialFormat }
  get INBOUND_CIRCLE_FORMAT()         { return this.systemInformation.driverSettings.circleFormat }

  /*
  **
  ** This class ensures that the correct components are attached up to the pipeline before pushing messages down the pipeline 
  ** When the metadata message is recieved it removes the DBWriter from the pipeline.
  ** When a 'table' message is recieved it attaches a database writer which will recieve the 'data' messages for a particular table
  ** When a 'eod' message is received it remove the current database writer.
  ** When the 'eof' message is received it reattaches the original DBWriter, allowing the pipeline to complete.
  **
  */
 
  constructor(dbi,yadamu,pipelineState) {
    super({objectMode: true });  
	this.dbi = dbi
	this.yadamu = yadamu
    this.PIPELINE_STATE = pipelineState
	this.LOGGER = this.yadamu.LOGGER
	this.rowsRead = 0	
  }


  pipe(outputStream,options) {
	// Cache the down stream participant
  	// this.LOGGER.trace([this.constructor.name,outputStream.constructor.name,outputStream.tableName],'Attaching pipe')
	this.dbWriter = this.dbWriter || outputStream
	return super.pipe(outputStream,options);
  } 
    
  generateTransformations(tableName) {

    // this.LOGGER.trace([this.constructor.name,tableName],'generateTransformations()')
	
	const tableMetadata = this.metadata[tableName]
	return tableMetadata.dataTypes.map((dataType,idx) => {	  
	
      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType);
	 
	  switch (true) {
        case (dataTypeDefinition.type.toUpperCase() === "CIRCLE"):
   		  if (this.INBOUND_CIRCLE_FORMAT === 'CIRCLE') {
			// console.log(tableMetadata.columnNames[idx],dataType,'==>','CIRCLE')
            return null;
		  }
		  // Deliberate FALL through to SPATIAL 
	    case (YadamuDataTypes.isSpatial(dataTypeDefinition.type)):
          // console.log(tableMetadata.columnNames[idx],dataType,'==>','isSpatial')
          if (this.INBOUND_SPATIAL_FORMAT.endsWith('WKB')) {
            return (row,idx)  => {
  		      row[idx] = Buffer.from(row[idx],'hex')
			}
          }
		  return null
	    case (YadamuDataTypes.isBoolean(dataTypeDefinition.type,tableMetadata.sizeConstraints[idx][0],tableMetadata.vendor)):
          // console.log(tableMetadata.columnNames[idx],dataType,'==>','isBoolean')
		  return null;
	    case (YadamuDataTypes.isBinary(dataTypeDefinition.type)):
          // console.log(tableMetadata.columnNames[idx],dataType,'==>','isBinary')
  		  return (row,idx) =>  {
			row[idx] = Buffer.from(row[idx],'hex')
		  }
	    case (YadamuDataTypes.isFloatingPoint(dataTypeDefinition.type)):
  		  // console.log(tableMetadata.columnNames[idx],dataType,'==>','isFloatingPoint')
  		  return (row, idx) => {
		    switch (row[idx]) {
		      case "NaN":
			    row[idx] = NaN
			    break;
			  case "Infinity":
				row[idx] = Number.POSITIVE_INFINITY
				break;
		      case "-Infinity":
				row[idx] = Number.NEGATIVE_INFINITY
				break;
		      default:
			}
          }			
		default:
  		  // console.log(tableMetadata.columnNames[idx],dataType,'==>','isDefault')
		  return null
      }
    }) 
		
  }
  
  setTransformations(tableName) {

    const transformations = this.generateTransformations(tableName)

	// Use a dummy rowTransformation function if there are no transformations required.

	this.rowTransformation = transformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
      transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }	
  }
  
  convertSizeConstraints(sizeConstraints) {
	return Array.isArray(sizeConstraints[0]) ? sizeConstraints : sizeConstraints.map((sizeConstraint) => {
	  switch (sizeConstraint) {
		case '':
		case '-1':
		case null:
		case undefined:
		  return []
	    default:
		  const sizeComponents = sizeConstraint.split(',')
		  return (sizeComponents.length === 1) ? [parseInt(sizeComponents[0])] : [parseInt(sizeComponents[0]),parseInt(sizeComponents[1])]
	  }
	})
  }  
  
  filterTables(data) {
	
	// Restrict operations to the list of tables specified.
	// Order operations according to the order in which the tables were specified

    if (this.TABLE_FILTER.length > 0) {
	  
	  // Check table names are valid.
	  // For each name in the Table Filter check there is a corresponding entry in the schemaInfoormation collection
	  
	  const tableNames = Object.keys(data.metadata)

	  const invalidTableNames = this.TABLE_FILTER.filter((tableName) => {
		 // Return true if the table does not have an entry in the schemaInformstion collection
		 return !tableNames.includes(tableName)
	  })
	  
	  if (invalidTableNames.length > 0) {
        throw new CommandLineError(`Could not resolve the following table names : "${invalidTableNames}".`)
      }
	
      this.LOGGER.info(['FILE'],`Operations restricted to the following tables: ${JSON.stringify(this.TABLE_FILTER)}.`)
	  	 
      const metadata = {}
	  this.TABLE_FILTER.forEach((table) => {
         metadata[table] = data.metadata[table]
	  })
	  Object.values(metadata).forEach((tableMetadata) => {
		 tableMetadata.sizeConstraints = this.convertSizeConstraints(tableMetadata.sizeConstraints)
	  })
	  return {metadata: metadata}
	}
    Object.values(data.metadata).forEach((tableMetadata) => {
	  tableMetadata.sizeConstraints = this.convertSizeConstraints(tableMetadata.sizeConstraints)
	})
	return data
  }

  async createTableWriter(tableName) {

    // Current State : Pipe Start Time is the time the first row was recieved by the JSON Parser class.
	
	const startState = { startTime : this.PIPELINE_STATE.startTime }
	this.PIPELINE_STATE = Object.assign(this.PIPELINE_STATE, DBIConstants.PIPELINE_STATE, startState)
	const streams = await this.dbWriter.dbi.getOutputStreams(tableName,this.PIPELINE_STATE)
	
	// tableDataWittten needs to listen on the YadamuOutputManager, not the duplex. If the event is put on the duplex generated by compose it is not raised.
	
	const outputManager = streams[0]

	this.tableDataWritten = new Promise((resolve,reject) => {
  	  outputManager.once(YadamuConstants.END_OF_DATA,() => {
	    resolve(performance.now())
	  })
    })
	
	const tableWriter = compose(...streams)
    this.dbWriter.listeners('error').forEach((f) => {tableWriter.on('error',f)});	
    // this.LOGGER.trace([this.constructor.name,'createTableWriter()'],`Waiting on Cache Loaded. [${this.dbWriter.dbi.cacheLoaded}]`);
	await this.dbWriter.dbi.cacheLoaded
	// this.LOGGER.trace([this.constructor.name,'createTableWriter()'],`Cache Loaded. [${this.dbWriter.dbi.cacheLoaded}]`);
	this.setTransformations(tableName)
	return tableWriter
  }
  
  writeDataToPipe(data) {
    this.rowsRead++
    this.rowTransformation(data.data)
	const state = this.push(data);
  }
  
  skipData(data) {
  }
      
  async doTransform(messageType,obj) {
	  // this.LOGGER.trace([this.constructor.name,'doTransform()'],`${messageType}`)
	  switch (messageType) {
	    case 'data':
		  this.processRow(obj)
 	      break;
        case 'systemInformation' :
          this.push(obj)
		  this.systemInformation = obj.systemInformation
	  	  this.yadamu.REJECTION_MANAGER.setSystemInformation(obj.systemInformation)
	  	  this.yadamu.WARNING_MANAGER.setSystemInformation(obj.systemInformation)
 	      break;
		  
        case 'metadata' :		
          obj = this.filterTables(obj)
          this.push(obj)
		  this.metadata = obj.metadata
	  	  this.yadamu.REJECTION_MANAGER.setMetadata(this.metadata)
	  	  this.yadamu.WARNING_MANAGER.setMetadata(this.metadata)
		  if ((this.yadamu.MODE === 'DDL_ONLY') || (YadamuLibrary.isEmpty(this.metadata))) {
  	  	    // DDL_ONLY operation or empty schema: Wait for DDL Complete.
			// this.LOGGER.trace([this.constructor.name,'doTransform()',messageType],`Waiting on DLL Complete. [${this.dbWriter.dbi.ddlComplete}]`);
    		await this.dbWriter.dbi.ddlComplete
			// this.LOGGER.trace([this.constructor.name,'doTransform()',messageType],`DDL Complete. [${this.dbWriter.dbi.ddlComplete}]`);
		  }
		  else {
			// There are one or more tables to process. 
			// this.LOGGER.trace([this.constructor.name,'doTransform()',messageType],`unpipe(${this.dbWriter.constructor.name})`);
	  	    this.unpipe(this.dbWriter);
	      }
	      break;
        case 'table':
          this.rowsRead = 0;
          if (this.metadata.hasOwnProperty(obj.table)) {
            // Table is in the list of tables to be processed.
			this.skipTable = false;
  		    this.processRow = this.writeDataToPipe
		    this.tableWriter = await this.createTableWriter(obj.table)
            // this.LOGGER.trace([this.constructor.name,'doTransform()'],`pipe(${this.tableWriter.constructor.name})`);
			this.PIPELINE_STATE.startTime = performance.now()
			// The psuedo pipeline for the table starts when the tableWriter is inserted into the pipeline
			this.pipe(this.tableWriter)
	  	    this.push(obj)
	      }
		  else {
			// this.LOGGER.trace([this.constructor.name,'doTransform()',messageType],`Skipping obj`);
     		this.skipTable = true;
			this.processRow = this.skipData
		  }
	      break;
        case 'eod':
		  if (!this.skipTable) {
		    // The parser sends an 'eod' message contains the parser stats for each table. 
			this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID].startTime = obj.eod.startTime
 			this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID].endTime = obj.eod.endTime
			this.PIPELINE_STATE.parsed = obj.eod.parsed
			// Table Level startTime and endTime and rows read are not available for input stream, since it is a readable stream over a file and is simply feeding chunks of text to the Parser.
			// Use the Parser startTime, endTime and rows parsed values as proxies for the input stream.
			this.PIPELINE_STATE[DBIConstants.INPUT_STREAM_ID].startTime = obj.eod.startTime
 			this.PIPELINE_STATE[DBIConstants.INPUT_STREAM_ID].endTime = obj.eod.endTime
			this.PIPELINE_STATE.read = obj.eod.parsed
			this.push(obj);
			// Wait for the target stream to get all the data If we unpipe to early data is lost.
			this.PIPELINE_STATE[DBIConstants.TRANSFORMATION_STREAM_ID].endTime = await this.tableDataWritten
			// this.LOGGER.trace([this.constructor.name,'doTransform()',messageType],`unpipe(${this.tableWriter.constructor.name})`);
			this.unpipe(this.tableWriter)
			// Pipe a null object to terminate the tableWriter and flush all pending data.
			const is = new Readable.from([])
			await pipeline(is,this.tableWriter)
			this.PIPELINE_STATE.endTime = performance.now()
			this.dbi.reportPipelineStatus(this.PIPELINE_STATE)
		  }
		  break;
	    case 'eof':
          // this.LOGGER.trace([this.constructor.name,'doTransform()'messageType],`pipe(${this.dbWriter.constructor.name})`);
		  this.pipe(this.dbWriter); 
	      this.push(obj);
      	  break;
	    default:
          this.push(obj);
	  }
  }
  
  
  _transform (obj,encoding,callback)  {
	const messageType = Object.keys(obj)[0]
	this.doTransform(messageType,obj).then(() => { 
	  callback()
	}).catch((e) => { 
	  this.LOGGER.handleException(['FILE','EVENT STREAM',`_TRANSFORM(${messageType})`,this?.dbWriter.dbi.ON_ERROR],e);
      callback(e)
	})
  };

}

export { StreamSwitcher as default }
  
