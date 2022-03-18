"use strict" 

import { Readable, Transform} from 'stream';
import { pipeline } from 'stream/promises';
import {compose} from 'stream';

import { performance } from 'perf_hooks';

import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuConstants from '../../lib/yadamuConstants.js';
import DBIConstants from '../base/dbiConstants.js';
import {CommandLineError} from '../../core/yadamuException.js';
import NullWritable from '../../util/nullWritable.js';

class StreamSwitcher extends Transform {
  
  get TABLE_FILTER()                  { return this.dbWriter.dbi.TABLE_FILTER }

  /*
  **
  ** This class ensures that the correct components are attached up to the pipeline before pushing messages down the pipeline 
  ** When the metadata message is recieved it removes the DBWriter from the pipeline.
  ** When a 'table' message is recieved it attaches a database writer which will recieve the 'data' messages for a particular table
  ** When a 'eod' message is received it remove the current database writer.
  ** When the 'eof' message is received it reattaches the original DBWriter, allowing the pipeline to complete.
  **
  */
 
  constructor(yadamu,metrics) {
    super({objectMode: true });  
	this.yadamu = yadamu
    this.READER_METRICS = metrics
	this.yadamuLogger = this.yadamu.LOGGER
	this.rowsRead = 0	
  }


  pipe(outputStream,options) {
	// Cache the down stream participant
  	// this.yadamuLogger.trace([this.constructor.name,outputStream.constructor.name,outputStream.tableName],'Attaching pipe')
	this.dbWriter = this.dbWriter || outputStream
	return super.pipe(outputStream,options);
  } 
    
  generateTransformations(tableName) {

    // this.yadamuLogger.trace([this.constructor.name,tableName],'generateTransformations()')
	 
	const tableMetadata = this.metadata[tableName]
	return tableMetadata.dataTypes.map((targetDataType,idx) => {	  
	
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);

	  if (YadamuLibrary.isBinaryType(dataType.type)) {
        return (row,idx) =>  {
  		  row[idx] = Buffer.from(row[idx],'hex')
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
        case "LINESTRING":
		case "MULTIPOINT":
        case "MULTILINESTRING":
        case "MULTIPOLYGON":
        case "GEOMETRYCOLLECTION":
        case "GEOMCOLLECTION":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.spatialFormat.endsWith('WKB')) {
            return (row,idx)  => {
  		      row[idx] = Buffer.from(row[idx],'hex')
			}
          }
		  return null;
        case "CIRCLE":
		  if (this.circleFormat === 'CIRCLE') { 
            return null;
		  }
          if (this.spatialFormat.endsWith('WKB')) {
            return (row,idx)  => {
  		      row[idx] = Buffer.from(row[idx],'hex')
			}
          }
		  return null;
		case "REAL":
        case "FLOAT":
		case "DOUBLE":
		case "DOUBLE PRECISION":
		case "BINARY_FLOAT":
		case "BINARY_DOUBLE":
		   return (row, idx) => {
//			 if (!isFinite(row[idx])) {
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
//		     }
		   }			 
   		 default:
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
	
      this.yadamuLogger.info(['FILE'],`Operations restricted to the following tables: ${JSON.stringify(this.TABLE_FILTER)}.`)
	  	 
      const metadata = {}
	  this.TABLE_FILTER.forEach((table) => {
         metadata[table] = data.metadata[table]
	  })
	  return {metadata: metadata}
	}
	return data
  }

  async createTableWriter(tableName) {

    // Reader Metrics Pipe Start Time is the time the first row was recieved by the 1JSON Parser class.

	this.COPY_METRICS                        = DBIConstants.NEW_COPY_METRICS
	this.COPY_METRICS.SOURCE_DATABASE_VENDOR = this.READER_METRICS.SOURCE_DATABASE_VENDOR
	const streams                            = await this.dbWriter.dbi.getOutputStreams(tableName,this.COPY_METRICS)
	
	// tableDataWittten needs to listen on the YadamuOutputManager, not the duplex. If the event is put on the duplex generated by compose it is not raised.
	
	this.outputManager = streams[0]
	this.tableDataWritten = new Promise((resolve,reject) => {
  	  this.outputManager.once(YadamuConstants.END_OF_DATA,() => {
	    resolve()
	  })
    })
	
	const tableWriter = compose(...streams)
    this.dbWriter.listeners('error').forEach((f) => {tableWriter.on('error',f)});	
    // this.yadamuLogger.trace([this.constructor.name,'createTableWriter()'],`Waiting on Cache Loaded. [${this.dbWriter.dbi.cacheLoaded}]`);
	await this.dbWriter.dbi.cacheLoaded
	// this.yadamuLogger.trace([this.constructor.name,'createTableWriter()'],`Cache Loaded. [${this.dbWriter.dbi.cacheLoaded}]`);
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
	  // this.yadamuLogger.trace([this.constructor.name,'doTransform()'],`${messageType}`)
	  switch (messageType) {
	    case 'data':
		  this.processRow(obj)
 	      break;
        case 'systemInformation' :
          this.push(obj)
	  	  this.spatialFormat = obj.systemInformation.typeMappings.spatialFormat
		  this.circleFormat = obj.systemInformation.typeMappings.circleFormat
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
			// this.yadamuLogger.trace([this.constructor.name,'doTransform()',messageType],`Waiting on DLL Complete. [${this.dbWriter.dbi.ddlComplete}]`);
    		await this.dbWriter.dbi.ddlComplete
			// this.yadamuLogger.trace([this.constructor.name,'doTransform()',messageType],`DDL Complete. [${this.dbWriter.dbi.ddlComplete}]`);
		  }
		  else {
			// There are one or more tables to process. 
			// this.yadamuLogger.trace([this.constructor.name,'doTransform()',messageType],`unpipe(${this.dbWriter.constructor.name})`);
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
            // this.yadamuLogger.trace([this.constructor.name,'doTransform()'],`pipe(${this.tableWriter.constructor.name})`);
	  	    this.pipe(this.tableWriter) 
	  	    this.push(obj)
	      }
		  else {
			// this.yadamuLogger.trace([this.constructor.name,'doTransform()',messageType],`Skipping obj`);
     		this.skipTable = true;
			this.processRow = this.skipData
		  }
	      break;
        case 'eod':
		  if (!this.skipTable) {
			// Simulate the reader and parser timings for the current table. Since we are processing a single file containing data for multiple tables the closest values are the current table's start and end times, as recorded by the Parser.
			this.COPY_METRICS.pipeStartTime = obj.eod.startTime
			this.COPY_METRICS.readerStartTime = obj.eod.startTime
			this.COPY_METRICS.readerEndTime = obj.eod.endTime
			this.COPY_METRICS.parserStartTime = obj.eod.startTime
			this.COPY_METRICS.parserEndTime = obj.eod.endTime
			this.COPY_METRICS.read = this.rowsRead
            this.push(obj);
			// Wait for the target stream to get all the data If we unpipe to early data is lost.
			await this.tableDataWritten
			// this.yadamuLogger.trace([this.constructor.name,'doTransform()',messageType],`unpipe(${this.tableWriter.constructor.name})`);
			this.unpipe(this.tableWriter)
			// Pipe a null object to terminate the tableWriter and flush all pending data.
			const is = new Readable.from([])
			await pipeline(is,this.tableWriter)
		  }
		  break;
	    case 'eof':
          // this.yadamuLogger.trace([this.constructor.name,'doTransform()'messageType],`pipe(${this.dbWriter.constructor.name})`);
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
	  this.yadamuLogger.handleException(['FILE','EVENT STREAM',`_TRANSFORM(${messageType})`,this?.dbWriter.dbi.ON_ERROR],e);
      callback(e)
	})
  };

}

export { StreamSwitcher as default }
  
