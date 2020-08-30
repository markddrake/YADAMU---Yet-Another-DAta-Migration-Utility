"use strict" 

const { Transform } = require('stream');
const { performance } = require('perf_hooks');

const YadamuLibrary = require('../../common/yadamuLibrary.js');

class EventStream extends Transform {
  
  get INPUT_TIMINGS()                 { return this._INPUT_TIMINGS }

  constructor(yadamu,inputTimings) {

    super({objectMode: true });  
	this.yadamu = yadamu
    this.yadamuLogger = this.yadamu.LOGGER
	this.writerComplete = true
	this.rowsRead = 0
	this._INPUT_TIMINGS = inputTimings
		
  }

  pipe(outputStream,options) {
	// Cache the target outputStream
  	// this.yadamuLogger.trace([this.constructor.name,outputStream.constructor.name,outputStream.tableName],'Attaching pipe')
	this.INPUT_TIMINGS.pipeStartTime = performance.now();
	this.outputStream = outputStream
	if (this.dbWriter === undefined) {
	  this.dbWriter = outputStream;
	}
	return super.pipe(outputStream,options);
  } 
  
  createWriter(tableName) {
    const writer = this.dbWriter.dbi.getOutputStream(tableName,this.dbWriter.ddlComplete)
    // Propigate Error event Handlers from dbWriter to writer
    this.dbWriter.listeners('error').forEach((f) => {writer.on('error',f)});
	return writer
  }
  
  async createTransformations(tableName) {

    // this.yadamuLogger.trace([this.constructor.name,tableName],'createTransformations()')
	  
	const tableMetadata = this.metadata[tableName]
	this.transformations = tableMetadata.dataTypes.map((targetDataType,idx) => {

      const dataType = YadamuLibrary.decomposeDataType(targetDataType);

	  if (YadamuLibrary.isBinaryDataType(dataType.type)) {
        return (row,idx) =>  {
  		  row[idx] = Buffer.from(row[idx],'hex')
		}
      }

	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.spatialFormat.endsWith('WKB')) {
            return (row,idx)  => {
  		      row[idx] = Buffer.from(row[idx],'hex')
			}
          }
		  return null;
		 default:
		   return null
      }
    }) 
	
	// Use a dummy rowTransformation function if there are no transformations required.

	this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }	  
	
  }
  
  async _transform (data,encoding,callback)  {
	let messageType
	try {
	  messageType = Object.keys(data)[0]
	  // this.yadamuLogger.trace([this.constructor.name,'_transform()'],`${messageType}`)
	  switch (messageType) {
	    case 'data':
	      this.rowsRead++
	  	  this.rowTransformation(data.data)
	  	  const state = this.push(data);
 	      break;
        case 'systemInformation' :
          this.push(data)
	  	  this.spatialFormat = data.systemInformation.spatialFormat
	  	  this.yadamu.REJECTION_MANAGER.setSystemInformation(data.systemInformation)
	  	  this.yadamu.WARNING_MANAGER.setSystemInformation(data.systemInformation)
 	      break;
        case 'metadata' :
          this.push(data)
	  	  this.unpipe();
	      this.metadata = data.metadata
	  	  this.yadamu.REJECTION_MANAGER.setMetadata(data.metadata)
	  	  this.yadamu.WARNING_MANAGER.setMetadata(data.metadata)
	  	  // Wait for DDL Complete and then release the DBWriter by invoking the deferredCallback
		  if ((this.yadamu.MODE === 'DDL_ONLY') || (YadamuLibrary.isEmpty(this.metadata))) {
    		await this.dbWriter.ddlComplete
	        this.pipe(this.dbWriter);
	      }
	      break;
        case 'table':
	  	  // Attach new Writer - Couldnot get this work with 'drain' for some reason
		  this.unpipe(this.outputStream);
	  	  const writer = await this.createWriter(data.table)
	  	  this.pipe(writer) 
	  	  this.push(data)
   	      this.INPUT_TIMINGS.parserStartTime = performance.now();
	      this.rowsRead = 0;
	  	  this.createTransformations(data.table)
	      break;
        case 'eod':
		  // Simulate an 'end' condition and report results for the current table.
		  const outputStream = this.outputStream
	      const allDataReceived = new Promise((resolve,reject) => {
	        outputStream.once('allDataReceived',() => {
	          // this.yadamuLogger.trace([this.constructor.name,outputStream.constructor.name,outputStream.tableName,'ON'],'allDataReceived')
			  resolve()
	  	    })
	      })
          this.push(data);

          // this.yadamuLogger.trace([this.constructor.name,outputStream.constructor.name,outputStream.tableName],'Waiting for Drain')
	  	  await allDataReceived;
  		  // this.yadamuLogger.trace([this.constructor.name,outputStream.constructor.name,outputStream.tableName],'Drained')

          // Once the writer acknowledges receipt of all data it can be closed.
		  const writerComplete = new Promise((resolve,reject) => {
            outputStream.on('close',() => {
		      // Get the Reader Start and End Time from the EOD message.
              this.INPUT_TIMINGS.rowsRead = this.rowsRead;
			  this.INPUT_TIMINGS.readerStartTime = data.eod.startTime;
			  this.INPUT_TIMINGS.readerEndTime = data.eod.endTime;
			  this.INPUT_TIMINGS.parserEndTime = performance.now();
			  outputStream.reportPerformance(this.INPUT_TIMINGS);
      		  // this.yadamuLogger.trace([this.constructor.name,outputStream.constructor.name,outputStream.tableName],'Closed')		
			  resolve()
			})
          })
	  	  outputStream.end(null,null,() => {outputStream.destroy()})
          // Ensure all records from current table have been written and committed before starting next table. This prevents transaction logic from getting out of Sync.
		  await writerComplete
		  break;
	    case 'eof':
		  this.pipe(this.dbWriter); 
	  	  this.dbWriter.deferredCallback()
	      this.push(data);
	  	  break;
	    default:
         this.push(data);
	  }
      callback();	
    } catch(e) { 
	  this.yadamuLogger.handleException(['FILE','EVENT STREAM',`_TRANSFORM(${messageType})`],e);
      this.destroy(e)
	}
  };

}

module.exports = EventStream;
