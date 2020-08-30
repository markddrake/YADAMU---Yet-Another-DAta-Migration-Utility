"use strict"

const Transform = require('stream').Transform
const { performance } = require('perf_hooks');

const pipeline = require('stream').pipeline;

class TableManager extends Transform {
	
  get INPUT_TIMINGS()                 { return this._INPUT_TIMINGS }
    
  constructor(readerDBI,writerDBI,schemaInfo,tableIdx,yadamuLogger,currentPipeline,inputTimings) {
    super({objectMode: false})
	this.schemaInfo = schemaInfo
	this.readerDBI = readerDBI
	this.writerDBI = writerDBI
	this._INPUT_TIMINGS = inputTimings;
	this.yadamuLogger = yadamuLogger;
	
	// All streams must close for the pipeline to complete and resolve.
	// When a table finishes create the input streams required to process the next table. 
    // If there are not more tables use the stream that geneerates the '}}' characters required to ensure a valid JSON document.
	// Unpipe the current set of input streams from the existing output stream. 
	// Explicity destroy each unpiped stream to free any resources they have consumed.
	// Create a new pipeline operation that pipes the next set of input streams into the existing output stream.
	
    this.on('end', async () => {
      const nexttStep = (tableIdx < this.schemaInfo.length) ? await this.constructPipline(tableIdx) : [writerDBI.END_EXPORT_FILE]
      this.INPUT_TIMINGS.pipeEndTime = performance.now();
      this.unpipe()
	  // Manually clean up the previous pipeline since it never completely ended. Prevents excessive memory usage..
	  // Object.values(this._events).forEach((e) => {Array.isArray(e) ? e.forEach((f) => {console.log(f.toString())}) : console.log(e.toString())})
	  currentPipeline.forEach((s) => {s.destroy()})
	  this.destroy();
      nexttStep.push(this.downStream)
      // this.yadamuLogger11.trace([this.constructor.name,'PIPELINE',this.readerDBI.DATABASE_VENDOR,this.writerDBI.DATABASE_VENDOR,tableIdx,this.downStream.writableEnded],`${nexttStep.map((proc) => { return proc.constructor.name }).join(' => ')}`)
      pipeline(nexttStep,(err,data) => {
		if (err) throw err
      })
    })
  }
  
  pipe(os,options) {
	this.pipeStartTime = performance.now();
    options = options || {}
	options.end = false;
    this.downStream = os
	return super.pipe(os,options);
  }
  
  async constructPipline(idx) {
	  
	 const inputTimings = {
      rowsRead        : 0
    , pipeStartTime   : undefined
    , readerStartTime : undefined
    , readerEndTime   : undefined
	, parserStartTime : undefined
    , parserEndTime   : undefined
    }
	  
	const components = []
    const task = this.readerDBI.generateQueryInformation(this.schemaInfo[idx])
	const inputStreams = await this.readerDBI.getInputStreams(task)
	components.push(...inputStreams);

	const mappedTableName = this.writerDBI.transformTableName(task.TABLE_NAME,this.readerDBI.getInverseTableMappings())
	const outputStream = await this.writerDBI.getOutputStream(task.MAPPED_TABLE_NAME,undefined,idx)
	outputStream.on('end',() => {
	  outputStream.reportPerformance(this.readerDBI.INPUT_TIMINGS)
	});
	components.push(outputStream);
	idx++
	const currentComponents = [...components]
    const tableManager = new TableManager(this.readerDBI,this.writerDBI,this.schemaInfo,idx,currentComponents,this.yadamuLogger,inputTimings)		
	components.push(tableManager);
	return components;
  }

  _transform(data,enc,callback) {
	  this.push(data);
	  callback();
  }
}

module.exports = TableManager;


 

