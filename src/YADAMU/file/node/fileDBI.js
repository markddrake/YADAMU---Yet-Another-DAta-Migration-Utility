"use strict" 
const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const {pipeline, Readable, PassThrough} = require('stream')

/*
**
** Obtain YADAMU_DBI_PARAMETERS and YADAMU_CONFIGURATION directly from YadamuConstants to avoid circular depandancy between FileDBI.js and Yadamu.js. 
** Importing Yadamu into FileDBI sets up a circular dependancy that causes deferred resolution of Yadamu class. This means attempts to refereence
** static GETTER methods result in undefined values.
**

const Yadamu = require('../../common/yadamu.js');

**
*/
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const JSONParser = require('./jsonParser.js');
const EventStream = require('./eventStream.js');
const JSONWriter = require('./jsonWriter.js');
const {FileError, FileNotFound, DirectoryNotFound} = require('./fileException.js');

const { createGzip, createGunzip, createDeflate, createInflate } = require('zlib');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class PipeOnce extends Readable {

  constructor(metadata,endOption) {
	 super();
	 this.metadata = metadata
	 this.endOption = endOption
  	 // this.pause();
  }
  
  pipe(os,options) {
	options = options || {}
	options.end = this.endOption;
	return super.pipe(os,options);
  }  
  
  _read() {
	 this.push(this.metadata)
	 this.metadata = null
  }
  
}

class FileDBI extends YadamuDBI {
 
  /*
  **
  ** !!! For the FileDBI an export operaton involves reading data from the file system and an Import operation involves writing data to the file system !!!
  **
  */

  static get DATABASE_KEY()          { return 'file' };
  static get DATABASE_VENDOR()       { return 'FILE' };

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  {
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return FileDBI.YADAMU_DBI_PARAMETERS
  }

  get DATABASE_KEY()          { return FileDBI.DATABASE_KEY };
  get DATABASE_VENDOR()       { return FileDBI.DATABASE_VENDOR };
  
  get COMPRESSED_OUTPUT()     { return this.yadamu.COMPRESSION !== 'NONE' }
  
  set PIPELINE_ENTRY_POINT(v) { this._PIPELINE_ENTRY_POINT = v }
  get PIPELINE_ENTRY_POINT()  { return this._PIPELINE_ENTRY_POINT }
  
  set START_EXPORT_FILE(v)    { this._START_EXPORT_FILE = new PipeOnce(v,false) }
  get START_EXPORT_FILE()     { return this._START_EXPORT_FILE }
  
  set END_EXPORT_FILE(v)      { this._END_EXPORT_FILE =  new PipeOnce(v,true) }
  get END_EXPORT_FILE()       { return this._END_EXPORT_FILE }
  
  constructor(yadamu,exportFilePath) {
    super(yadamu)
	this.exportFilePath = exportFilePath
    this.outputStream = undefined;
    this.inputStream = undefined;
	this.firstTable = true;
	this.ddl = undefined;
  }

  generateStatementCache() {
	this.statementCache = {}
  }
      
  async executeDDL(ddl) {
	this.ddl = ddl
    return ddl
  }
  
  getConnectionProperties() {
    return {}
  }
  
  exportComplete(message) {
	this.eventManager.exportComplete(message);
  }
  
  closeInputStream() {      
    this.inputStream.close();
  }

  closeOutputStream() {
    this.outputStream.close();
  }
  
  // Override YadamuDBI - Any DDL is considered valid and written to the export file.
 
  isValidDDL() {
    return true;
  }
  
  // Override YadamuDBI
  
  isDatabase() {
    return false;
  }
  
  // Override YadamuDBI

  objectMode() {
     return false;  
  }

  async getMetadata() {
	return []
  }
  
  async getSystemInformation() {
	return {}
  }

  async setSystemInformation(systemInformation) {
	super.setSystemInformation(systemInformation) 
  }
    
  async setMetadata(metadata) {
    // Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)
  }
 
  async releaseConnection() {
  }
 
  async initialize() {
    super.initialize(false);
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
	this.exportFilePath = this.exportFilePath === undefined ? this.parameters.FILE : this.exportFilePath
	this.exportFilePath = this.COMPRESSED_OUTPUT ? `${this.exportFilePath}.gz` : this.exportFilePath
	this.exportFilePath =  path.resolve(this.exportFilePath)
  }

  async initializeExport() {
	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	super.initializeExport();
    await new Promise((resolve,reject) => {
      this.inputStream = fs.createReadStream(this.exportFilePath);
	  const stack = new Error().stack
      this.inputStream.on('open',() => {resolve()}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(err,stack,this.exportFilePath) : new FileError(err,stack,this.exportFilePath) )})
    })
  }

  async finalizeExport() {
 	// this.yadamuLogger.trace([this.constructor.name,],'finalizeExport()')
	this.closeInputStream()
  }
  
  async initializeImport() {
	// For FileDBI Import is Writing data to the file system.
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
	super.initializeImport()
    await new Promise((resolve,reject) => {
      this.outputStream = fs.createWriteStream(this.exportFilePath,{flags :"w"})
	  const stack = new Error().stack
      this.outputStream.on('open',() => {resolve()}).on('error',(err) => {reject(err.code === 'ENOENT' ? new DirectoryNotFound(err,stack,this.exportFilePath) : new FileError(err,stack,this.exportFilePath) )})
	})
    this.yadamuLogger.info([this.DATABASE_VENDOR],`Data written to "${this.exportFilePath}".`)
  }
  
  async initializeData() {
  
	// Set up the pipeline and write the system information, ddl and metadata sections to the pipe...
    // this.yadamuLogger.trace([this.constructor.name],`initializeData()`)

    // Remove the source structure from each metadata object prior to serializing it. Put it back after the serialization has been completed.

    const sourceInfo = {}
    Object.keys(this.metadata).forEach((key) => {if (this.metadata[key].source) sourceInfo[key] = this.metadata[key].source; delete this.metadata[key].source})
    let startJSON = `{"systemInformation":${JSON.stringify(this.systemInformation)}${this.ddl ? `,"ddl":${JSON.stringify(this.ddl)}` : ''},"metadata":${JSON.stringify(this.metadata)}`
	Object.keys(sourceInfo).forEach((key) => {this.metadata[key].source = sourceInfo[key]})

	let endJSON = undefined
    if ((this.MODE === 'DDL_ONLY') || (YadamuLibrary.isEmpty(this.metadata))) {
	  endJSON = '}'
	}
    else {
	  startJSON = `${startJSON},"data":{` 
	  endJSON = '}}'
	} 
	 
	this.START_EXPORT_FILE = startJSON
	this.END_EXPORT_FILE  = endJSON
	 
	const outputStreams = this.getOutputStreams()
	// this.yadamuLogger.trace([this.constructor.name,'EXPORT',this.DATABASE_VENDOR,this.parameters.TO_USER],`${outputStreams.map((proc) => { return proc.constructor.name }).join(' => ')}`)
	this.PIPELINE_ENTRY_POINT = outputStreams[0]
	
	outputStreams.unshift(this.START_EXPORT_FILE)
	// this.yadamuLogger.trace([this.constructor.name,'EXPORT',this.DATABASE_VENDOR,this.parameters.TO_USER],`${outputStreams.map((proc) => { return proc.constructor.name }).join(' => ')}`)
	 
	this.pipelineComplete = new Promise((resolve,reject) => {
	  pipeline(outputStreams,(err) => {
	    if (err) reject(err);
	    resolve()
      })
	})
	 
	if ((this.MODE === 'DDL_ONLY') || (YadamuLibrary.isEmpty(this.metadata)))  {
	  pipeline([this.END_EXPORT_FILE,this.PIPELINE_ENTRY_POINT],(err) => {
	    if (err) throw(err);
	  })
    }
	 
  }
    
  async finalizeImport() {}
    
  async finalize() {
    if (this.inputStream !== undefined) {
      await this.closeInputStream()
    }
  }


  /*
  **
  **  Abort the database connection.
  **
  */

  async abort(e) {

    try {
      if (this.inputStream !== undefined) {
        await this.closeInputStream()
	  }
    } catch (err) {
      this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,'ABORT','InputStream'],err);
    }
	 
    try {
      if (this.outputStream !== undefined) {
        await this.closeOutputStream()
	  }
    } catch (err) {
      this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,'ABORT','OutputStream'],err);
    }
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */
  
      
  async generateStatementCache(schema,executeDDL) {
    this.statementCache = []
	return this.statementCache
  }

  async getDDLOperations() {
    return []
  }
  
  async getSchemaInfo(schema) {
    return []
  }
  
  getTableInfo(tableName) {
	
    if (tableName === null) {
	  // Hack to enable statisticsCollector to use the YadamuWriter interface to collect statistics about the cotnents of a YADAMU export file...
      return {}
    }

    if (this.metadata === undefined) {
      this.yadamuLogger.logInternalError([this.constructor.name,`getTableInfo()`,tableName],`Metadata undefined. Cannot obtain required information.`)
	}

	if (this.metadata[tableName] === undefined) {
      this.yadamuLogger.logInternalError([this.constructor.name,`getTableInfo()`,tableName],`No metadata entry for "${tableName}". Current entries: ${JSON.stringify(Object.keys(this.metadata))}`)
	}

	// ### Need to simplify and standardize DataTypes - Data type mapping for Files.. 
	
	// Include a dummy dataTypes array of the correct length to ensure the column count assertion does not throw
	return { 
	  tableName         : tableName
	, _SPATIAL_FORMAT   : this.systemInformation.spatialFormat
    , columnNames       : [... this.metadata[tableName].columnNames]
    , targetDataTypes   : [... this.metadata[tableName].dataTypes]
    }
  }

  getInputStream() {  
    // Return the inputStream and the transform streams required to process it.
    const stats = fs.statSync(this.exportFilePath)
    const fileSizeInBytes = stats.size
    this.yadamuLogger.info([this.DATABASE_VENDOR],`Processing file "${this.exportFilePath}". Size ${fileSizeInBytes} bytes.`)
	return this.inputStream
  }
  
  getInputStreams() {
	const streams = []
	this.INPUT_METRICS = DBIConstants.NEW_TIMINGS
	this.INPUT_METRICS.DATABASE_VENDOR = this.DATABASE_VENDOR
	const is = this.getInputStream();
	is.once('readable',() => {
	  this.INPUT_METRICS.readerStartTime = performance.now()
	}).on('error',(err) => { 
      this.INPUT_METRICS.readerEndTime = performance.now()
	  this.INPUT_METRICS.readerError = err
	  this.INPUT_METRICS.failed = true
    }).on('end',() => {
      this.INPUT_METRICS.readerEndTime = performance.now()
    })
	streams.push(is)
	
	if (this.COMPRESSED_OUTPUT) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGunzip() : createInflate())
	}
	
	const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, this.exportFilePath)
	jsonParser.once('readable',() => {
	  this.INPUT_METRICS.parserStartTime = performance.now()
	}).on('error',(err) => { 
      this.INPUT_METRICS.parserEndTime = performance.now()
	  this.INPUT_METRICS.parserError = err
	  this.INPUT_METRICS.failed = true
    })
	streams.push(jsonParser);
	
	const eventStream = new EventStream(this.yadamu)
	eventStream.on('error',(err) => { 
      this.INPUT_METRICS.parserEndTime = performance.now()
	  this.INPUT_METRICS.parserError = err
	  this.INPUT_METRICS.failed = true
    }).on('end',() => {
      this.INPUT_METRICS.parserEndTime = performance.now()
    })
	streams.push(eventStream)
	return streams;
  }
    
  getOutputStream(tableName,ddlComplete) {
    // Override parent method to allow output stream to be passed to worker
    // this.yadamuLogger.trace([this.constructor.name],`getOutputStream(${tableName},${this.firstTable})`)
	const os =  new JSONWriter(this,tableName,ddlComplete,this.firstTable,this.status,this.yadamuLogger)
	this.firstTable = false;
    return os
  }
  
  getFileOutputStream(tableName) {
    return this.outputStream
  }

  getOutputStreams(tableName) {
    const streams = []
	if (this.COMPRESSED_OUTPUT) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGzip() : createDeflate())
    }
	streams.push(this.getFileOutputStream(tableName))
	
	return streams;
  }
    
  getDatabaseConnection() { /* OVERRIDE */ }
  
  closeConnection() { /* OVERRIDE */ }
 
  
}

module.exports = FileDBI
