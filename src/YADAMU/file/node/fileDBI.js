"use strict" 

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
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

class TableSwitcher extends PassThrough {

  constructor() {
	super()
  }
  
  pipe(os,options) {
    options = options || {}
	options.end = false;
	return super.pipe(os,options)
  }
  
  _transform(data,enc,callback) {
    this.push(data)
    callback()
  }
 
}

class FirstTableSwitcher extends TableSwitcher {

  constructor(exportFileHeader) {
	super()
    this.push(exportFileHeader) 
  }
 
}

class EndExportOperation extends Readable {

  constructor(exportFileFooter) {
	super()
    this.push(exportFileFooter) 
  }

  _read() {
   this.push(null)
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

  get DATABASE_KEY()           { return FileDBI.DATABASE_KEY };
  get DATABASE_VENDOR()        { return FileDBI.DATABASE_VENDOR };
  
  get COMPRESSED_FILE()        { return this.yadamu.COMPRESSION !== 'NONE' }
  get ENCRYPTED_FILE()         { return this.yadamu.ENCRYPTION }
  
  set PIPELINE_ENTRY_POINT(v)  { this._PIPELINE_ENTRY_POINT = v }
  get PIPELINE_ENTRY_POINT()   { return this._PIPELINE_ENTRY_POINT }
  
  set EXPORT_FILE_HEADER(v)    { this._EXPORT_FILE_HEADER = v }
  get EXPORT_FILE_HEADER()     { return this._EXPORT_FILE_HEADER }
  
  set INITIALIZATION_VECTOR(v) { this._INITIALIZATION_VECTOR =  v }
  get INITIALIZATION_VECTOR()  { return new Uint8Array(16).fill(1); /* this._INITIALIZATION_VECTOR */}
  
  
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
	return Object.assign(
	  super.getSystemInformation()
	, {}
    )
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
    this.spatialFormat = this.parameters.SPATIAL_FORMAT || super.SPATIAL_FORMAT
	this.exportFilePath = this.exportFilePath || this.parameters.FILE
	this.exportFilePath = this.COMPRESSED_FILE ? `${this.exportFilePath}.gz` : this.exportFilePath
	this.exportFilePath =  path.resolve(this.exportFilePath)
  }

  createInputStream() {
    return new Promise((resolve,reject) => {
      this.inputStream = fs.createReadStream(this.exportFilePath);
	  const stack = new Error().stack
      this.inputStream.on('open',() => {resolve()}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(err,stack,this.exportFilePath) : new FileError(err,stack,this.exportFilePath) )})
    })
  }
	 
  async initializeExport() {
	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	super.initializeExport();
	await this.createInputStream()
  }

  async finalizeExport() {
 	// this.yadamuLogger.trace([this.constructor.name,],'finalizeExport()')
	this.closeInputStream()
  }
  
  async createInitializationVector() {

	return await new Promise((resolve,reject) => {
      crypto.randomFill(new Uint8Array(16), (err, iv) => {
		if (err) reject(err)
	    resolve(iv);
      })
	})	    
  } 
  
  createOutputStream() {
    return new Promise((resolve,reject) => {
      this.outputStream = fs.createWriteStream(this.exportFilePath,{flags :"w"})
	  const stack = new Error().stack
      this.outputStream.on('open',() => {resolve()}).on('error',(err) => {reject(err.code === 'ENOENT' ? new DirectoryNotFound(err,stack,this.exportFilePath) : new FileError(err,stack,this.exportFilePath) )})
	})
  }

  getFileOutputStream(tableName) {
    return this.outputStream
  }

  createOutputStreams() {

    this.outputStreams = []
	
    if (this.COMPRESSED_FILE) {
      this.outputStreams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGzip() : createDeflate())
    }
	
    if (this.ENCRYPTED_FILE) {
  	  console.log('Cipher',this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR);
	  const cipherStream = crypto.createCipheriv(this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  this.outputStreams.push(cipherStream)
	}
	
	this.outputStreams.push(this.getFileOutputStream())
  }
  
  async initializeImport() {
	// For FileDBI Import is Writing data to the file system.
	
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
	
	super.initializeImport()
	await this.createOutputStream()
	if (this.ENCRYPTED_FILE) {
      this.INITIALIZATION_VECTOR = await this.createInitializationVector();
    }
	this.createOutputStreams()
	this.PIPELINE_ENTRY_POINT = this.outputStreams[0]
    this.yadamuLogger.info([this.DATABASE_VENDOR],`Writing data to "${this.exportFilePath}".`)
  }
  
  async initializeData() {
  
	// Set up the pipeline and write the system information, ddl and metadata sections to the pipe...
    // this.yadamuLogger.trace([this.constructor.name],`initializeData()`)

    // Remove the source structure from each metadata object prior to serializing it. Put it back after the serialization has been completed.

    const sourceInfo = {}
    Object.keys(this.metadata).forEach((key) => {if (this.metadata[key].source) sourceInfo[key] = this.metadata[key].source; delete this.metadata[key].source})
    let exportFileHeader = `{"systemInformation":${JSON.stringify(this.systemInformation)}${this.ddl ? `,"ddl":${JSON.stringify(this.ddl)}` : ''},"metadata":${JSON.stringify(this.metadata)}`
	Object.keys(sourceInfo).forEach((key) => {this.metadata[key].source = sourceInfo[key]})

    if ((this.MODE === 'DDL_ONLY') || (YadamuLibrary.isEmpty(this.metadata))) {
	  const exportFileContents = `${exportFileHeader}}`
      const finalize = new EndExportOperation(exportFileContents)
	  this.outputStreams.unshift(finalize)
	  await new Promise((resolve,reject) => {
        pipeline(this.outputStreams,(err) => {
		  if (err) reject(err)
	      resolve()
        })
	 
	 })
	}
    else {
	  exportFileHeader = `${exportFileHeader},"data":{` 
	} 
	 
	this.EXPORT_FILE_HEADER = exportFileHeader
	
  }
    
  async finalizeData() {
    
	// this.yadamuLogger.trace([this.constructor.name],`finalizeData(${YadamuLibrary.isEmpty(this.metadata)})`)
	
	if (!YadamuLibrary.isEmpty(this.metadata)) {
      const finalize = new EndExportOperation('}}')
	  this.outputStreams.unshift(finalize)
	  await new Promise((resolve,reject) => {
        pipeline(this.outputStreams,(err) => {
		  if (err) reject(err)
	      resolve()
        })
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
	, _SPATIAL_FORMAT   : this.systemInformation.typeMappings.spatialFormat 
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
	
	if (this.ENCRYPTED_FILE) {
  	  console.log('Decipher',this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR);
	  const decipherStream = crypto.createDecipheriv(this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  streams.push(decipherStream);
	}

	if (this.COMPRESSED_FILE) {
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
	  this.INPUT_METRICS.parserError =
	  err
	  this.INPUT_METRICS.failed = true
    }).on('end',() => {
      this.INPUT_METRICS.parserEndTime = performance.now()
    })
	streams.push(eventStream)

    // console.log(streams.map((s) => { return s.constructor.name }).join(' ==> '))
	return streams;
  }

  getOutputStream(tableName,ddlComplete) {
    // Override parent method to allow output stream to be passed to worker
    // this.yadamuLogger.trace([this.constructor.name],`getOutputStream(${tableName},${this.firstTable})`)
	const os =  new JSONWriter(this,tableName,ddlComplete,this.firstTable,this.status,this.yadamuLogger)
	return os
  }
  
  getOutputStreams(tableName) {

    const outputStreams = [...this.outputStreams]
	
    // The TableSwitcher is used to prevent 'end' events propegating to the output stream
    const tableSwitcher = this.firstTable ? new FirstTableSwitcher(this.EXPORT_FILE_HEADER) : new TableSwitcher() 
    outputStreams.unshift(tableSwitcher)
    
    // Create a JSON Writer
    const jsonWriter = this.getOutputStream(tableName,undefined)
    outputStreams.unshift(jsonWriter)
	this.firstTable = false;
	 
    // console.log(outputStreams.map((s) => { return s.constructor.name }).join(' ==> '))
    return outputStreams	
  }
  
  async createCloneStream(options) {
	await this.initialize()
	const streams = []
	await this.createInputStream();
	streams.push(this.inputStream)
	
	if (options.encryptedInput) {
  	  console.log('Decipher',this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR);
	  const decipherStream = crypto.createDecipheriv(this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  streams.push(decipherStream);
	}
	
	if (options.compressedInput) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGunzip() : createInflate())
	}
      
	if (options.compressedOutput) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGzip() : createDeflate())
	}
	
	if (options.encryptedOutput) {
  	  console.log('Cipher',this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR);
	  const cipherStream = crypto.createCipheriv(this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  streams.push(cipherStream)
	}

    const outputFilePath = path.resolve(options.filename);
	const inputFilePath = this.exportFilePath;
    this.exportFilePath = outputFilePath
	await this.createOutputStream();
	streams.push(this.outputStream)
	
    // console.log(`"${inputFilePath}" ==> ${streams.map((s) => { return s.constructor.name }).join(' ==> ')} ==> "${outputFilePath}".`)
	return streams;
  }
    
  getDatabaseConnection() { /* OVERRIDE */ }
  
  closeConnection() { /* OVERRIDE */ }
 
  
}

module.exports = FileDBI
