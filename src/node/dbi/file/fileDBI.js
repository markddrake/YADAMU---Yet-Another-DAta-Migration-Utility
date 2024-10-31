					                  
import fs                             from 'fs';
import fsp                            from 'fs/promises';
import path                           from 'path';
import crypto                         from 'crypto';
					                  
import {                              
  finished,                           
  compose,                            
  Readable,                           
  PassThrough                         
}                                     from 'stream'
import {                              
  pipeline                            
}                                     from 'stream/promises'
import {                              
  createGzip,                         
  createGunzip,                       
  createDeflate,                      
  createInflate                       
}                                     from 'zlib';
					                  
import {                              
  performance                         
}                                     from 'perf_hooks';

/*
**
** Obtain DBI_PARAMETERS and YADAMU_CONFIGURATION directly from YadamuConstants to avoid circular depandancy between FileDBI.js and Yadamu.js. 
** Importing Yadamu into FileDBI sets up a circular dependancy that causes deferred resolution of Yadamu class. This means attempts to refereence
** static GETTER methods result in undefined values.
**

import Yadamu from '../../core/yadamu.js'

**
*/

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    

import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                   

import JSONParser                     from './jsonParser.js'
import StreamSwitcher                 from './streamSwitcher.js'
import JSONOutputManager              from './jsonOutputManager.js'
import FileCompare                    from './fileCompare.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                     from './fileException.js'

class ExportWriter extends Readable {

  constructor(exportFileHeader) {
	super()
    this.push(exportFileHeader) 
  }
  
  _read() {
   this.push(null)
  }
 
 
}

class IVWriter extends PassThrough {

  constructor(iv) {
	super()
	this.push(iv)
  }
    
}

class IVReader extends PassThrough {

  constructor(ivLength) {
	super()
	this.ivLength = ivLength
  }
   
  passthrough = (data,enc,callback) => {
    this.push(data)
    callback()
  }
  
  extractIV = (data,enc,callback) => {
	this.iv = Buffer.from(data,0,this.ivLength)
    this.push(data.slice(this.ivLength))
	this._transform = this.passthrough
    callback()
  }
  
  getInitializationVector() {
    return this.iv
  }
  
  _transform = this.extractIV
    
}

class FileDBI extends YadamuDBI {
 
  /*
  **
  ** !!! For the FileDBI an export operaton involves reading data from the file system and an Import operation involves writing data to the file system !!!
  **
  */

  static get DATABASE_KEY()          { return 'file' };
  static get DATABASE_VENDOR()       { return 'YABASC' };
  static get SOFTWARE_VENDOR()       { return 'YABASC - Yet Another Bay Area Software Compsny'};

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  {
	// Delete values inherited from YADAMU_CONFIGURATION
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(
	  (() => {
  	    const parms = {
          ...DBIConstants.DBI_PARAMETERS
        , ...YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}
	    }
	    delete parms.FILE
	    delete parms.CIPHER
	    delete parms.ENCRYPTION
	    delete parms.SALT
	    return parms
	  })()
	)
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return FileDBI.DBI_PARAMETERS
  }

  get DATABASE_KEY()               { return FileDBI.DATABASE_KEY };
  get DATABASE_VENDOR()            { return FileDBI.DATABASE_VENDOR };
  get SOFTWARE_VENDOR()            { return FileDBI.SOFTWARE_VENDOR };
  
  get PARALLEL_READ_OPERATIONS()   { return false };
  get PARALLEL_WRITE_OPERATIONS()  { return false }  
  
  get USE_COMPRESSION()            { return this.COMPRESSION !== 'NONE' }
  get USE_ENCRYPTION()             { return this.parameters.hasOwnProperty('ENCRYPTION') ? (this.parameters.ENCRYPTION !== false) : this.yadamu.ENCRYPTION !== false }
  get CREATE_TARGET_DIRECTORY()    { return this.parameters.hasOwnProperty('CREATE_TARGET_DIRECTORY') ? this.parameters.CREATE_TARGET_DIRECTORY : this.yadamu.CREATE_TARGET_DIRECTORY }
							      
  set EXPORT_FILE_HEADER(v)        { this._EXPORT_FILE_HEADER = v }
  get EXPORT_FILE_HEADER()         { return this._EXPORT_FILE_HEADER }
							      
  set INITIALIZATION_VECTOR(v)     { this._INITIALIZATION_VECTOR =  v }
  get INITIALIZATION_VECTOR()      { return this._INITIALIZATION_VECTOR }
  get IV_LENGTH()                  { return 16 }  
    
  get DIRECTORY()  { 
     this._DIRECTORY  = this._DIRECTORY || (() => {
	   const directory = this.parameters.DIRECTORY || this.yadamu.parameters.DIRECTORY || ""
	   return directory
	 })()
	 return this._DIRECTORY
  } 
  set DIRECTORY(v) { this._DIRECTORY = v };
    
  get FILE()                     {

    /*
	**
	** Rules for File Location are as follows
	**
	
	Parameter FILE is absolute: FILE
    OTHERWISE: 
	
	  Parameter DIRECTORY is not supplied: conn:directory/FILE
	  OTHERWISE
    
        Paramter DIRECTORY is absolute: DIRECTORY/FILE
	    OTHERWISE: conn:directory/DIRECTORY/FILE
	
	**
	*/
	
    return this._FILE || (() => {
	  let file =  this.parameters.FILE || super.FILE
      if (!path.isAbsolute(file)) {
	    if (this.DIRECTORY) {
          if (path.isAbsolute(this.DIRECTORY)) {
 		    file = path.join(this.DIRECTORY,file)
          }
          else {
            file = path.join(this.CONNECTION_PROPERTIES.directory,this.DIRECTORY,file)
          }
	    }
        else {
          file = path.join(this.CONNECTION_PROPERTIES.directory  || '',file)
		}
	  }
	  file = (this.USE_COMPRESSION && (!file.endsWith('.gz'))) ? `${file}.gz` : file
	  file = YadamuLibrary.macroSubstitions(file,this.yadamu.MACROS)
	  this._UNRESOLVED_FILE = file
	  this._FILE = path.resolve(file)
	  return this._FILE
    })()
  }

  set FILE(v)               {this._FILE = v }   
 
  get UNRESOLVED_FILE()     { return this._UNRESOLVED_FILE }   
  
  get OUTPUT_FORMAT()       { return 'JSON' }
  
  get CHECK_POINT()         { return  this._CHECK_POINT }
  set CHECK_POINT(v)        { this._CHECK_POINT = this.OUTPUT_STREAM_OFFSET + v.bytesWritten + v.writableLength}
  
  get OUTPUT_STREAK_SIZE()  { return this._OUTPUT_STREAM_OFFSET}
  set OUTPUT_STREAK_SIZE(v) { this._OUTPUT_STREAM_OFFSET = v }
  
  get IS_FILE_BASED()       { return true }

  addVendorExtensions(connectionProperties) {

  // connectionProperties.directory   = this.parameters.DIRECTORY || connectionProperties.directory 
  return connectionProperties

  }
  
  constructor(yadamu,connectionSettings,parameters) {
	super(yadamu,null,connectionSettings,parameters)
	this.outputStream = undefined;
    this.inputStream = undefined;
	this.firstTable = true;
	this.ddl = undefined;
	this.baseDirectory = path.resolve(this.CONNECTION_PROPERTIES.directory || "")
	this._DATABASE_VERSION = YadamuConstants.YADAMU_VERSION
  }

  setDescription(description) {
    this.DESCRIPTION = description.indexOf(this.baseDirectory) === 0 ? description.substring(this.baseDirectory.length+1) : description
  }
  
  executeDDL(ddl) {
	const startTime = performance.now()
	this.ddl = ddl
    this.emit(YadamuConstants.DDL_UNNECESSARY)
	return ddl
  }
    
  exportComplete(message) {
	this.eventManager.exportComplete(message)
  }
  
  closeInputStream() {      
    this.inputStream.close()
  }

  closeOutputStream() {
    this.outputStream.close()
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

  getMetadata() {
	return []
  }
  
  getSystemInformation() {
	return Object.assign(
	  super.getSystemInformation()
	, {}
    )
  }

  setSystemInformation(systemInformation) {
	super.setSystemInformation(systemInformation) 
  }
    
  setMetadata(metadata) {
	super.setMetadata(metadata)
  }
 
  releaseConnection() {
  }
 
  async initialize() {
    await super.initialize()
    // Check if an Encryption key is needed
	// An encryption key is needed if USE_ENCRYPTION is true and a KEY is not available via YADAMU or a connection level passphrase was specified.
	if (this.USE_ENCRYPTION) {
      this.ENCRYPTION_KEY = this.parameters.PASSPHRASE
	                      // Connection level passphrase specified - Generate a new Key
	                      ? await this.yadamu.generateCryptoKey(this.parameters)
				 	      : this.yadamu.parameters.ENCRYPTION_KEY_AVAILABLE 
						  // Use prcoess level encryption key
				 	      ? this.yadamu.ENCRYPTION_KEY
						  // Generate a connection level passphrase - Will take it from YADAMU_PASSPHRASE environment variable or prompt
				 	      : await this.yadamu.generateCryptoKey(this.parameters)
	}
  }
 
  async createInputStream() {
    return new Promise((resolve,reject) => {
	  const stack = new Error().stack
      const is = fs.createReadStream(this.FILE)
      is.once('open',() => {resolve(is)}).once('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this.DRIVER_ID,err,stack,this.FILE) : new FileError(this.DRIVER_ID,err,stack,this.FILE) )})
    })
  }
  
  async initializeExport() {

    // For FileDBI Export is Reading data from the file system.
	
	this.DIRECTORY = this.SOURCE_DIRECTORY
	
	// this.LOGGER.trace([this.constructor.name],`initializeExport()`)
	super.initializeExport()
	this.setDescription(this.FILE)
	
	if (this.USE_ENCRYPTION) {
      await this.loadInitializationVector()
    }

	this.inputStream = await this.createInputStream()
  }

  finalizeExport() {
 	// this.LOGGER.trace([this.constructor.name,],'finalizeExport()')
	this.closeInputStream()
  }
  
  async createInitializationVector() {

	this.INITIALIZATION_VECTOR = await new Promise((resolve,reject) => {
      crypto.randomFill(new Uint8Array(this.IV_LENGTH), (err, iv) => {
		if (err) reject(err)
	    resolve(iv)
      })
	})	    
  } 
  
  getFileOutputStream(tableName) {
    return this.outputStream
  }

  async createWriteStream() {
   return new Promise((resolve,reject) => {
      const ws = fs.createWriteStream(this.FILE,{flags :"w"})
	  this.OUTPUT_STREAM_OFFSET = 0;
	  const stack = new Error().stack
      ws.on('open',() => {resolve(ws)})
	    .on('error',(err) => {reject(err.code === 'ENOENT' ? new DirectoryNotFound(this.DRIVER_ID,err,stack,this.FILE) : new FileError(this.DRIVER_ID,err,stack,this.FILE) )})
	})
  }
  
  async createOutputStream() {

    const streams = []
	
    if (this.USE_COMPRESSION) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGzip() : createDeflate())
    }

    if (this.USE_ENCRYPTION) {
      await this.createInitializationVector()
      // this.LOGGER.trace([this.constructor.name,'ENCRYPTION',this.yadamu.parameters.ENCRYPTION_KEY_AVAILABLE,this.parameters.ENCRYPTION_KEY_AVAILABLE,this.CIPHER,this.ENCRYPTION_KEY.toString('hex'), Buffer.from(this.INITIALIZATION_VECTOR).toString('hex')],'Generating CIPHER stream')
	  const cipherStream = crypto.createCipheriv(this.CIPHER,this.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  streams.push(cipherStream)
	  streams.push(new IVWriter(this.INITIALIZATION_VECTOR))
	}
    
	if (this.CREATE_TARGET_DIRECTORY) {
      await fsp.mkdir(path.dirname(this.FILE),{recursive:true})
	}
	
	const ws = await this.createWriteStream()
	streams.push(ws)
	const os = streams.length === 1 ? streams[0] : compose(...streams)
	
	
	// Add a dummy Error Handler - it will be removed and replaced with real when the stream is used
	
	this.osErrorHandler = YadamuLibrary.NOOP
	os.on('error',this.osErrorHandler)
	return os;

  }

  checkDirectory() {
  }
	 
  
  async initializeImport() {

    // For FileDBI Import is Writing data to the file system.
	// this.LOGGER.trace([this.constructor.name],`initializeImport()`)
		
	this.DIRECTORY = this.TARGET_DIRECTORY
	super.initializeImport()
    this.setDescription(this.FILE)

	this.outputStream = await this.createOutputStream()
    this.LOGGER.info([this.DATABASE_VENDOR,YadamuConstants.WRITER_ROLE],`Writing data to "${this.FILE}".`)
  }
  
  async initializeData() {
  
	// Set up the pipeline and write the system information, ddl and metadata sections to the pipe...
    // this.LOGGER.trace([this.constructor.name],`initializeData()`)

    // Remove the source structure from each metadata object prior to serializing it. Put it back after the serialization has been completed.

    const sourceInfo = {}
    Object.keys(this.metadata).forEach((key) => {
	  if (this.metadata[key].source || this.metadata[key].partitionCount) {
		sourceInfo[key] = this.metadata[key].source; 
	    delete this.metadata[key].source
		delete this.metadata[key].partitionCount
	  }
	})
    let exportFileHeader = `{"systemInformation":${JSON.stringify(this.systemInformation)}${this.ddl ? `,"ddl":${JSON.stringify(this.ddl)}` : ''},"metadata":${JSON.stringify(this.metadata)}`
	Object.keys(sourceInfo).forEach((key) => {this.metadata[key].source = sourceInfo[key]})

    if ((this.MODE === 'DDL_ONLY') || (YadamuLibrary.isEmpty(this.metadata))) {
	  const exportFileContents = `${exportFileHeader}}`
      const finalizeExport = new ExportWriter(exportFileContents)
      await pipeline(finalizeExport,this.outputStream)
	}
    else {
	  exportFileHeader = `${exportFileHeader},"data":{` 
	} 
	 
	this.EXPORT_FILE_HEADER = exportFileHeader
	const initializeExport = new ExportWriter(this.EXPORT_FILE_HEADER)
	await pipeline(initializeExport,this.outputStream,{end: false})
  }	
	
  traceStreamEvents(streams) {

    // Add event tracing to the streams
	  
	streams[0].once('readable',() => {
	  console.log(streams[0].constructor.name,'readable')
	})
	
    streams.forEach((s,idx) => {
	  s.once('end',() => {
	     console.log(s.constructor.name,'end')
	  }).once('finish', (err) => {
	    console.log(s.constructor.name,'finish')
	  }).once('close', (err) => {
        console.log(s.constructor.name,'close')
	  }).once('error', (err) => {
        console.log(s.constructor.name,'error',err.message)
      })
	})
  }
  
  async finalizeData() {
    
	// this.LOGGER.trace([this.constructor.name],`finalizeData(${YadamuLibrary.isEmpty(this.metadata)})`)
	
	if (!YadamuLibrary.isEmpty(this.metadata)) {
      const finalizeExport = new ExportWriter('}}')
	  // Restore the default listeners to the outputStreams
      // this.traceStreamEvents([finalize,...this.outputStreams])
      await pipeline(finalizeExport,this.outputStream)
    }
  }
  
  finalizeImport() {}

    
  finalize() {
    if (this.inputStream !== undefined) {
      this.closeInputStream()
    }
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  abort(e) {

    try {
      if (this.inputStream !== undefined) {
        this.closeInputStream()
	  }
    } catch (err) {
      this.LOGGER.handleException([this.DATABASE_VENDOR,'ABORT','InputStream'],err)
    }
	 
    try {
      if (this.outputStream !== undefined) {
        this.closeOutputStream()
	  }
    } catch (err) {
      this.LOGGER.handleException([this.DATABASE_VENDOR,'ABORT','OutputStream'],err)
    }
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */
      
  async generateStatementCache(schema,executeDDL) {
    this.statementCache = []
	this.emit(YadamuConstants.CACHE_LOADED)
	return this.statementCache
  }

  getDDLOperations() {
    return []
  }
  
  getSchemaMetadata(){
    return []
  }
  
  getTableInfo(tableName) {
	
    if (tableName === null) {
	  // Hack to enable statisticsCollector to use the YadamuWriter interface to collect statistics about the cotnents of a YADAMU export file...
      return {}
    }

    if (this.metadata === undefined) {
      this.LOGGER.logInternalError([this.constructor.name,`getTableInfo()`,tableName],`Metadata undefined. Cannot obtain required information.`)
	}

	if (this.metadata[tableName] === undefined) {
      this.LOGGER.logInternalError([this.constructor.name,`getTableInfo()`,tableName],`No metadata entry for "${tableName}". Current entries: ${JSON.stringify(Object.keys(this.metadata))}`)
	}

	// ### Need to simplify and standardize DataTypes - Data type mapping for Files.. 
	
	// Include a dummy dataTypes array of the correct length to ensure the column count assertion does not throw
	return { 
	  tableName         : tableName
	, _SPATIAL_FORMAT   : this.INBOUND_SPATIAL_FORMAT
    , columnNames       : [... this.metadata[tableName].columnNames]
	, insertMode        : 'Batch'
	, columnCount       : this.metadata[tableName].columnNames.length
    , targetDataTypes   : [... this.metadata[tableName].dataTypes]
	, vendor            : this.systemInformation.vendor
    }
  }

  _getInputStream() {  
    // Return the inputStream and the transform streams required to process it.
    const stats = fs.statSync(this.FILE)
    const fileSizeInBytes = stats.size
    this.LOGGER.info([this.DATABASE_VENDOR,YadamuConstants.READER_ROLE],`Processing file "${this.FILE}". Size ${fileSizeInBytes} bytes.`)
	return this.inputStream
  }
  
  async loadInitializationVector() {

    let cause	  
	try {
      cause = new FileError(this.DRIVER_ID,new Error(`Unable to load Initialization Vector from "${this.FILE}".`))
	  const fd = await fsp.open(this.FILE)
      const iv = new Uint8Array(this.IV_LENGTH)
	  const results = await fd.read(iv,0,this.IV_LENGTH,0)
	  this.INITIALIZATION_VECTOR = iv;
	  await fd.close()
	} catch (e) {
	  cause.cause = e
	  throw cause
    }
	
  }	
	  
  async getInputStreams(pipelineState) {

    this.PIPELINE_STATE = pipelineState
	pipelineState.readerState = this.ERROR_STATE
	this.resetExceptionTracking()

	const streams = []

    const inputStream = await this.getInputStream(null,pipelineState)
	const inputStreamState = inputStream.STREAM_STATE
	
	inputStream.once('readable',() => {
      inputStreamState.startTime    = performance.now()
    }).on('end',() => {
      inputStreamState.endTime = performance.now()
	}).on('error',(err) => { 
  	  inputStreamState.endTime = performance.now()
      pipelineState.failed = true;
  	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.INPUT_STREAM_ID
      inputStreamState.error = err
	})
	
	streams.push(inputStream)
	
	if (this.USE_ENCRYPTION) {
	  streams.push(new IVReader(this.IV_LENGTH))
      // this.LOGGER.trace([this.constructor.name,'DECRYPTION',this.yadamu.parameters.ENCRYPTION_KEY_AVAILABLE,this.parameters.ENCRYPTION_KEY_AVAILABLE,this.CIPHER,this.ENCRYPTION_KEY.toString('hex'), Buffer.from(this.INITIALIZATION_VECTOR).toString('hex')],'Generating CIPHER stream')
	  const decipherStream = crypto.createDecipheriv(this.CIPHER,this.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  streams.push(decipherStream)
	}

	if (this.USE_COMPRESSION) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGunzip() : createInflate())
	}
	
	const jsonParser = new JSONParser(this.MODE, this.FILE, pipelineState, this.LOGGER)
	const parserStreamState = jsonParser.STREAM_STATE
    jsonParser.on('error',(err) => {
      parserStreamState.endTime = performance.now()
      pipelineState.failed = true;
	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.PARSER_STREAM_ID
      parserStreamState.pipelineError = err
    })

	streams.push(jsonParser)
	
	const streamSwitcher = new StreamSwitcher(this,this.yadamu,pipelineState)
	const streamSwitcherState = streamSwitcher.STREAM_STATE
    streamSwitcher.on('end',() => {
      streamSwitcherState.endTime = performance.now()
    }).on('error',(err) => { 
      streamSwitcherState.endTime = performance.now()
      pipelineState.failed = true;
	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.PARSER_STREAM_ID
      streamSwitcherState.pipelineError = err
    })
	
	streams.push(streamSwitcher)
    // console.log(streams.map((s) => { return s.constructor.name }).join(' ==> '))

	return streams;
  }

  getOutputManager(tableName,pipelineState) {
    // Override parent method to allow output stream to be passed to worker
    // s.LOGGER.trace([this.constructor.name],`getOutputStream(${tableName},${this.firstTable})`)
	const jw =  new JSONOutputManager(this,tableName,pipelineState,this.firstTable,this.status,this.LOGGER)
	return jw
  }
  
  getOutputStream(pipelineState) {  
	this.outputStream.PIPELINE_STATE = pipelineState
	this.outputStream.STREAM_STATE = { 
	  vendor : this.DATABASE_VENDOR 
	}
    pipelineState[DBIConstants.OUTPUT_STREAM_ID] = this.outputStream.STREAM_STATE
    return this.outputStream
  }
      
  getOutputStreams(tableName,pipelineState) {

    this.PIPELINE_STATE = pipelineState
	pipelineState.writerState = this.ERROR_STATE
	this.resetExceptionTracking()

    const outputStreams = []

	// Create a JSON Writer
	
	// For the purposes of tracking pipeline operation the JSON Writer (which transforms the data into JSON for output) is a proxy
	// for the Writer. This is necessary as the file writer is used to handle multiple pipeline operations. 

	const jsonWriter = this.getOutputManager(tableName,pipelineState)
	const transformationStreamState = jsonWriter.STREAM_STATE
    
	jsonWriter.once('readable',() => {
      transformationStreamState.startTime = performance.now()
    }).on('finish',() => { 
      transformationStreamState.endTime = performance.now()
      transformationStreamState.endTime = performance.now()
      pipelineState.lost += jsonWriter.writableLength
    }).on('error',(err) => {
      transformationStreamState.endTime = performance.now()
      pipelineState.failed = true;
	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.TRANSFORMATION_STREAM_ID
      pipelineState.lost += jsonWriter.writableLength
      transformationStreamState.pipelineError = err
    })
	
	outputStreams.push(jsonWriter)

    const outputStream = this.getOutputStream(pipelineState)
	this.CHECK_POINT = outputStream.bytesWritten+outputStream.writableLength
    const outputStreamState = outputStream.STREAM_STATE
	
	this.outputStream.removeListener('error',this.osErrorHandler)
	
	this.osErrorHandler = (err) => {
      outputStreamState.endTime = performance.now()
      pipelineState.failed = true;
	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.OUTPUT_STREAM_ID
      pipelineState.lost += jsonWriter.writableLength
      outputStreamState.pipelineError = err
    }
	
	outputStream.once('pipe',() => {
      outputStreamState.startTime = performance.now()
    }).once('finish',() => { 
      outputStreamState.endTime = performance.now()
      pipelineState.lost += jsonWriter.writableLength
    }).on('error', this.osErrorHandler )

    this.CHECK_POINT = outputStream
	outputStreams.push(outputStream)
    
	this.firstTable = false;
    // console.log(outputStreams.map((s) => { return s.constructor.name }).join(' ==> '))
    return outputStreams	
	
  }
  
  async createCloneStream(options) {
	await this.initialize()
	const streams = []
	this.inputStream = await this.createInputStream()
	streams.push(this.inputStream)
	
	if (options.encryptedInput) {
	  await this.loadInitializationVector()
	  streams.push(new IVReader(this.IV_LENGTH))
	  const decipherStream = crypto.createDecipheriv(this.CIPHER,this.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  streams.push(decipherStream)
	}
	
	if (options.compressedInput) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGunzip() : createInflate())
	}
      
	if (options.compressedOutput) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGzip() : createDeflate())
	}
	
	if (options.encryptedOutput) {
  	  await this.createInitializationVector()
	  const cipherStream = crypto.createCipheriv(this.CIPHER,this.ENCRYPTION_KEY,this.INITIALIZATION_VECTOR)
	  streams.push(cipherStream)
	  streams.push(new IVWriter(this.INITIALIZATION_VECTOR))
	}

    const outputFilePath = path.resolve(options.filename)
	const inputFilePath = this.FILE;
    this.FILE = outputFilePath
	await this.createOutputStream()
	streams.push(this.outputStream)
	this.LOGGER.info([this.DATABASE_VENDOR,YadamuConstants.WRITER_ROLE,options.encryptedInput ? 'DECRYPT' : 'ENCRYPT'],`File: "${inputFilePath}" ==> "${outputFilePath}"`)
	return streams;
  }
    
  async createConnectionPool() { /* OVERRIDE */ }
  
  async getConnectionFromPool() { /* OVERRIDE */ }
  
  async closeConnection() { /* OVERRIDE */ }
  
  async closePool() { /* OVERRIDE */ }
  
  async getComparator(configuration) {
	 await this.initialize()
	 return new FileCompare(this,configuration)
  }
  
  async truncateTable() {
	  
	if (this.USE_COMPRESSION || this.USE_ENCRYPTION) {
      throw new Error('Error recovery not supported with compressed and/or encypted output')
	}
	  
	await new Promise((resolve,reject) => {
	  this.outputStream.close(() => {
		resolve()
	  })
	})
	await fsp.truncate(this.outputStream.path,this.CHECK_POINT);
    this.OUTPUT_STREAM_OFFSET = this.CHECK_POINT;
	this.outputStream = await new Promise((resolve,reject) => {
      const ws = fs.createWriteStream(this.outputStream.path,{flags :"a"})
	  const stack = new Error().stack
      ws.on('open',() => {resolve(ws)})
	    .on('error',(err) => {reject(err.code === 'ENOENT' ? new DirectoryNotFound(this.DRIVER_ID,err,stack,this.FILE) : new FileError(this.DRIVER_ID,err,stack,this.FILE) )})
	})
  }
	  
}

export { FileDBI as default }
