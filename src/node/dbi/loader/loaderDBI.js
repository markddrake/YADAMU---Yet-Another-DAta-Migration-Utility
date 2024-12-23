					                  
import fs                             from 'fs';
import fsp                            from 'fs/promises';
import path                           from 'path';
import crypto                         from 'crypto';
					                  
import {                              
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

import csv                            from 'csv-parser';

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

import {
  YadamuError
}                                     from '../../core/yadamuException.js'

/* Yadamu DBI */                                    

import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                    from '../file/fileException.js'

/* Vendor Specific DBI Implimentation */                                   

import Comparitor                     from './loaderCompare.js'

import LoaderConstants                from './loaderConstants.js'

import JSONParser                     from './jsonParser.js'
import JSONTransform                  from './jsonTransform.js'
import JSONOutputManager              from './jsonOutputManager.js'
import ArrayOutputManager             from './arrayOutputManager.js'
import CSVOutputManager               from './csvOutputManager.js'
import CSVParser                      from './csvParser.js'
import CSVTransform                   from './csvTransform.js'

/*
**
** YADAMU Database Inteface class skeleton
**
*/

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

class CloudService {
	
  constructor() {
  }
  
  async getContentAsString(key) {
	return await fsp.readFile(key,{encoding: 'utf8'})
  }
  
  async createReadStream(path) {
    return new Promise((resolve,reject) => {
	  const stack = new Error().stack
      const inputStream = fs.createReadStream(path);
      inputStream.once('open',() => {resolve(inputStream)}).once('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this,err,stack,path) : new FileError(this,err,stack,path) )})
    })
  }
}

class LoaderDBI extends YadamuDBI {
 
  /*
  **
  ** Extends YadamuDBI enabling operations on local File System.
  ** 
  ** !!! Make sure your head is wrapped around the following statements before touching this code.
  **
  ** An Export\load operaton involves reading data from the local file systems 
  ** An Import\UnLoad operation involves writing data to the local file system.
  **
  */

  static get DATABASE_KEY()          { return LoaderConstants.DATABASE_KEY};
  static get DATABASE_VENDOR()       { return LoaderConstants.DATABASE_VENDOR};
  static get SOFTWARE_VENDOR()       { return LoaderConstants.SOFTWARE_VENDOR};
  static get PROTOCOL()              { return LoaderConstants.PROTOCOL }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	// Delete FILE iherited from YADAMU_CONFIGURATION
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
	return LoaderDBI.DBI_PARAMETERS
  }
    
  get DATABASE_KEY()               { return LoaderDBI.DATABASE_KEY };
  get DATABASE_VENDOR()            { return LoaderDBI.DATABASE_VENDOR };
  get SOFTWARE_VENDOR()            { return LoaderDBI.SOFTWARE_VENDOR };
  get PARTITION_LEVEL_OPERATIONS() { return true }
  get PROTOCOL()                   { return LoaderDBI.PROTOCOL };
  
  get DBI_PARAMETERS()    { 
	this._DBI_PARAMETERS = this._DBI_PARAMETERS || Object.freeze({
	  ...super.DBI_PARAMETERS
	})
	return this._DBI_PARAMETERS
  }
  
  get JSON_OUTPUT()              { return this.OUTPUT_FORMAT === 'JSON' }
  get ARRAY_OUTPUT()             { return this.OUTPUT_FORMAT === 'ARRAY' }
  get CSV_OUTPUT()               { return this.OUTPUT_FORMAT === 'CSV'  }
  get PROTOCOL()                 { return 'file://' }
  
  get BASE_DIRECTORY() {

    /*
	**
	** Rules for Root Folder Location are as follows
	**
	
	Parameter BASE_DIRECTORY is absolute: DIRECTORY
    OTHERWISE: 
	
	  Parameter DIRECTORY is not supplied: conn:directory
      OTHERWISE: conn:directory/DIRECTORY/FILE
	
	**
	*/
	
    return this._BASE_DIRECTORY || (() => {
	  let baseDirectory =  this.CONNECTION_PROPERTIES.directory || ""
	  if (this.DIRECTORY) {
        if (path.isAbsolute(this.DIRECTORY)) {
	      baseDirectory = this.DIRECTORY
        }
        else {
          baseDirectory = path.join(baseDirectory,this.DIRECTORY)
		}
	  }
	  baseDirectory = YadamuLibrary.macroSubstitions(baseDirectory,this.yadamu.MACROS)
	  this._BASE_DIRECTORY = path.resolve(baseDirectory)
	  return this._BASE_DIRECTORY
    })()
  
  }
  
  set CONTROL_FILE_PATH(v)       { this._CONTROL_FILE_FOLDER = path.dirname(v); this._CONTROL_FILE_PATH = v }   
  get CONTROL_FILE_PATH()        { return this._CONTROL_FILE_PATH }
  get CONTROL_FILE_FOLDER()      { return this._CONTROL_FILE_FOLDER }
  get METADATA_FOLDER()          { return this._METADATA_FOLDER     || (() => {this._METADATA_FOLDER = path.join(this.CONTROL_FILE_FOLDER,'metadata'); return this._METADATA_FOLDER })() }
  get DATA_FOLDER()              { return this._DATA_FOLDER         || (() => {this._DATA_FOLDER = path.join(this.CONTROL_FILE_FOLDER,'data'); return this._DATA_FOLDER })() }
  
  get IMPORT_FOLDER()            { return this._IMPORT_FOLDER       || (() => {this._IMPORT_FOLDER = path.join(this.BASE_DIRECTORY,this.CURRENT_SCHEMA); return this._IMPORT_FOLDER})() }
  get EXPORT_FOLDER()            { return this._EXPORT_FOLDER       || (() => {this._EXPORT_FOLDER = path.join(this.BASE_DIRECTORY,this.CURRENT_SCHEMA); return this._EXPORT_FOLDER})() }
  
  get OUTPUT_FORMAT() { 
    this._OUTPUT_FORMAT = this._OUTPUT_FORMAT || (() => {
	  switch (this.controlFile?.settings.contentType || this.parameters.OUTPUT_FORMAT?.toUpperCase() || 'JSON') {
        case 'ARRAY':
          this._FILE_EXTENSION = 'data'
          this._OUTPUT_FORMAT = 'ARRAY'
		  this._OUTPUT_MANAGER = ArrayOutputManager
          break;
        case 'CSV':
          this._FILE_EXTENSION = 'csv'
          this._OUTPUT_FORMAT = 'CSV'
		  this._OUTPUT_MANAGER = CSVOutputManager
          break;
        case JSON:
        default:
          this._FILE_EXTENSION = 'json'
          this._OUTPUT_FORMAT = 'JSON'
		  this._OUTPUT_MANAGER = JSONOutputManager
	  }
	  return this._OUTPUT_FORMAT
	})()
    return this._OUTPUT_FORMAT
  }

  get OUTPUT_MANAGER() {
    this._OUTPUT_MANAGER = this._OUTPUT_MANAGER || (() => { 
	  // Referencing _OUTPUT_FORMAT sets _OUTPUT_MANAGER
	  const outputformat = this.OUTPUT_FORMAT; 
	  return this._OUTPUT_MANAGER
	})()
	return this._OUTPUT_MANAGER
  }
  
  get FILE_EXTENSION() {
    this._FILE_EXTENSION = this._FILE_EXTENSION || (() => { 
	  // Referencing _OUTPUT_FORMAT sets _FILE_EXTENSION
	  this.PIPELINE_STATE = this.OUTPUT_FORMAT; 
	  return this._FILE_EXTENSION
	})()
	return this._FILE_EXTENSION
  }

  get USE_COMPRESSION()        { return this.COMPRESSION !== 'NONE' }
  get USE_ENCRYPTION()         { return this.parameters.hasOwnProperty('ENCRYPTION') ? (this.parameters.ENCRYPTION !== false) : this.yadamu.ENCRYPTION !== false }

  get COMPRESSED_INPUT()       { return this.controlFile.settings.compression !== "NONE" }
  get COMPRESSION_FORMAT()     { return this.controlFile.settings.compression }
  get ENCRYPTED_INPUT()        { return this.controlFile.settings.encryption !== "NONE" }
  
  set INITIALIZATION_VECTOR(v) { this._INITIALIZATION_VECTOR =  v }
  get INITIALIZATION_VECTOR()  { return this._INITIALIZATION_VECTOR }
  get IV_LENGTH()              { return 16 }  
  
  get ENCRYPTION_KEY()         { return this.isManager() ? super.ENCRYPTION_KEY : this.manager.ENCRYPTION_KEY }
  set ENCRYPTION_KEY(v)        { super.ENCRYPTION_KEY = v }
  
  addVendorExtensions(connectionProperties) {

    // connectionProperties.directory   = this.parameters.DIRECTORY || connectionProperties.directory 
    return connectionProperties

  }
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)

	this.COMPARITOR_CLASS = Comparitor

	this.yadamuProperties = {}
	this.baseDirectory = path.resolve(this.CONNECTION_PROPERTIES.directory || "")
	this._DATABASE_VERSION = YadamuConstants.YADAMU_VERSION
  }    	
 
  isValidDDL() {
    return true;
  }
  
  resolve(target) {
	return path.resolve(target)
  }

  setDescription(description) {
    this.DESCRIPTION = description.indexOf(this.baseDirectory) === 0 ? description.substring(this.baseDirectory.length+1) : description
  }
  
  getURI(target) {
    return `${this.PROTOCOL}${this.resolve(target)}`
  }
  
  makeCloudPath(target) {
	return target
  }
  
  isDatabase() {
    return true;
  }
  
  async getSystemInformation() {
    // this.LOGGER.trace([this.constructor.name,this.exportFilePath],`getSystemInformation()`)     	
	return this.controlFile.systemInformation
  }

  makeRelative(target) {
	return path.join(this.EXPORT_FOLDER,path.relative(this.controlFile.settings.baseFolder,target))
  }
  
  getDataFileName(tableName,partitionNumber) {	 
    return Array.isArray(this.controlFile.data[tableName].files) ?  this.controlFile.data[tableName].files.shift() : this.controlFile.data[tableName].file 
  }
  
  async isValidCopyFormat(supportedFormats) {
	 await this.loadControlFile();
	 return supportedFormats.includes(this.controlFile.settings.contentType)
  }

  async loadMetadataFiles(stagedDataCopy) {
  	this.metadata = {}
    if (this.controlFile.metadata) {
	  let stack
      let metadataPath
      try {
        stack = new Error().stack;
        const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
		  metadataPath = this.makeAbsolute(this.controlFile.metadata[tableName].file)
   	      return fsp.readFile(metadataPath,{encoding: 'utf8'})
	    }))
        metdataRecords.forEach((content) =>  {
          const json = this.parseJSON(content)
		  this.metadata[json.tableName] = json;
          if (stagedDataCopy) {
            json.dataFile = this.controlFile.data[json.tableName].files || this.controlFile.data[json.tableName].file 
		  }
        })
      } catch (err) {
        throw err.code === 'ENOENT' ? new FileNotFound(this,err,stack,metadataPath) : new FileError(this,err,stack,metadataPath)
	  }
    }
    return this.metadata;      
  }

  generateMetadata() {
	return this.metadata
  }

  async getSchemaMetadata() {

    // this.LOGGER.trace([this.constructor.name,this.EXPORT_PATH],`getSchemaMetadata()`)
	
	this.metadata = await this.loadMetadataFiles(false)
	
	const schemaInformation = Object.keys(this.metadata).flatMap((tableName) => {
	  const tableInfo =  {
		TABLE_SCHEMA          : this.metadata[tableName].tableSchema
	  , TABLE_NAME            : tableName
      , DATA_TYPE_ARRAY       : this.metadata[tableName].dataTypes
	  , SPATIAL_FORMAT        : this.INBOUND_SPATIAL_FORMAT
	  } 
      if (this.yadamu.PARALLEL_ENABLED && this.PARTITION_LEVEL_OPERATIONS && Array.isArray(this.controlFile.data[tableName].files)) {
	    const partitionInfo = this.controlFile.data[tableName].files.map((fileName,idx) => { 
		  return {
		    ...tableInfo
		  , PARTITION_COUNT: this.controlFile.data[tableName].files.length, PARTITION_NUMBER: idx+1 
		  }
        })
		return partitionInfo
	  }
      return tableInfo
	})
	return schemaInformation;
  }
	   
  /*
  **
  ** Remember: Import is Writing data to the local file system - unload.
  **
  */

  createControlFile() {

	this.controlFile = { 
  	  settings : {
  	    contentType        : this.OUTPUT_FORMAT
      , compression        : this.COMPRESSION
	  , encryption         : this.USE_ENCRYPTION ? this.CIPHER : 'NONE'
	  , baseFolder         : this.IMPORT_FOLDER
      },
	}
  }


  metadataRelativePath(tableName) {
     let filename = `${tableName}.json`
     return path.relative(this.CONTROL_FILE_FOLDER,path.join(this.METADATA_FOLDER,filename))
  }
  
  dataRelativePath(tableName) {
     let filename = `${tableName}.${this.FILE_EXTENSION}`
	 filename = this.USE_COMPRESSION ? `${filename}.gz` : filename
	 return path.relative(this.CONTROL_FILE_FOLDER,path.join(this.DATA_FOLDER,filename))
  }
  
  makeAbsolute(relativePath) {
    return path.resolve(this.CONTROL_FILE_FOLDER,relativePath)
  }
  
  writeFile(filename,content) {
	return fsp.writeFile(filename,JSON.stringify(content))
  }
  
  async writeMetadata() {
	  
	// this.LOGGER.trace([this.constructor.name],`writeMetadata()`)
    Object.values(this.metadata).forEach((table) => {delete table.source})

    this.controlFile.systemInformation = this.systemInformation
	
	if (this.ddl  && (this.ddl.length > 0)) {
	  this.controlFile.ddl = this.ddl
	}
	
    this.controlFile.metadata = {}
    this.controlFile.data = {}

    Object.values(this.metadata).forEach((tableMetadata) => {
	   const file = this.metadataRelativePath(tableMetadata.tableName) 
       this.controlFile.metadata[tableMetadata.tableName] = {file: file}
    })
	
	Object.values(this.metadata).forEach((tableMetadata) =>  {
	  if (tableMetadata.partitionCount) {
		const padSize = tableMetadata.partitionCount.toString().length
		this.controlFile.data[tableMetadata.tableName] = {files : new Array(tableMetadata.partitionCount).fill(0).map((v,i) => { return this.dataRelativePath(`${tableMetadata.tableName}.${(i+1).toString().padStart(padSize,"0")}`)})}
	  }
	  else {
	    const file =  this.dataRelativePath(tableMetadata.tableName) 
        this.controlFile.data[tableMetadata.tableName] = {file : file }
      }
    })
	
    let stack
	try {
	  stack = new Error().stack;
   	  await this.writeFile(this.CONTROL_FILE_PATH,this.controlFile)
	} catch (err) {
      throw err.code === 'ENOENT' ? new DirectoryNotFound(this,err,stack,this.CONTROL_FILE_PATH) : new FileError(this,err,stack,this.CONTROL_FILE_PATH)
	}
	
    let file
	try {
      const results = await Promise.all(Object.values(this.metadata).map((tableMetadata,idx) => {
	    stack = new Error().stack;
   	    file = this.makeAbsolute(this.controlFile.metadata[tableMetadata.tableName].file)
        return this.writeFile(file,tableMetadata)   
      }))
	} catch (err) {
      throw err.code === 'ENOENT' ? new DirectoryNotFound(this,err,stack,file) : new FileError(this,err,stack,file)
	}
	this.emit(YadamuConstants.DDL_UNNECESSARY)
	this.LOGGER.info(['IMPORT',this.DATABASE_VENDOR],`Created Control File: "${this.getURI(this.CONTROL_FILE_PATH)}"`)
  }

  async setMetadata(metadata) {

    // this.LOGGER.trace([this.constructor.name,this.getWorkerNumber()],`setMetadata()`)

    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)

  }
  
  setFolderPaths(controlFileFolder,schema) {  
	this.CONTROL_FILE_PATH  = `${path.join(controlFileFolder,schema)}.json`
	this.setDescription(this.CONTROL_FILE_FOLDER)
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
  
  async initializeImport() {
	 
	// this.LOGGER.trace([this.constructor.name,this.getWorkerNumber()],`initializeImport()`)
    
    // Must set DIRECTORY before referencing EXPORT_FOLDER
    // Calculate the base directory for the unload operation. The Base Directory is dervied from the target schema name specified by the CURRENT_SCHEMA

    this.DIRECTORY = this.TARGET_DIRECTORY
	this.setFolderPaths(this.IMPORT_FOLDER,this.CURRENT_SCHEMA)
	
    // Create the Upload, Metadata and Data folders
	await fsp.mkdir(this.IMPORT_FOLDER, { recursive: true })
    await fsp.mkdir(this.METADATA_FOLDER, { recursive: true })
    await fsp.mkdir(this.DATA_FOLDER, { recursive: true })
    
	this.LOGGER.info(['IMPORT',this.DATABASE_VENDOR],`Created directory: "${this.PROTOCOL}${this.resolve(this.IMPORT_FOLDER)}"`)
    this.createControlFile()
  }
  
  async initializeData() {
    const result = await this.writeMetadata()
  }
  
  getOutputStream(tableName,pipelineState) {
	// this.LOGGER.trace([this.constructor.name],`getOutputStream()`)
	const os = new this.OUTPUT_MANAGER(this,tableName,pipelineState,this.status,this.LOGGER)  
    return os;
  }
  
  getFileOutputStream(tableName,pipelineState) {

    // this.LOGGER.trace([this.constructor.name],`getFileOutputStream(${this.controlFile.data[tableName].file})`)
    const outputStream = fs.createWriteStream(this.makeAbsolute(this.getDataFileName(tableName)))
	outputStream.PIPELINE_STATE = pipelineState
	outputStream.STREAM_STATE = { 
	  vendor : this.DATABASE_VENDOR
	}
    pipelineState[DBIConstants.OUTPUT_STREAM_ID] = outputStream.STREAM_STATE
	outputStream.on('finish',() => {
      outputStream.STREAM_STATE.endTime = performance.now()
    }).on('error',(err) => {
      outputStream.STREAM_STATE.endTime = performance.now()
      pipelineState.failed = true;
  	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.INPUT_STREAM_ID
      outputStream.STREAM_STATE.onError = err
      this.failedPrematureClose = YadamuError.prematureClose(err)
    })
	return outputStream;
  }

  async createInitializationVector() {

	return await new Promise((resolve,reject) => {
      crypto.randomFill(new Uint8Array(this.IV_LENGTH), (err, iv) => {
		if (err) reject(err)
	    resolve(iv)
      })
	})	    
  } 
 
  async getOutputStreams(tableName,pipelineState) {

    // this.LOGGER.trace([this.constructor.name,'getOutputStreams()'],`Waiting on DDL Complete. [${this.ddlComplete}]`)
	await this.ddlComplete;
    // this.LOGGER.trace([this.constructor.name,'getOutputStreams()'],`DDL Complete. [${this.ddlComplete}]`)
	this.reloadControlFile()

    this.PIPELINE_STATE = pipelineState
	pipelineState.writerState = this.ERROR_STATE

	const streams = []
	
	const transformationManager = this.getOutputStream(tableName,pipelineState)
	const transformStreamState = transformationManager.STREAM_STATE
	
	transformationManager.once('readable',() => {
	  transformStreamState.startTime = performance.now()
	})
	streams.push(transformationManager)
	
	if (this.USE_COMPRESSION) {
      streams.push(this.COMPRESSION_FORMAT === 'GZIP' ? createGzip() : createDeflate())
    }
	
	if (this.USE_ENCRYPTION) {
	  const iv = await this.createInitializationVector()
      // this.LOGGER.trace([this.constructor.name,'ENCRYPTION',this.yadamu.parameters.ENCRYPTION_KEY_AVAILABLE,this.parameters.ENCRYPTION_KEY_AVAILABLE,this.CIPHER,this.ENCRYPTION_KEY.toString('hex'), Buffer.from(this.INITIALIZATION_VECTOR).toString('hex')],'Generating CIPHER stream')
	  const cipherStream = crypto.createCipheriv(this.CIPHER,this.ENCRYPTION_KEY,iv)
	  streams.push(cipherStream)
	  streams.push(new IVWriter(iv))
	}

	const outputStream = this.getFileOutputStream(tableName,pipelineState)
    streams.push(outputStream)

    // console.log(streams.map((s) => { return s.constructor.name }).join(' ==> '))
	return streams;
  }
  /*
  **
  ** !!! Remember: Export is Reading data from the local file system - Load
  **
  */
  
  async loadControlFile() {

    // Must set DIRECTORY before referencing EXPORT_FOLDER
    // Calculate the base directory for the load operation. The Base Directory is dervied from the schema name specified by CURRENT_SCHEMA

    this.DIRECTORY = this.SOURCE_DIRECTORY
    this.setFolderPaths(this.EXPORT_FOLDER,this.CURRENT_SCHEMA)

	let stack
	try {
	  stack = new Error().stack;
      const fileContents = await fsp.readFile(this.CONTROL_FILE_PATH,{encoding: 'utf8'})
      this.controlFile = this.parseJSON(fileContents)
	} catch (err) {
      throw err.code === 'ENOENT' ? new FileNotFound(this,err,stack,this.CONTROL_FILE_PATH) : new FileError(this,err,stack,this.CONTROL_FILE_PATH)
	}
  }
  
  async initializeExport() {

	// this.LOGGER.trace([this.constructor.name],`initializeExport()`)
	
	await this.loadControlFile()
    if ((this.MODE != 'DDL_ONLY') && (this.controlFile.settings.contentType === 'CSV')) {
      // throw new YadamuError('Loading of "CSV" data sets not supported')
    }
	this.LOGGER.info(['EXPORT',this.DATABASE_VENDOR],`Using Control File: "${this.getURI(this.CONTROL_FILE_PATH)}"`)

  }
  
  getDDLOperations() {
	return this.controlFile.ddl
  }

  async _getInputStream(filename) {
    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,tableInfo.TABLE_NAME],`Creating input stream on ${filename}`)
    const stream = fs.createReadStream(filename)
    const stack = new Error().stack;
    await new Promise((resolve,reject) => {
	  stream.on('open',() => {resolve(stream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this,err,stack,filename) : new FileError(this,err,stack,filename))})
	})
    return stream
  }

  async loadInitializationVector(filename)  {
	const fd = await fsp.open(filename)
	const iv = new Uint8Array(this.IV_LENGTH)
	const results = await fd.read(iv,0,this.IV_LENGTH,0)
	await fd.close()
	return iv;
  }	
  
  async getInputStreams(tableInfo,pipelineState) {
	  
    this.PIPELINE_STATE = pipelineState
	pipelineState.readerState = this.ERROR_STATE
	this.resetExceptionTracking()
	
	const streams = []
	const filename = this.makeAbsolute(this.getDataFileName(tableInfo.TABLE_NAME))

    const inputStream = await this.getInputStream(filename,pipelineState)
    const inputStreamState = inputStream.STREAM_STATE
	
	inputStream.once('readable',() => {
	  inputStreamState.startTime = performance.now()
	}).on('error',(err) => { 
      inputStreamState.endTime = performance.now()
	  pipelineState.failed = true
	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.INPUT_STREAM_ID
      inputStreamState.pipelineError = err
    }).on('end',() => {
      inputStreamState.endTime = performance.now()
    })
	
	streams.push(inputStream)
	
	if (this.ENCRYPTED_INPUT) {
	  const iv = await this.loadInitializationVector(filename)
	  streams.push(new IVReader(this.IV_LENGTH))
  	  // this.LOGGER.trace([this.constructor.name,'DECRYPTION',this.yadamu.parameters.ENCRYPTION_KEY_AVAILABLE,this.parameters.ENCRYPTION_KEY_AVAILABLE,this.CIPHER,this.ENCRYPTION_KEY.toString('hex'), Buffer.from(iv).toString('hex')],'Generating CIPHER stream')
      const decipherStream = crypto.createDecipheriv(this.controlFile.settings.encryption,this.ENCRYPTION_KEY,iv)
	  streams.push(decipherStream)
	}

	if (this.COMPRESSED_INPUT) {
      streams.push(this.controlFile.settings.compression === 'GZIP' ? createGunzip() : createInflate())
	}
	
	let parser
	let transform
	
	switch (this.controlFile.settings.contentType) {
	  case 'CSV':
	    parser = csv({headers: false})
		parser.STREAM_STATE = pipelineState
	    transform = new CSVTransform(this,tableInfo, pipelineState, this.LOGGER)
		break;
	  case 'JSON':
	    parser =  new JSONParser(this.MODE, filename, pipelineState, this.LOGGER )
	    transform = new JSONTransform(this,tableInfo, pipelineState, this.LOGGER)
	}  

    const parserStreamState = parser.STREAM_STATE
	  
    parser.once('readable',() => {
	  parserStreamState.startTime = performance.now()
	})
    streams.push(parser)  
	  
    transform.on('end',() => {
	  parserStreamState.endTime = performance.now()
	}).on('error',(err) => {
	  parserStreamState.endTime = performance.now()
	  pipelineState.failed = true;
	  pipelineState.errorSource = pipelineState.errorSource || DBIConstants.PARSER_STREAM_ID
      parserStreamState.pipelineError = err
    })

    streams.push(transform)
	
	// console.log(streams.map((s) => { return s.constructor.name }).join(' ==> '))
	return streams;
  }
 
  async generateStatementCache() {

	this.statementCache = {}
    Object.keys(this.metadata).forEach((table,idx) => {
      const tableMetadata = this.metadata[table];
	  this.statementCache[tableMetadata.tableName] = {
 	    tableName         : table
	  , _SPATIAL_FORMAT   : this.INBOUND_SPATIAL_FORMAT
	  , _BATCH_SIZE       : this.BATCH_SIZE
      , insertMode        : this.OUTPUT_FORMAT
      , columnNames       : [... tableMetadata.columnNames]
      , targetDataTypes   : [... tableMetadata.dataTypes]
	  , dml               : null
	  , ddl               : null
      }
    })
	this.emit(YadamuConstants.CACHE_LOADED)
    return this.statementCache
  }

  async executeDDL(ddl) {
	this.ddl = ddl
    return [true]
  }
  
  parseJSON(fileContents) {
    return JSON.parse(fileContents)
  }  
  
  classFactory(yadamu) {
	return new LoaderDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async cloneSettings() {
    await super.cloneSettings()
    this.CONTROL_FILE_PATH = this.manager.CONTROL_FILE_PATH
	this.controlFile = this.manager.controlFile
	this.statementCache = this.manager.statementCache
  }
  
  reloadControlFile() {
    if (!this.isManager()) {
      this.controlFile = this.manager.controlFile
	}	 
  }
  
  getControlFile() {
	 return this.controlFile
  }

  async getConnectionID() { /* OVERRIDE */ }
  
  async configureConnection() { /* OVERRIDE */ }
  
  async createConnectionPool() { 
  	// Enabled Method sharing with Cloud based implementations/
	this.cloudService = new CloudService()
  }
  
  async getConnectionFromPool() { /* OVERRIDE */ }
  
  async closeConnection() { /* OVERRIDE */ }
  
  async closePool() { /* OVERRIDE */ }
	    
  async compareInputStreams(filename) {
         
    const streams = []
    const is = await this.getInputStream(filename,DBIConstants.PIPELINE_STATE);
    streams.push(is)
      
    if (this.ENCRYPTED_INPUT) {
      const iv = await this.loadInitializationVector(filename)
      streams.push(new IVReader(this.IV_LENGTH))
      // this.LOGGER.trace([this.constructor.name,'DECRYPTION',this.yadamu.parameters.ENCRYPTION_KEY_AVAILABLE,this.parameters.ENCRYPTION_KEY_AVAILABLE,this.CIPHER,this.ENCRYPTION_KEY.toString('hex'), Buffer.from(iv).toString('hex')],'Generating CIPHER stream')
      const decipherStream = crypto.createDecipheriv(this.controlFile.yadamuOptions.encryption,this.ENCRYPTION_KEY,iv)
      streams.push(decipherStream);
    }
   
    if (this.COMPRESSED_INPUT) {
      streams.push(this.controlFile.yadamuOptions.compression === 'GZIP' ? createGunzip() : createInflate())
    }
      
    return streams
    
  }

  async truncateTable(schema,tableName) {

    const datafilePath = this.makeAbsolute(this.getDataFileName(tableName));
    await fsp.rm(datafilePath)
	
  }


}

export {LoaderDBI as default }