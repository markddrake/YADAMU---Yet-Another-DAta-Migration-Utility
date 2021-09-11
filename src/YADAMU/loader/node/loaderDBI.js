"use strict" 

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const { performance } = require('perf_hooks');
const crypto = require('crypto');
const { PassThrough, finished} = require('stream')

const { createGzip, createGunzip, createDeflate, createInflate } = require('zlib');

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')

const LoaderConstants = require('./loaderConstants.js');
const JSONParser = require('./jsonParser.js');
const EventStream = require('./eventStream.js');
const JSONWriter = require('./jsonWriter.js');
const ArrayWriter = require('./arrayWriter.js');
const CSVWriter = require('./csvWriter.js');
const CSVTransform = require('./csvTransform.js');
const {YadamuError, CommandLineError} = require('../../common/yadamuException.js');
const {FileError, FileNotFound, DirectoryNotFound} = require('../../file/node/fileException.js');

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

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return LoaderDBI.YADAMU_DBI_PARAMETERS
  }
    
  get DATABASE_KEY()               { return LoaderDBI.DATABASE_KEY };
  get DATABASE_VENDOR()            { return LoaderDBI.DATABASE_VENDOR };
  get SOFTWARE_VENDOR()            { return LoaderDBI.SOFTWARE_VENDOR };
  get DATA_STAGING_SUPPORTED()     { return true } 
  get PARTITION_LEVEL_OPERATIONS() { return true }
  get PROTOCOL()                   { return LoaderDBI.PROTOCOL };
  
  get YADAMU_DBI_PARAMETERS()    { 
	this._YADAMU_DBI_PARAMETERS = this._YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},super.YADAMU_DBI_PARAMETERS,{}))
	return this._YADAMU_DBI_PARAMETERS
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
	  let baseDirectory =  this.vendorProperties.directory || ""
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
  
  get IMPORT_FOLDER()            { return this._IMPORT_FOLDER       || (() => {this._IMPORT_FOLDER = path.join(this.BASE_DIRECTORY,this.parameters.TO_USER); return this._IMPORT_FOLDER})() }
  get EXPORT_FOLDER()            { return this._EXPORT_FOLDER       || (() => {this._EXPORT_FOLDER = path.join(this.BASE_DIRECTORY,this.parameters.FROM_USER); return this._EXPORT_FOLDER})() }
  
  
  get OUTPUT_FORMAT() { 
    this._OUTPUT_FORMAT = this._OUTPUT_FORMAT || (() => {
	  switch (this.controlFile?.settings.contentType || this.parameters.OUTPUT_FORMAT?.toUpperCase() || 'JSON') {
        case 'ARRAY':
          this._FILE_EXTENSION = 'data'
          this._OUTPUT_FORMAT = 'ARRAY'
		  this._WRITER = ArrayWriter
          break;
        case 'CSV':
          this._FILE_EXTENSION = 'csv'
          this._OUTPUT_FORMAT = 'CSV'
		  this._WRITER = CSVWriter
          break;
        case JSON:
        default:
          this._FILE_EXTENSION = 'json'
          this._OUTPUT_FORMAT = 'JSON'
		  this._WRITER = JSONWriter
	  }
	  return this._OUTPUT_FORMAT
	})();
    return this._OUTPUT_FORMAT
  }

  get WRITER() {
    this._WRITER = this._WRITER || (() => { 
	  // Referencing _OUTPUT_FORMAT sets _WRITER
	  const outputformat = this.OUTPUT_FORMAT; 
	  return this._WRITER
	})();
	return this._WRITER
  }
  
  get FILE_EXTENSION() {
    this._FILE_EXTENSION = this._FILE_EXTENSION || (() => { 
	  // Referencing _OUTPUT_FORMAT sets _FILE_EXTENSION
	  const outputformat = this.OUTPUT_FORMAT; 
	  return this._FILE_EXTENSION
	})();
	return this._FILE_EXTENSION
  }

  get COMPRESSED_CONTENT()     { return (this.COMPRESSION_FORMAT !== 'NONE') }
  get ENCRYPTED_CONTENT()      { return this.yadamu.ENCRYPTION && this.yadamu.ENCRYPTION !== 'NONE' }

  get COMPRESSED_INPUT()       { return this.controlFile.settings.compression !== "NONE" }
  get COMPRESSION_FORMAT()     { return this.controlFile.settings.compression }
  get ENCRYPTED_INPUT()        { return this.controlFile.settings.encryption !== "NONE" }
  
  set INITIALIZATION_VECTOR(v) { this._INITIALIZATION_VECTOR =  v }
  get INITIALIZATION_VECTOR()  { return this._INITIALIZATION_VECTOR }
  get IV_LENGTH()              { return 16 }  
  
  
  constructor(yadamu,settings,parameters) {
    super(yadamu,settings,parameters)
	this.yadamuProperties = {}
	this.writeOperations = new Set()
	this.baseDirectory = path.resolve(this.vendorProperties.directory || "")
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
  
  updateVendorProperties(vendorProperties) {

  // vendorProperties.directory   = this.parameters.DIRECTORY || vendorProperties.directory 
	
  }
  
  isDatabase() {
    return true;
  }
  
  async getSystemInformation() {
    // this.yadamuLogger.trace([this.constructor.name,this.exportFilePath],`getSystemInformation()`)     	
	return this.controlFile.systemInformation
  }

  makeRelative(target) {
	return path.join(this.EXPORT_FOLDER,path.relative(this.controlFile.settings.baseFolder,target))
  }
  
  getDataFileName(tableName,partitionNumber) {
	 
	 return Array.isArray(this.controlFile.data[tableName].files) ?  this.controlFile.data[tableName].files.shift() : this.controlFile.data[tableName].file 
	 
  }

  async loadMetadataFiles(copyStagedData) {
  	this.metadata = {}
    if (this.controlFile) {
	  let stack
      let metadataPath
      try {
        stack = new Error().stack;
        const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
		  metadataPath = this.makeAbsolute(this.controlFile.metadata[tableName].file)
   	      return fsp.readFile(metadataPath,{encoding: 'utf8'})
	    }))
        metdataRecords.forEach((content) =>  {
          const json = JSON.parse(content)
	 	  this.metadata[json.tableName] = json;
          if (copyStagedData) {
            json.dataFile = this.controlFile.data[json.tableName].files || this.controlFile.data[json.tableName].file 
		  }
        })
      } catch (err) {
        throw err.code === 'ENOENT' ? new FileNotFound(err,stack,metadataPath) : new FileError(err,stack,metadataPath)
	  }
    }
    return this.metadata;      
  }

  generateMetadata() {
	return this.metadata
  }

  async getSchemaInfo() {

    // this.yadamuLogger.trace([this.constructor.name,this.EXPORT_PATH],`getSchemaInfo()`)
	
	this.metadata = await this.loadMetadataFiles(false)
	
	const schemaInformation = Object.keys(this.metadata).flatMap((tableName) => {
	  const tableInfo =  {
		TABLE_SCHEMA          : this.metadata[tableName].tableSchema
	  , TABLE_NAME            : tableName
      , DATA_TYPE_ARRAY       : this.metadata[tableName].dataTypes
	  , SPATIAL_FORMAT        : this.systemInformation.typeMappings.spatialFormat 
	  } 
      if (this.yadamu.PARALLEL_ENABLED && this.PARTITION_LEVEL_OPERATIONS && Array.isArray(this.controlFile.data[tableName].files)) {
	    const partitionInfo = this.controlFile.data[tableName].files.map((fileName,idx) => { return Object.assign({}, tableInfo, { PARTITION_COUNT: this.controlFile.data[tableName].files.length, partitionInfo : {PARTITION_NUMBER: idx+1 }})})
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

  async createControlFile(metadataFileList,dataFileList) {
  	const settings = {
	  contentType        : this.OUTPUT_FORMAT
    , compression        : this.yadamu.COMPRESSION
	, encryption         : this.ENCRYPTED_CONTENT ? this.yadamu.CIPHER : 'NONE'
	, baseFolder         : this.IMPORT_FOLDER
	, timestampPrecision : this.TIMESTAMP_PRECISION
    }
	this.controlFile = { settings : settings, systemInformation : {}, metadata : metadataFileList, data: dataFileList}  
  }


  metadataRelativePath(tableName) {
     let filename = `${tableName}.json`
     return path.relative(this.CONTROL_FILE_FOLDER,path.join(this.METADATA_FOLDER,filename))
  }
  
  dataRelativePath(tableName) {
     let filename = `${tableName}.${this.FILE_EXTENSION}`
	 filename = this.COMPRESSED_CONTENT ? `${filename}.gz` : filename
	 return path.relative(this.CONTROL_FILE_FOLDER,path.join(this.DATA_FOLDER,filename))
  }
  
  makeAbsolute(relativePath) {
    return path.resolve(this.CONTROL_FILE_FOLDER,relativePath)
  }
  
  writeFile(filename,content) {
	return fsp.writeFile(filename,JSON.stringify(content))
  }
  
  async writeMetadata(metadata) {
    
    // this.yadamuLogger.trace([this.constructor.name],`writeMetadata()`)
    
    Object.values(metadata).forEach((table) => {delete table.source})
    this.controlFile.systemInformation = this.systemInformation
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
      throw err.code === 'ENOENT' ? new DirectoryNotFound(err,stack,this.CONTROL_FILE_PATH) : new FileError(err,stack,this.CONTROL_FILE_PATH)
	}
	
    let file
	try {
      const results = await Promise.all(Object.values(this.metadata).map((tableMetadata,idx) => {
	    stack = new Error().stack;
   	    file = this.makeAbsolute(this.controlFile.metadata[tableMetadata.tableName].file)
        return this.writeFile(file,tableMetadata)   
      }))
	} catch (err) {
      throw err.code === 'ENOENT' ? new DirectoryNotFound(err,stack,file) : new FileError(err,stack,file)
	}
	this.yadamuLogger.info(['IMPORT',this.DATABASE_VENDOR],`Created Control File: "${this.getURI(this.CONTROL_FILE_PATH)}"`);
  }

  async setMetadata(metadata) {

    // this.yadamuLogger.trace([this.constructor.name,this.getWorkerNumber()],`setMetadata()`)

    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)
  }
  
  setFolderPaths(controlFileFolder,schema) {  
	this.CONTROL_FILE_PATH  = `${path.join(controlFileFolder,schema)}.json`
	this.setDescription(this.CONTROL_FILE_FOLDER)
  }      
  
  async initializeImport() {
	 
	// this.yadamuLogger.trace([this.constructor.name,this.getWorkerNumber()],`initializeImport()`)
    
    // Must set DIRECTORY before referencing EXPORT_FOLDER
    // Calculate the base directory for the unload operation. The Base Directory is dervied from the target schema name specified by the TO_USER parameter

    this.DIRECTORY = this.TARGET_DIRECTORY
	this.setFolderPaths(this.IMPORT_FOLDER,this.parameters.TO_USER)
	
    // Create the Upload, Metadata and Data folders
	await fsp.mkdir(this.IMPORT_FOLDER, { recursive: true });
    await fsp.mkdir(this.METADATA_FOLDER, { recursive: true });
    await fsp.mkdir(this.DATA_FOLDER, { recursive: true });
    
	this.yadamuLogger.info(['Import',this.DATABASE_VENDOR],`Created directory: "${this.PROTOCOL}${this.resolve(this.IMPORT_FOLDER)}"`);
    const dataFileList = {}
    const metadataFileList = {}
    this.createControlFile(metadataFileList,dataFileList)
  }

  getOutputStream(tableName,ddlComplete) {
	// this.yadamuLogger.trace([this.constructor.name],`getOutputStream()`)
	const os = new this.WRITER(this,tableName,ddlComplete,this.status,this.yadamuLogger)  
    return os;
  }
  
  getFileOutputStream(tableName) {
     // this.yadamuLogger.trace([this.constructor.name],`getFileOutputStream(${this.controlFile.data[tableName].file})`)
    const os = fs.createWriteStream(this.makeAbsolute(this.getDataFileName(tableName)))
    const opComplete = new Promise((resolve,reject) => {
	  finished(os,(err) => {
	    this.writeOperations.delete(opComplete)	
		if (err) {reject(err)} 
		resolve()
      })
    })
    this.writeOperations.add(opComplete)	
	return os;
  }

  async createInitializationVector() {

	return await new Promise((resolve,reject) => {
      crypto.randomFill(new Uint8Array(this.IV_LENGTH), (err, iv) => {
		if (err) reject(err)
	    resolve(iv);
      })
	})	    
  } 
 
  async getOutputStreams(tableName,ddlComplete) {
	await ddlComplete;
	this.reloadStatementCache()
	const streams = []
	
	const writer = this.getOutputStream(tableName,ddlComplete)
	streams.push(writer)
	
	if (this.COMPRESSED_CONTENT) {
      streams.push(this.COMPRESSION_FORMAT === 'GZIP' ? createGzip() : createDeflate())
    }
	
	if (this.ENCRYPTED_CONTENT) {
	  const iv = await this.createInitializationVector()
	  // console.log('Cipher',this.getDataFileName(tableName),this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,iv);
	  const cipherStream = crypto.createCipheriv(this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,iv)
	  streams.push(cipherStream)
	  streams.push(new IVWriter(iv))
	}
	  
	streams.push(this.getFileOutputStream(tableName))

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
    // Calculate the base directory for the load operation. The Base Directory is dervied from the schema name specified by the FROM_USER parameter

    this.DIRECTORY = this.SOURCE_DIRECTORY
    this.setFolderPaths(this.EXPORT_FOLDER,this.parameters.FROM_USER)

	let stack
	try {
	  stack = new Error().stack;
      const fileContents = await fsp.readFile(this.CONTROL_FILE_PATH,{encoding: 'utf8'})
      this.controlFile = JSON.parse(fileContents)
	} catch (err) {
      throw err.code === 'ENOENT' ? new FileNotFound(err,stack,this.CONTROL_FILE_PATH) : new FileError(err,stack,this.CONTROL_FILE_PATH)
	}
  }
  
  async initializeExport() {

	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	
	await this.loadControlFile()
    if ((this.MODE != 'DDL_ONLY') && (this.controlFile.settings.contentType === 'CSV')) {
      throw new YadamuError('Loading of "CSV" data sets not supported')
    }
	this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using Control File: "${this.PROTOCOL}${this.resolve(this.CONTROL_FILE_PATH)}"`);

  }

  async getInputStream(filename) {
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating input stream on ${filename}`)
    const stream = fs.createReadStream(filename);
    const stack = new Error().stack;
    await new Promise((resolve,reject) => {
	  stream.on('open',() => {resolve(stream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(err,stack,filename) : new FileError(err,stack,filename))})
	})
    return stream
  }

  async loadInitializationVector(filename) {
	const fd = await fsp.open(filename)
	const iv = new Uint8Array(this.IV_LENGTH)
	const results = await fd.read(iv,0,this.IV_LENGTH,0)
	await fd.close();
	return iv;
  }	
  
  getCSVParser() {
    throw new YadamuError('Loading of "CSV" data sets not supported')
  }
  
  async getInputStreams(tableInfo) {
	const streams = []
	const filename = this.makeAbsolute(this.getDataFileName(tableInfo.TABLE_NAME))

    this.INPUT_METRICS = DBIConstants.NEW_TIMINGS
	this.INPUT_METRICS.DATABASE_VENDOR = this.DATABASE_VENDOR

	const is = await this.getInputStream(filename);
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
	
	if (this.ENCRYPTED_INPUT) {
	  const iv = await this.loadInitializationVector(filename)
	  streams.push(new IVReader(this.IV_LENGTH))
  	  // console.log('Decipher',filename,this.controlFile.settings.encryption,this.yadamu.ENCRYPTION_KEY,iv);
	  const decipherStream = crypto.createDecipheriv(this.controlFile.settings.encryption,this.yadamu.ENCRYPTION_KEY,iv)
	  streams.push(decipherStream);
	}

	if (this.COMPRESSED_INPUT) {
      streams.push(this.controlFile.settings.compression === 'GZIP' ? createGunzip() : createInflate())
	}
	
	if (this.controlFile.settings.contentType === 'CSV') {
      const csvParser = this.getCSVParser()
  	  csvParser.once('readable',() => {
	    this.INPUT_METRICS.parserStartTime = performance.now()
	  }).on('error',(err) => { 
        this.INPUT_METRICS.parserEndTime = performance.now()
	    this.INPUT_METRICS.parserError = err
	    this.INPUT_METRICS.failed = true
      })
      streams.push(csvParser);  
	  const csvTransform = new CSVTransform(tableInfo,this.yadamuLogger).on('end',() => {
	    this.INPUT_METRICS.parserEndTime = performance.now()
	    this.INPUT_METRICS.rowsRead = csvTransform.getRowCount()
        this.INPUT_METRICS.lost = csvTransform.writableLength
	  })
	  streams.push(csvTransform);
	}
	else {
	  const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, filename)
  	  jsonParser.once('readable',() => {
	    this.INPUT_METRICS.parserStartTime = performance.now()
	  }).on('error',(err) => { 
        this.INPUT_METRICS.parserEndTime = performance.now()
	    this.INPUT_METRICS.parserError = err
	    this.INPUT_METRICS.failed = true
      })
	  streams.push(jsonParser);
	
      const eventStream = new EventStream(tableInfo,this.yadamuLogger)
	  eventStream.once('readable',() => {
	    this.INPUT_METRICS.parserStartTime = performance.now()
	  }).on('end',() => {
	    this.INPUT_METRICS.parserEndTime = performance.now()
	    this.INPUT_METRICS.rowsRead = eventStream.getRowCount()
        this.INPUT_METRICS.lost = eventStream.writableLength
	  }).on('error',(err) => {
	    this.INPUT_METRICS.parserEndTime = performance.now()
	    this.INPUT_METRICS.rowsRead = eventStream.getRowCount()
        this.INPUT_METRICS.lost = eventStream.writableLength
	    this.INPUT_METRICS.parserError = err
	    this.INPUT_METRICS.failed = true;
	  })
	  streams.push(eventStream)
	}
    // console.log(streams.map((s) => { return s.constructor.name }).join(' ==> '))
	return streams;
  }
 
  generateStatementCache() {

	this.statementCache = {}
	
    Object.keys(this.metadata).forEach((table,idx) => {
      const tableMetadata = this.metadata[table];
	  this.statementCache[tableMetadata.tableName] = {	tableName         : table
	  , _SPATIAL_FORMAT   : this.systemInformation.typeMappings.spatialFormat 
	  , _BATCH_SIZE       : this.BATCH_SIZE
      , columnNames       : [... tableMetadata.columnNames]
      , targetDataTypes   : [... tableMetadata.dataTypes]
      }
    })
    return this.statementCache
  }

  async executeDDL(ddl) {
	if (this.MODE !== 'DDL_ONLY') {
	  await this.writeMetadata(this.metadata)
	}
	return []
    
  }
  
  classFactory(yadamu) {
	return new LoaderDBI(yadamu)
  }
  
  async cloneCurrentSettings(manager) {
    super.cloneCurrentSettings(manager)
    this.CONTROL_FILE_PATH = this.manager.CONTROL_FILE_PATH
	this.controlFile = manager.controlFile
	this.statementCache = manager.statementCache
  }
  
  reloadStatementCache() {
    if (!this.isManager()) {
      this.controlFile = this.manager.controlFile
	}	 
  }
  
  async finalize() {
	await Promise.all(Array.from(this.writeOperations));
	super.finalize()
  }
  
  getControlFile() {
	 return this.controlFile
  }

  async getConnectionID() { /* OVERRIDE */ }
  
  createConnectionPool() { /* OVERRIDE */ }
  
  getConnectionFromPool() { /* OVERRIDE */ }
  
  configureConnection() { /* OVERRIDE */ }
  
  closeConnection(options) { /* OVERRIDE */ }

  closePool(options) { /* OVERRIDE */ }

}

module.exports = LoaderDBI
