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
    
  get DATABASE_KEY()             { return LoaderDBI.DATABASE_KEY };
  get DATABASE_VENDOR()          { return LoaderDBI.DATABASE_VENDOR };
  get SOFTWARE_VENDOR()          { return LoaderDBI.SOFTWARE_VENDOR };
  get DATA_STAGING_SUPPORTED()   { return true } 
  get PROTOCOL()                 { return LoaderDBI.PROTOCOL };
  
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
  
  get IMPORT_FOLDER()            { return this._IMPORT_FOLDER || (() => {this._IMPORT_FOLDER = path.join(this.BASE_DIRECTORY,this.parameters.TO_USER); return this._IMPORT_FOLDER})() }
  get EXPORT_FOLDER()            { return this._EXPORT_FOLDER || (() => {this._EXPORT_FOLDER = path.join(this.BASE_DIRECTORY,this.parameters.FROM_USER); return this._EXPORT_FOLDER})() }
  
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

  async loadMetadataFiles(copyStagedData) {
  	this.metadata = {}
    if (this.controlFile) {
      const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
        return fsp.readFile(this.controlFile.metadata[tableName].file,{encoding: 'utf8'})
      }))
      metdataRecords.forEach((content) =>  {
        const json = JSON.parse(content)
		this.metadata[json.tableName] = json;
        if (copyStagedData) {
		  json.dataFile = this.controlFile.data[json.tableName].file
		}
      })
    }
    return this.metadata;      
  }

  generateMetadata() {
	return this.metadata
  }

  async getSchemaInfo() {
    // this.yadamuLogger.trace([this.constructor.name,this.EXPORT_PATH],`getSchemaInfo()`)
	
	this.metadata = await this.loadMetadataFiles(false)
	
    return Object.keys(this.metadata).map((tableName) => {
      return {
		TABLE_SCHEMA          : this.metadata[tableName].tableSchema
	  , TABLE_NAME : tableName
      , DATA_TYPE_ARRAY       : this.metadata[tableName].dataTypes
	  , SPATIAL_FORMAT        : this.systemInformation.typeMappings.spatialFormat 
	  } 
	  /*
      return {
      , TABLE_NAME            : tableName
      , COLUMN_NAME_ARRAY     : this.metadata[tableName].columnNames
	  , SIZE_CONSTRAINT_ARRAY : this.metadata[tableName].sizeConstraints
      } 
	  */
    })

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

  getMetadataPath(tableName) {
     return `${path.join(this.metadataFolderPath,tableName)}.json`
  }
  
  getDatafilePath(filename) {
	return filename
  }
  
  writeFile(filename,content) {
    return fsp.writeFile(filename,JSON.stringify(content))
  }
  
  async writeMetadata(metadata) {
    
    // this.yadamuLogger.trace([this.constructor.name],`writeMetadata()`)
    
    Object.values(metadata).forEach((table) => {delete table.source})
    this.controlFile.systemInformation = this.systemInformation
	const compressedOuput = (this.COMPRESSED_CONTENT)
    Object.values(this.metadata).forEach((tableMetadata) => {
	   const file = this.getMetadataPath(tableMetadata.tableName) 
       this.controlFile.metadata[tableMetadata.tableName] = {file: file}
    })
    Object.values(this.metadata).forEach((tableMetadata) =>  {
	  let filename = `${path.join(this.dataFolderPath,tableMetadata.tableName)}.${this.FILE_EXTENSION}`
	  filename = compressedOuput ? `${filename}.gz` : filename
      this.controlFile.data[tableMetadata.tableName] = {file: this.getDatafilePath(filename)}
    })
	await this.writeFile(this.controlFilePath,this.controlFile)

    const results = await Promise.all(Object.values(this.metadata).map((tableMetadata,idx) => {
	   const file = this.controlFile.metadata[tableMetadata.tableName].file
       return this.writeFile(file,tableMetadata)   
    }))
	this.yadamuLogger.info(['IMPORT',this.DATABASE_VENDOR],`Created Control File: "${this.getURI(this.controlFilePath)}"`);
  }

  async setMetadata(metadata) {

    // this.yadamuLogger.trace([this.constructor.name,this.getWorkerNumber()],`setMetadata()`)

    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)
  }
  
  setFolderPaths(rootFolder,schema) {
      
	this.controlFilePath = `${path.join(rootFolder,schema)}.json`
    this.metadataFolderPath = path.join(rootFolder,'metadata')
    this.dataFolderPath = path.join(rootFolder,'data')
  }      
  
  async initializeImport() {
	 
	// this.yadamuLogger.trace([this.constructor.name,this.getWorkerNumber()],`initializeImport()`)
    
    // Calculate the base directory for the unload operation. The Base Directory is dervied from the target schema name specified by the TO_USER parameter

    this.DIRECTORY = this.TARGET_DIRECTORY
	this.setFolderPaths(this.IMPORT_FOLDER,this.parameters.TO_USER)
    this.setDescription(this.IMPORT_FOLDER)
	
    // Create the Upload, Metadata and Data folders
	await fsp.mkdir(this.IMPORT_FOLDER, { recursive: true });
    await fsp.mkdir(this.metadataFolderPath, { recursive: true });
    await fsp.mkdir(this.dataFolderPath, { recursive: true });
    
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
    const os = fs.createWriteStream(this.controlFile.data[tableName].file)
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
	  // console.log('Cipher',this.controlFile.data[tableName].file,this.yadamu.CIPHER,this.yadamu.ENCRYPTION_KEY,iv);
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

    this.DIRECTORY = this.SOURCE_DIRECTORY
	
    this.setFolderPaths(this.EXPORT_FOLDER,this.parameters.FROM_USER)
    this.setDescription(this.EXPORT_FOLDER)

	let stack

	try {
	  stack = new Error().stack;
      const fileContents = await fsp.readFile(this.controlFilePath,{encoding: 'utf8'})
      this.controlFile = JSON.parse(fileContents)
	} catch (err) {
      throw err.code === 'ENOENT' ? new FileNotFound(err,stack,this.controlFilePath) : new FileError(err,stack,this.controlFilePath)
	}
  }
  
  async initializeExport() {

	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	
	await this.loadControlFile()
    if ((this.MODE != 'DDL_ONLY') && (this.controlFile.settings.contentType === 'CSV')) {
      throw new YadamuError('Loading of "CSV" data sets not supported')
    }
	this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using Control File: "${this.PROTOCOL}${this.resolve(this.controlFilePath)}"`);

  }

  getTableInfo(tableName) {
	
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
	, _BATCH_SIZE       : this.BATCH_SIZE
    , columnNames       : [... this.metadata[tableName].columnNames]
    , targetDataTypes   : [... this.metadata[tableName].dataTypes]
    }
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
  
  async getInputStreams(tableInfo) {
	const streams = []
	const filename = this.controlFile.data[tableInfo.TABLE_NAME].file  

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
	
    // console.log(streams.map((s) => { return s.constructor.name }).join(' ==> '))
	return streams;
  }
 
  generateStatementCache() {
	this.statementCache = {}
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
	this.controlFile = manager.controlFile
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
