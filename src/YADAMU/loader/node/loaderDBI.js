"use strict" 

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const { performance } = require('perf_hooks');

const { createGzip, createGunzip, createDeflate, createInflate } = require('zlib');

const YadamuDBI = require('../../common/yadamuDBI.js');
const {YadamuError} = require('../../common/yadamuError.js');
const JSONParser = require('./jsonParser.js');
const EventStream = require('./eventStream.js');
const JSONWriter = require('./jsonWriter.js');
const ArrayWriterWriter = require('./arrayWriter.js');
const CSVWriter = require('./csvWriter.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const {FileError, FileNotFound, DirectoryNotFound} = require('../../file/node/fileError.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class LoaderDBI extends YadamuDBI {
 
  /*
  **
  ** Extends YadamuDBI enabling operations on local File System.
  ** 
  ** !!! Make sure your head is wrapped around the following statements before touching this code.
  **
  ** An Export operaton involves reading data from the local file systems
  ** An Import operation involves writing data to the local file system.
  **
  */
  
  get PIPELINE_OPERATION_HANGS() {return true }
  
  get JSON_OUTPUT()              { return this.OUTPUT_FORMAT === 'JSON' }
  get ARRAY_OUTPUT()             { return this.OUTPUT_FORMAT === 'ARRAY' }
  get CSV_OUTPUT()               { return this.OUTPUT_FORMAT === 'CSV'  }
    
  get DATABASE_VENDOR()          { return 'LOADER' };
  get SOFTWARE_VENDOR()          { return 'YABASC'};
  
  get ROOT_FOLDER()              { 
    return this._ROOT_FOLDER || (() => { 
      const rootFolder = this.parameters.ROOT_FOLDER || this.connectionProperties.rootFolder || '' 
	  this._ROOT_FOLDER = YadamuLibrary.macroSubstitions(rootFolder, this.yadamu.MACROS)
	  return this._ROOT_FOLDER
    })() 
  }
  
  get IMPORT_FOLDER()            { return this._IMPORT_FOLDER || (() => {this._IMPORT_FOLDER = path.join(this.ROOT_FOLDER,this.parameters.TO_USER); return this._IMPORT_FOLDER})() }
  get EXPORT_FOLDER()            { return this._EXPORT_FOLDER || (() => {this._EXPORT_FOLDER = path.join(this.ROOT_FOLDER,this.parameters.FROM_USER); return this._EXPORT_FOLDER})() }
  
  get OUTPUT_FORMAT() { 
    this._OUTPUT_FORMAT = this._OUTPUT_FORMAT || (() => {
	  switch (this.parameters.OUTPUT_FORMAT ? this.parameters.OUTPUT_FORMAT.toUpperCase() : 'JSON') {
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
	  // Referencing OUTPUT_FORAMT sets _WRITER
	  const outputformat = this.OUTPUT_FORMAT; 
	  return this._WRITER
	})();
	return this._WRITER
  }
  
  get FILE_EXTENSION() {
    this._FILE_EXTENSION = this._FILE_EXTENSION || (() => { 
	  // Referencing OUTPUT_FORAMT sets _FILE_EXTENSION
	  const outputformat = this.OUTPUT_FORMAT; 
	  return this._FILE_EXTENSION
	})();
	return this._FILE_EXTENSION
  }

  get COMPRESSION_FORMAT()  { return this.controlFile.yadamuOptions.compression }
  get COMPRESSED_CONTENT()  { return (this.COMPRESSION_FORMAT !== 'NONE') }
  
  
  constructor(yadamu,exportFilePath) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,exportFilePath)
	this._EXPORT_PATH = exportFilePath
	this.yadamuProperties = {}
  }    
  
  getConnectionProperties() {
    return {
      rootFolder       : this.parameters.ROOT_FOLDER
    }
  }

  isDatabase() {
    return true;
  }
  
  async setMetadata(metadata) {
    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)
  }

  async getSystemInformation() {
    // this.yadamuLogger.trace([this.constructor.name,this.exportFilePath],`getSystemInformation()`)     
	return this.controlFile.systemInformation
  }

  async loadMetadataFiles() {
  	const metadata = {}
    if (this.controlFile) {
      const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
        return fsp.readFile(this.controlFile.metadata[tableName].file,{encoding: 'utf8'})
      }))
      metdataRecords.forEach((content) =>  {
        const json = JSON.parse(content)
		metadata[json.tableName] = json;
      })
    }
    return metadata;      
  }

  async getSchemaInfo() {
    // this.yadamuLogger.trace([this.constructor.name,this.EXPORT_PATH],`getSchemaInfo()`)
    this.metadata = await this.loadMetadataFiles()
    return Object.keys(this.metadata).map((tableName) => {
      return {
		TABLE_SCHEMA          : this.metadata[tableName].tableSchema
      , TABLE_NAME            : tableName
      , MAPPED_TABLE_NAME     : tableName
      , INCLUDE_TABLE         : this.applyTableFilter(tableName)
      , COLUMN_NAME_ARRAY     : this.metadata[tableName].columnNames
	  , DATA_TYPE_ARRAY       : this.metadata[tableName].dataTypes
	  , SIZE_CONSTRAINT_ARRAY : this.metadata[tableName].sizeConstraints
	  , SPATIAL_FORMAT        : this.systemInformation.spatialFormat
      } 
    })
  }
   
  /*
  **
  ** Remember: Import is Writing data to the local file system - unload.
  **
  */

  async createControlFile(metadataFileList,dataFileList) {

  	const yadamuOptions = {
	  contentType : this.OUTPUT_FORMAT
    , compression : this.yadamu.COMPRESSION
    }
	this.controlFile = { yadamuOptions : yadamuOptions, systemInformation : this.systemInformation, metadata : metadataFileList, data: dataFileList}  
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
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)
    const metadataFileList = {}
    const metadatObject = await Promise.all(Object.values(this.metadata).map((tableMetadata) => {
	   const file = this.getMetadataPath(tableMetadata.tableName) 
       metadataFileList[tableMetadata.tableName] = {file: file}
       return this.writeFile(metadataFileList[tableMetadata.tableName].file,tableMetadata)   
    }))
    const dataFileList = {}
    const compressedOuput = (this.yadamu.COMPRESSION !== 'NONE')
    Object.values(this.metadata).forEach((tableMetadata) =>  {
	  let filename = `${path.join(this.dataFolderPath,tableMetadata.tableName)}.${this.FILE_EXTENSION}`
	  filename = compressedOuput ? `${filename}.gz` : filename
      dataFileList[tableMetadata.tableName] = {file: this.getDatafilePath(filename)}
    })
	this.createControlFile(metadataFileList,dataFileList)
	await this.writeFile(this.controlFilePath,this.controlFile)
  }

  async setMetadata(metadata) {
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)
	if (this.controlFile === undefined) {
      await this.writeMetadata(metadata)
	}
  }
  
  async initializeImport() {
	 
	// this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
    
    // Calculate the base directory for the unload operation. The Base Directory is dervied from the target schema name specified by the TO_USER parameter

	this.controlFilePath = `${path.join(this.IMPORT_FOLDER,this.parameters.TO_USER)}.json`
    this.metadataFolderPath = path.join(this.IMPORT_FOLDER,'metadata')
    this.dataFolderPath = path.join(this.IMPORT_FOLDER,'data')
    
    // Create the Upload, Metadata and Data folders
	await fsp.mkdir(this.IMPORT_FOLDER, { recursive: true });
    await fsp.mkdir(this.metadataFolderPath, { recursive: true });
    await fsp.mkdir(this.dataFolderPath, { recursive: true });
    
	this.yadamuLogger.info(['Import',this.DATABASE_VENDOR],`Created control file "${this.controlFilePath}"`);

  }

  getOutputStream(tableName,ddlComplete) {
	// this.yadamuLogger.trace([this.constructor.name],`getOutputStream()`)
	const os = new this.WRITER(this,tableName,ddlComplete,this.status,this.yadamuLogger)  
    return os;
  }
  
  getFileOutputStream(tableName) {
    // this.yadamuLogger.trace([this.constructor.name],`getFileOutputStream(${this.controlFile.data[tableName].file})`)
  	return fs.createWriteStream(this.controlFile.data[tableName].file)
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
	streams.push(this.getFileOutputStream(tableName))
	
	return streams;
  }
  /*
  **
  ** !!! Remember: Export is Reading data from the local file system - Load
  **
  */

  async initializeExport() {
	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
    this.controlFilePath = `${path.join(this.EXPORT_FOLDER,this.parameters.FROM_USER)}.json`
	this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using control file "${this.controlFilePath}"`);
	let stack
	try {
	  stack = new Error().stack;
      const fileContents = await fsp.readFile(this.controlFilePath,{encoding: 'utf8'})
      this.controlFile = JSON.parse(fileContents)
	  if (this.controlFile.yadamuOptions.contentType === 'CSV') {
  	    throw new YadamuError('Loading of CSV formatted data sets is not supported')
      }
	} catch (err) {
      throw err.code === 'ENOENT' ? new FileNotFound(err,stack,this.controlFilePath) : new FileError(err,stack,this.controlFilePath)
	}

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
	, _SPATIAL_FORMAT   : this.systemInformation.spatialFormat
	, _BATCH_SIZE       : this.BATCH_SIZE
    , columnNames       : [... this.metadata[tableName].columnNames]
    , targetDataTypes   : [... this.metadata[tableName].dataTypes]
    }
  }

  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating input stream on ${this.controlFile.data[tableInfo.TABLE_NAME].file}`)
	const filename = this.controlFile.data[tableInfo.TABLE_NAME].file
    const stream = fs.createReadStream(filename);
    await new Promise((resolve,reject) => {
	  stream.on('open',() => {resolve(stream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(err,stack,filename) : new FileError(err,stack,filename))})
	})
    return stream
  }

  async getInputStreams(tableInfo) {
    
	const streams = []
    this.INPUT_METRICS = DBIConstants.NEW_TIMINGS
	
	const is = await this.getInputStream(tableInfo);
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
	
	if (this.COMPRESSED_CONTENT) {
      streams.push(this.COMPRESSION_FORMAT === 'GZIP' ? createGunzip() : createInflate())
	}
	
	const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, this.controlFile.data[tableInfo.TABLE_NAME].file)
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
	}).on('error',(err) => {
	  this.INPUT_METRICS.parserEndTime = performance.now()
	  this.INPUT_METRICS.rowsRead = eventStream.getRowCount()
	  this.INPUT_METRICS.parserError = err
	  this.INPUT_METRICS.failed = true;
	})
	streams.push(eventStream)
	return streams;
  }
 
  generateStatementCache() {
	this.statementCache = {}
	return this.statementCache
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
  
  async getConnectionID() { /* OVERRIDE */ }
  
  createConnectionPool() { /* OVERRIDE */ }
  
  getConnectionFromPool() { /* OVERRIDE */ }
  
  configureConnection() { /* OVERRIDE */ }
  
  closeConnection(options) { /* OVERRIDE */ }

  closePool(options) { /* OVERRIDE */ }

}

module.exports = LoaderDBI
