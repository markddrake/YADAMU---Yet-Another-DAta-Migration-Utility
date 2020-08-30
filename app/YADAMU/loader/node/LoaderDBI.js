"use strict" 

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const { performance } = require('perf_hooks');

const { createGzip, createGunzip, createDeflate, createInflate } = require('zlib');

const YadamuDBI = require('../../common/YadamuDBI.js');
const JSONParser = require('./jsonParser.js');
const EventStream = require('./eventStream.js');
const JSONWriter = require('./jsonWriter.js');
const ArrayWriterWriter = require('./arrayWriter.js');
const CSVWriter = require('./csvWriter.js');

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
  
  get JSON_OUTPUT()    { return this.OUTPUT_FORMAT === 'JSON' }
  get ARRAY_OUTPUT()   { return this.OUTPUT_FORMAT === 'ARRAY' }
  get CSV_OUTPUT()     { return this.OUTPUT_FORMAT === 'CSV'  }
    
  get DATABASE_VENDOR()     { return 'LOADER' };
  get SOFTWARE_VENDOR()     { return 'YABASC'};
  get EXPORT_FOLDER()       { return this.parameters.FILE ? path.join(path.dirname(this.parameters.FILE),path.basename(this.parameters.FILE,path.extname(this.parameters.FILE))) : undefined }
  get EXPORT_PATH()         { return (this._EXPORT_PATH || this.EXPORT_FOLDER || `/yadamu/${new Date().toISOString().replace(/:/g,'.')}`)}  
  
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

  get COMPRESSED_OUTPUT()  { return this.yadamu.COMPRESSION !== 'NONE' }
  
  constructor(yadamu,exportFilePath) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,exportFilePath)
	this._EXPORT_PATH = exportFilePath
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
        TABLE_NAME            : tableName
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

  async writeMetadata(metadata) {
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)

    const metadataFileList = {}
    const metadatObject = await Promise.all(Object.values(this.metadata).map((tableMetadata) => {
	   const file = `${path.join(this.metadataFolderPath,tableMetadata.tableName)}.json`
       metadataFileList[tableMetadata.tableName] = {file: file}
       return fsp.writeFile( metadataFileList[tableMetadata.tableName].file,JSON.stringify(tableMetadata))
    }))
    const dataFileList = {}
    Object.values(this.metadata).forEach((tableMetadata) =>  {
	  let filename = `${path.join(this.dataFolderPath,tableMetadata.tableName)}.${this.OUTPUT_FORMAT.toLowerCase()}`
	  filename = this.COMPRESSED_OUTPUT ? `${filename}.gz` : filename
      dataFileList[tableMetadata.tableName] = {file: filename}
    })
	this.controlFile = { systemInformation : this.systemInformation, metadata : metadataFileList, data: dataFileList}
	await fsp.writeFile(this.controlFilePath,JSON.stringify(this.controlFile))
  }

  async setMetadata(metadata) {
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
    Object.values(metadata).forEach((table) => {delete table.source})
	super.setMetadata(metadata)
    await this.writeMetadata(metadata)
  }
  
  async initializeImport() {
	 
	// this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
    
    // Calculate the base directory for the unload operation. The Base Directory is dervied from the target schema name specified by the TO_USER parameter

	this.uploadFolder = path.resolve(path.join(this.EXPORT_PATH,this.parameters.TO_USER))
	this.controlFilePath = `${path.join(this.uploadFolder,this.parameters.TO_USER)}.json`
    this.metadataFolderPath = path.join(this.uploadFolder,'metadata')
    this.dataFolderPath = path.join(this.uploadFolder,'data')
    
    // Create the Upload, Metadata and Data folders
	await fsp.mkdir(this.uploadFolder, { recursive: true });
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
    const streams = []
	
	const writer = this.getOutputStream(tableName,ddlComplete)
	streams.push(writer)
	
	if (this.COMPRESSED_OUTPUT) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGzip() : createDeflate())
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
	this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
    this.uploadFolder = path.resolve(path.join(this.EXPORT_PATH,this.parameters.FROM_USER))
    this.controlFilePath = `${path.join(this.uploadFolder,this.parameters.FROM_USER)}.json`
	this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using control file "${this.controlFilePath}"`);
    const fileContents = await fsp.readFile(this.controlFilePath,{encoding: 'utf8'})
    this.controlFile = JSON.parse(fileContents)
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
    const stream = fs.createReadStream(this.controlFile.data[tableInfo.TABLE_NAME].file);
    await new Promise((resolve,reject) => {stream.on('open',() => {resolve(stream)}).on('error',(err) => {reject(err)})})
    return stream
  }

  async getInputStreams(tableInfo) {
    
	const streams = []
	const is = await this.getInputStream(tableInfo);
	is.once('readable',() => {
	  this.INPUT_TIMINGS.readerStartTime = performance.now()
	}).on('error',(err) => { 
      this.INPUT_TIMINGS.readerEndTime = performance.now()
	  this.INPUT_TIMINGS.readerError = err
	  this.INPUT_TIMINGS.failed = true
    }).on('end',() => {
      this.INPUT_TIMINGS.readerEndTime = performance.now()
    })
	streams.push(is)
	
	if (this.COMPRESSED_OUTPUT) {
      streams.push(this.yadamu.COMPRESSION === 'GZIP' ? createGunzip() : createInflate())
	}
	
	const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, this.controlFile.data[tableInfo.TABLE_NAME].file)
	jsonParser.once('readable',() => {
	  this.INPUT_TIMINGS.parserStartTime = performance.now()
	}).on('error',(err) => { 
      this.INPUT_TIMINGS.parserEndTime = performance.now()
	  this.INPUT_TIMINGS.parserError = err
	  this.INPUT_TIMINGS.failed = true
    })
	streams.push(jsonParser);
	
	const eventStream = new EventStream(tableInfo,this.yadamuLogger)
	eventStream.once('readable',() => {
	  this.INPUT_TIMINGS.parserStartTime = performance.now()
	}).on('end',() => {
	  this.INPUT_TIMINGS.parserEndTime = performance.now()
	  this.INPUT_TIMINGS.rowsRead = eventStream.getRowCount()
	}).on('error',(err) => {
	  this.INPUT_TIMINGS.parserEndTime = performance.now()
	  this.INPUT_TIMINGS.rowsRead = eventStream.getRowCount()
	  this.INPUT_TIMINGS.parserError = err
	  this.INPUT_TIMINGS.failed = true;
	})
	streams.push(eventStream)
	return streams;
  }
 

  generateStatementCache() {
	this.statementCache = {}
  }
  
  reloadStatementCache() {
    if (!this.isManager()) {
      this.controlFile = this.manager.controlFile
	}	 
  }
  
  cloneManager(dbi) {
	super.cloneManager(dbi)
	dbi.controlFile = this.controlFile
  }
  
  classFactory(yadamu) {
	return new LoaderDBI(yadamu)
  }
  
  async getConnectionID() { /* OVERRIDE */ }
  
  createConnectionPool() { /* OVERRIDE */ }
  
  getConnectionFromPool() { /* OVERRIDE */ }
  
  configureConnection() { /* OVERRIDE */ }
  
  closeConnection() { /* OVERRIDE */ }

  closePool() { /* OVERRIDE */ }

}

module.exports = LoaderDBI
