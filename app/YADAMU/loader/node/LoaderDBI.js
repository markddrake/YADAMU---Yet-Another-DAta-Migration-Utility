"use strict" 

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');

const YadamuDBI = require('../../common/YadamuDBI.js');
const JSONParser = require('./jsonParser.js');
const DatafileParser = require('./datafileParser.js');
const FileWriter = require('../../file/node/fileWriter.js');
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
  get CSV_OUTPUT()     { return this.OUTPUT_FORMAT === 'CSV'  }
    
  get DATABASE_VENDOR()     { return 'LOADER' };
  get SOFTWARE_VENDOR()     { return 'YABASC'};
  get EXPORT_FOLDER()       { return this.parameters.FILE ? path.join(path.dirname(this.parameters.FILE),path.basename(this.parameters.FILE,path.extname(this.parameters.FILE))) : undefined }
  get EXPORT_PATH()         { return (this._EXPORT_PATH || this.EXPORT_FOLDER || `/yadamu/${new Date().toISOString().replace(/:/g,'.')}`)}  
  
  get WRITER() {
	return this.JSON_OUTPUT ? FileWriter : CSVWriter
  }
  
  get OUTPUT_FORMAT() { 
    this._OUTPUT_FORMAT = this._OUTPUT_FORMAT || ((this.parameters.OUTPUT_FORMAT  && this.parameters.OUTPUT_FORMAT.toUpperCase() === 'CSV') ? 'CSV' : 'JSON')
    return this._OUTPUT_FORMAT
  }
  
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
      dataFileList[tableMetadata.tableName] = {file: `${path.join(this.dataFolderPath,tableMetadata.tableName)}.${this.OUTPUT_FORMAT.toLowerCase()}`}
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
    
	this.yadamuLogger.info(['Import',this.DATABASE_VENDOR],`Using control file "${this.controlFilePath}"`);

  }

  getOutputStream(tableName,ddlComplete) {
	// this.yadamuLogger.trace([this.constructor.name],`getOutputStream()`)
	const os = new this.WRITER(this,tableName,ddlComplete,this.status,this.yadamuLogger)  
    return os;
  }
  
  getFileOutputStream(tableName) {
    // this.yadamuLogger.trace([this.constructor.name],`getFileOutputStream(${this.dataFileList[tableName].file})`)
  	return fs.createWriteStream(this.controlFile.data[tableName].file)
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

  createParser(tableInfo,objectMode) {
    const datafileParser = new DatafileParser(tableInfo,this.yadamuLogger,this.MODE)
	return datafileParser
  }

  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating input stream on ${this.controlFile.data[tableInfo.TABLE_NAME].file}`)
    const stream = fs.createReadStream(this.controlFile.data[tableInfo.TABLE_NAME].file);
    await new Promise((resolve,reject) => {stream.on('open',() => {resolve(stream)}).on('error',(err) => {reject(err)})})
    const jsonParser  = new JSONParser(this.yadamuLogger,this.MODE,this.controlFile.data[tableInfo.TABLE_NAME].file);
    return stream.pipe(jsonParser)
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
  
  async getConnectionID() {}
  
  createConnectionPool() {}
  
  getConnectionFromPool() {}
  
  configureConnection() {}
  
  closeConnection() {}

  closePool() {}

}

module.exports = LoaderDBI
