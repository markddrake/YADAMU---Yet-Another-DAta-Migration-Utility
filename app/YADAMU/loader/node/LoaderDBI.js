"use strict" 

const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');

const FileDBI = require('../../file/node/FileDBI.js');
const FileWriter = require('../../file/node/fileWriter.js');
const CSVWriter = require('./csvWriter.js')
const JSONParser = require('./jsonParser.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class LoaderDBI extends FileDBI {
 
  /*
  **
  ** !!! For the LoaderDBI an export operaton involves reading data from the file system and an Import operation involves writing data to the file system !!!
  **
  */
  
  get OUTPUT_FORMAT() { 
    this._OUTPUT_FORMAT = this._OUTPUT_FORMAT || (this.parameters.OUTPUT_FORMAT === 'CSV' ? 'CSV' : 'JSON')
    return this._OUTPUT_FORMAT
  }
  
  get JSON_OUTPUT()    { return this.OUTPUT_FORMAT === 'JSON' }
  get CSV_OUTPUT()     { return this.OUTPUT_FORMAT === 'CSV'  }
  
  get DATABASE_VENDOR()    { return 'LOADER' };

  constructor(yadamu,exportFilePath) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,exportFilePath)
  }    

  isDatabase() {
    return true
  }
  
  closeInputStream() {      
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
    this.yadamuLogger.trace([this.constructor.name,this.exportFilePath],`getSchemaInfo()`)
    this.metadata = await this.loadMetadataFiles()
    return Object.keys(this.metadata).map((tableName) => {
      return {
        TABLE_NAME        : tableName
      , MAPPED_TABLE_NAME : tableName
      , INCLUDE_TABLE     : this.applyTableFilter(tableName)
      , COLUMN_NAME_ARRAY : this.metadata[tableName].columnNames
	  , DATA_TYPE_ARRAY       : this.metadata[tableName].dataTypes
	  , SIZE_CONSTRAINT_ARRAY : this.metadata[tableName].sizeConstraints
      } 
    })
  }

  async writeMetadata(metadata) {
    // this.yadamuLogger.trace([this.constructor.name],`writeMetadata()`)
	
    if (this.outputStream !== undefined) {
  	  this.outputStream.write(',');
      const metadataFileList = {}
      await Promise.all(Object.values(metadata).map((tableMetadata) => {
         metadataFileList[tableMetadata.tableName] = {file: `${path.join(this.metadataFolderPath,tableMetadata.tableName)}.json`}
         return fsp.writeFile( metadataFileList[tableMetadata.tableName].file,JSON.stringify(tableMetadata))
      }))
      this.outputStream.write(`"metadata":${JSON.stringify(metadataFileList)}`);
      this.dataFileList = {}
      Object.values(metadata).map((tableMetadata) =>  {
        this.dataFileList[tableMetadata.tableName] = {file: `${path.join(this.dataFolderPath,tableMetadata.tableName)}.${this.OUTPUT_FORMAT.toLowerCase()}`}
      }) 
  	}
  }
  
  async initializeData() {
    this.outputStream.write(',');
    this.outputStream.write('"data":'); 
  }

  async finalizeData() {
    this.outputStream.write(JSON.stringify(this.dataFileList))

  } 
  
  /*
  **
  ** For LoaderDBI Import is Writing data to the file system. - Unload
  **
  */
  
  async initializeImport() {
	 
	// 
	// this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
    
    // Create the base directory for the unload operation is it does not exists. The Base Directory is dervied from the target schema name specified by the TO_USER parameter
    
	this.loaderBaseFolder = path.join(path.dirname(this.exportFilePath),path.basename(this.exportFilePath,path.extname(this.exportFilePath)),this.parameters.TO_USER)
    await fsp.mkdir(this.loaderBaseFolder, { recursive: true });
    
    // Update the exportFilePath before calling super().. The exportFilePath is TO_USER\TO_USER.json.
    this.exportFilePath = `${path.join(this.loaderBaseFolder,this.parameters.TO_USER)}.json`
    await super.initializeImport()
    
    // Create the Metadata and Data folders
    this.metadataFolderPath = path.join(this.loaderBaseFolder,'metadata');
    await fsp.mkdir(this.metadataFolderPath, { recursive: true });
    this.dataFolderPath = path.join(this.loaderBaseFolder,'data')
    await fsp.mkdir(this.dataFolderPath, { recursive: true });
  
  }
 
  /*
  **
  ** For LoaderDBI Export is Reading data from the File System.
  **
  */

  async initializeExport() {
	this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	this.loaderBaseFolder = path.join(path.dirname(this.exportFilePath),path.basename(this.exportFilePath,path.extname(this.exportFilePath)),this.parameters.FROM_USER)
    this.controlFilePath = `${path.join(this.loaderBaseFolder,this.parameters.FROM_USER)}.json`
    const fileContents = await fsp.readFile(this.controlFilePath,{encoding: 'utf8'})
    this.controlFile = JSON.parse(fileContents)
  }

  createParser(tableInfo,objectMode) {
    const jsonParser  = new JSONParser(tableInfo.TABLE_NAME,this.yadamuLogger,this.MODE);
    return jsonParser
  }

  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating input stream on ${this.controlFile.data[tableInfo.TABLE_NAME].file}`)
    const inputStream = fs.createReadStream(this.controlFile.data[tableInfo.TABLE_NAME].file);
    await new Promise((resolve,reject) => {inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})})
    return inputStream
  }
  
  getOutputStream(tableName,ddlComplete) {
	// this.yadamuLogger.trace([this.constructor.name],`getOutputStream()`)
    const Writer = this.JSON_OUTPUT ? FileWriter : CSVWriter
	const os = new Writer(this,tableName,ddlComplete,this.status,this.yadamuLogger)  
    return os;
  }
  
  getFileOutputStream(tableName) {
	return fs.createWriteStream(this.dataFileList[tableName].file)
  }

  getDatabaseConnection() {}
  
  closeConnection() {}
 
  async workerDBI(workerNumber) {
	return this
  }
}

module.exports = LoaderDBI
