"use strict" 

const { BlobServiceClient } = require('@azure/storage-blob');
const path = require('path')
const Stream = require('stream')

const LoaderDBI = require('../node/LoaderDBI.js');
const AzureConstants = require('./azureConstants.js');
const AzureIO = require('./azureIO.js');
/*
**
** YADAMU Database Inteface class skeleton
**
*/

class AzureDBI extends LoaderDBI {
 
  /*
  **
  ** Extends LoaderDBI enabling operations on Amazon Web Services S3 Buckets rather than a local file system.
  ** 
  ** !!! Make sure your head is wrapped around the following statements before touching this code.
  **
  ** An Export operaton involves reading data from the S3 object store
  ** An Import operation involves writing data to the S3 object store
  **
  */

  get DATABASE_VENDOR()     { return AzureConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()     { return AzureConstants.SOFTWARE_VENDOR};
  
  get CONTAINER_NAME()      { return this.parameters.CONTAINER_NAME || AzureConstants.CONTAINER_NAME }
  
  constructor(yadamu,exportFilePath) {
    // Export File Path is a Directory for in Load/Unload Mode
	super(yadamu,exportFilePath)
	this.connectionProperties = AzureConstants.AZURITE
  }    
  
  async createConnectionPool() {
	// this.yadamuLogger.trace([this.constructor.name],`new AWS.S3()`)
    this.azure = BlobServiceClient.fromConnectionString(this.connectionProperties);
	this.ioManager = new AzureIO(this.azure,{},this.yadamuLogger)
  }
  
  async loadMetadataFiles() {
    // this.yadamuLogger.trace([this.constructor.name,this.EXPORT_PATH],`loadMetadataFiles()`)
  	const metadata = {}
    if (this.controlFile) {
      const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
		return this.ioManager.getObject(this.controlFile.metadata[tableName].file)
      }))
	  metdataRecords.forEach((content) =>  {
        const json = JSON.parse(content)
        metadata[json.tableName] = json;
      })
    }
    return metadata;      
  }
  
  /*
  **
  ** Remember: Import is Writing data to an S3 Object Store - unload.
  **
  */

  async writeMetadata() {
    const metadataFileList = {}
    const metadataObject = await Promise.all(Object.values(this.metadata).map((tableMetadata) => {
	     const file = `${path.join(this.metadataFolderPath,tableMetadata.tableName)}.json`.split(path.sep).join(path.posix.sep)
         metadataFileList[tableMetadata.tableName] = {file: file}
	     return this.ioManager.putObject(file,tableMetadata);
    }))
	const dataFileList = {}
    Object.values(this.metadata).forEach((tableMetadata) =>  {
	  let filename = `${path.join(this.dataFolderPath,tableMetadata.tableName)}.${this.FILE_EXTENSION}`
	  filename = this.COMPRESSED_OUTPUT ? `${filename}.gz` : filename
      dataFileList[tableMetadata.tableName] = {file: filename}
      dataFileList[tableMetadata.tableName] = {file: filename.split(path.sep).join(path.posix.sep)}
    })
	this.controlFile = { systemInformation : this.systemInformation, metadata : metadataFileList, data: dataFileList}
	await this.ioManager.putObject(this.controlFilePath,this.controlFile)
  }
  
  async initializeImport() {
	 
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
      	
	await this.ioManager.verifyContainer()	

	this.uploadFolder = path.join(this.EXPORT_PATH,this.parameters.TO_USER).split(path.sep).join(path.posix.sep) 
	this.controlFilePath = `${path.join(this.uploadFolder,this.parameters.TO_USER)}.json`.split(path.sep).join(path.posix.sep) 
    this.metadataFolderPath = path.join(this.uploadFolder,'metadata').split(path.sep).join(path.posix.sep) 
    this.dataFolderPath = path.join(this.uploadFolder,'data').split(path.sep).join(path.posix.sep) 
    this.yadamuLogger.info(['Import',this.DATABASE_VENDOR],`Using control file "${this.CONTAINER_NAME}/${this.controlFilePath}"`);

  }

  getFileOutputStream(tableName) {
    // this.yadamuLogger.trace([this.constructor.name,this.DATABASE_VENDOR,tableName],`Creating readable stream ongetFileOutputStream(${this.controlFile.data[tableName].file})`)
	return this.ioManager.createWriteStream(this.controlFile.data[tableName].file)
  }  /*
  **
  ** Remember: Export is Reading data from an S3 Object Store - load.
  **
  */

  async initializeExport() {
	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	this.uploadFolder = path.join(this.EXPORT_PATH,this.parameters.FROM_USER).split(path.sep).join(path.posix.sep) 
    this.controlFilePath = `${path.join(this.uploadFolder,this.parameters.FROM_USER)}.json`.split(path.sep).join(path.posix.sep) 
    this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using control file "${this.CONTAINER_NAME}/${this.controlFilePath}"`);
    const fileContents = await this.ioManager.getObject(this.controlFilePath)
    this.controlFile = JSON.parse(fileContents)
  }

  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([this.constructor.name,this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating readable stream on ${this.controlFile.data[tableInfo.TABLE_NAME].file}`)
    const stream = await this.ioManager.createReadStream(this.controlFile.data[tableInfo.TABLE_NAME].file)
	return stream
  }
  
  async setWorkerConnection() {
    // DBI implementations that do not use a pool / connection mechansim need to overide this function. eg MSSQLSERVER
	this.s3 = await this.manager.s3
	this.ioManager = this.manager.ioManager
  }
  
  classFactory(yadamu) {
	return new S3DBI(yadamu)
  }
    
}

module.exports = AzureDBI
