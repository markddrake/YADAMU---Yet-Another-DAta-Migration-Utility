"use strict" 

const path = require('path')
const mime = require('mime-types');

const LoaderDBI = require('../node/loaderDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const {YadamuError} = require('../../../YADAMU/common/yadamuException.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class CloudDBI extends LoaderDBI {
 
  /*
  **
  ** Extends LoaderDBI enabling operations on Cloud Services. This is an Abstract Class that provides methods that 
  ** will be used by implimenting classes
  **
  */

  get DATABASE_VENDOR()     { return 'ABSTRACT_VENDOR_NAME' };
  
  get STORAGE_ID()          { return 'ABSTACT_STORAGE_ID' }
  
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
	  let baseDirectory =  this.vendorSettings.directory || ""
	  if (this.DIRECTORY) {
        if (path.isAbsolute(this.DIRECTORY)) {
	      baseDirectory = this.DIRECTORY
        }
        else {
          baseDirectory = path.join(baseDirectory,this.DIRECTORY)
		}
	  }
	  this._BASE_DIRECTORY  = YadamuLibrary.macroSubstitions(baseDirectory,this.yadamu.MACROS)
	  return this._BASE_DIRECTORY
    })()
  }
  
  constructor(yadamu,settings,parameters) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,settings,parameters)
  }    
  
  async createInitializationVector() {
    throw new YadamuError(`Encyption option not currently supported for "${this.DATABASE_VENDOR}"`);
  }	

  async loadInitializationVector(filename) {
    throw new YadamuError(`Encyption option not currently supported for "${this.DATABASE_VENDOR}"`);
  }	
   
  makeCloudPath(target) {
	return target.split(path.sep).join(path.posix.sep)
  }
  
  setConnectionProperties(connectionSettings) {
	this.vendorSettings = connectionSettings.settings
	// delete connectionSettings.settings;
	super.setConnectionProperties(connectionSettings)
  }
  
  async loadMetadataFiles(copyStagedData) {
    // this.yadamuLogger.trace([this.constructor.name,this.EXPORT_PATH],`loadMetadataFiles()`)
  	const metadata = {}
    if (this.controlFile) {
      const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
		return this.cloudService.getObject(this.makeAbsolute(this.controlFile.metadata[tableName].file))
      }))
	  metdataRecords.forEach((content) =>  {
        const json = this.parseContents(content)
        metadata[json.tableName] = json;
        // json.dataFile = this.getDataFileName(json.tableName)
        if (copyStagedData) {
          json.dataFile = this.controlFile.data[json.tableName].files || this.controlFile.data[json.tableName].file 
        }
      })
    }
    return metadata;      
  }
  
  /*
  **
  ** Remember: Import is Writing data to an S3 Object Store - unload.
  **
  */

  metadataRelativePath(tableName) {
     return this.makeCloudPath(super.metadataRelativePath(tableName))
  }
  
  dataRelativePath(filename) {
    return this.makeCloudPath(super.dataRelativePath(filename))
  }
  
  makeAbsolute(relativePath) {
	return this.makeCloudPath(path.join(this.CONTROL_FILE_FOLDER,relativePath))
  }
  
  writeFile(filename,content) {
	const res = this.cloudService.putObject(filename,content)
    this.cloudService.writeOperations.add(res)
	res.then(() => { this.cloudService.writeOperations.delete(res)})
    return res;
  }
  
  getURI(target) {
    return `${this.PROTOCOL}${this.STORAGE_ID}${path.posix.sep}${this.makeCloudPath(target)}`
  }
  
  async initializeImport() {
	 
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
      	
	this.DIRECTORY = this.TARGET_DIRECTORY
    await this.cloudService.verifyBucketContainer()	
       
    // Calculate the base directory for the unload operation. The Base Directory is dervied from the target schema name specified by the TO_USER parameter

    this.setFolderPaths(this.IMPORT_FOLDER,this.parameters.TO_USER)
	this.DESCRIPTION = this.IMPORT_FOLDER
    this.yadamuLogger.info(['IMPORT',this.DATABASE_VENDOR],`Created directory: "${this.getURI(this.IMPORT_FOLDER)}"`);
    
    const dataFileList = {}
    const metadataFileList = {}
    this.createControlFile(metadataFileList,dataFileList)
    
  }
  
  getFileOutputStream(tableName) {
    // this.yadamuLogger.trace([this.constructor.name,this.DATABASE_VENDOR,tableName],`Creating readable stream on getFileOutputStream(${this.getDataFileName(tableName)})`)
	const file = this.makeAbsolute(this.getDataFileName(tableName))
	const extension = path.extname(file);
	const contentType = mime.lookup(extension) || 'application/octet-stream'
	return this.cloudService.createWriteStream(file,contentType)
  }  
  
  /*
  **
  ** Remember: Export is Reading data from an S3 Object Store - load.
  **
  */

  async loadControlFile() {

	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)

    this.DIRECTORY = this.SOURCE_DIRECTORY
    this.setFolderPaths(this.EXPORT_FOLDER,this.parameters.FROM_USER)

	const fileContents = await this.cloudService.getObject(this.CONTROL_FILE_PATH)
	this.controlFile = this.parseContents(fileContents)
  }

  async getInputStream(filename) {

    // this.yadamuLogger.trace([this.constructor.name,this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating readable stream on ${this.getDataFileName(tableName)}`)
    const stream = await this.cloudService.createReadStream(filename)
	return stream
  }
  
  async setWorkerConnection() {
    // DBI implementations that do not use a pool / connection mechansim need to overide this function. eg MSSQLSERVER
	this.cloudConnection = this.manager.cloudConnection
	this.cloudService = this.manager.cloudService
  }
  
  classFactory(yadamu) {
	return new S3DBI(yadamu)
  }
    
}

module.exports = CloudDBI
