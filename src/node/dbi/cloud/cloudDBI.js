
import path                           from 'path';
import mime                          from 'mime-types';
import {PassThrough} from "stream"

import { 
  performance 
}                                     from 'perf_hooks';

/* Yadamu Core */                                    
							          
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import NullWritable  from '../../util/nullWritable.js';

import {
  YadamuError
}                    from '../../core/yadamuException.js';


/* Yadamu DBI */                                    
import DBIConstants  from '../base/dbiConstants.js'

import LoaderDBI     from '../loader/loaderDBI.js';

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                    from '../file/fileException.js'

import Comparitor    from './cloudCompare.js';

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
	  let baseDirectory =  this.CONNECTION_PROPERTIES.directory || ""
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
  
  set CONTROL_FILE_PATH(v)       { super.CONTROL_FILE_PATH = v }   
  get CONTROL_FILE_PATH()        { return this.makeCloudPath(super.CONTROL_FILE_PATH) } 
  get CONTROL_FILE_FOLDER()      { return this.makeCloudPath(super.CONTROL_FILE_FOLDER) }
  get METADATA_FOLDER()          { return this.makeCloudPath(super.METADATA_FOLDER) }
  get DATA_FOLDER()              { return this.makeCloudPath(super.DATA_FOLDER) }
  get IMPORT_FOLDER()            { return this.makeCloudPath(super.IMPORT_FOLDER) }
  get EXPORT_FOLDER()            { return this.makeCloudPath(super.EXPORT_FOLDER) }

  constructor(yadamu,manager,settings,parameters) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,manager,settings,parameters)
	this.COMPARITOR_CLASS = Comparitor
  }    
  
  makeCloudPath(target) {
	return target.split(path.sep).join(path.posix.sep)
  }

  
  async loadMetadataFiles(stagedDataCopy) {
    // this.LOGGER.trace([this.constructor.name,this.EXPORT_PATH],`loadMetadataFiles()`)
 	const metadata = {}
    if (this.controlFile.metadata) {
      const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
		return this.cloudService.getContentAsString(this.makeAbsolute(this.controlFile.metadata[tableName].file))
      }))
	  metdataRecords.forEach((content) =>  {
        const json = this.parseJSON(content)
        metadata[json.tableName] = json;
        // json.dataFile = this.getDataFileName(json.tableName)
        if (stagedDataCopy) {
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
	const res = this.cloudService.putObject(this.makeCloudPath(filename),content)
    this.activeWriters.add(res)
	res.then(() => {this.activeWriters.delete(res)})
    return res;
  }
  
  getURI(target) {
    return `${this.PROTOCOL}${this.STORAGE_ID}${path.posix.sep}${this.makeCloudPath(target)}`
  }
  
  async initializeImport() {
	 
    // this.LOGGER.trace([this.constructor.name],`initializeImport()`)
      	
	this.DIRECTORY = this.TARGET_DIRECTORY
    await this.cloudService.verifyBucketContainer()	
       
    // Calculate the base directory for the unload operation. The Base Directory is dervied from the target schema name specified by the TO_USER parameter

    this.setFolderPaths(this.IMPORT_FOLDER,this.parameters.TO_USER)
	this.DESCRIPTION = this.IMPORT_FOLDER
    this.LOGGER.info(['IMPORT',this.DATABASE_VENDOR],`Created directory: "${this.getURI(this.IMPORT_FOLDER)}"`);
    
    const dataFileList = {}
    const metadataFileList = {}
    this.createControlFile(metadataFileList,dataFileList)
    
  }
  
  getFileOutputStream(tableName,pipelineState) {
    // this.LOGGER.trace([this.constructor.name,this.DATABASE_VENDOR,tableName],`Creating readable stream on getFileOutputStream(${this.getDataFileName(tableName)})`)
	const file = this.makeAbsolute(this.getDataFileName(tableName))
	const extension = path.extname(file);
	const contentType = mime.lookup(extension) || 'application/octet-stream'
	const outputStream = this.cloudService.createWriteStream(file,contentType,this.activeWriters)
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
	return outputStream
  }  
  
  /*
  **
  ** Remember: Export is Reading data from an S3 Object Store - load.
  **
  */
  
  async loadControlFile() {

	// this.LOGGER.trace([this.constructor.name],`initializeExport()`)

    this.DIRECTORY = this.SOURCE_DIRECTORY
    this.setFolderPaths(this.EXPORT_FOLDER,this.parameters.FROM_USER)

    let stack
    try {
	  stack = new Error().stack
	  const fileContents = await this.cloudService.getContentAsString(this.CONTROL_FILE_PATH)
  	  this.controlFile = this.parseJSON(fileContents)
	} catch (err) {
      throw (err.notFound && err.notFound()) ? new FileNotFound(this,err,stack,this.CONTROL_FILE_PATH) : new FileError(this,err,stack,this.CONTROL_FILE_PATH)
	}
  }

  async _getInputStream(filename) {

    // this.LOGGER.trace([this.constructor.name,this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating readable stream on ${this.getDataFileName(tableName)}`)
    const stream = await this.cloudService.createReadStream(filename)
	return stream
  }
  
  async setWorkerConnection() {
    // DBI implementations that do not use a pool / connection mechansim need to overide this function. eg MSSQLSERVER
	this.cloudConnection = this.manager.cloudConnection
	this.cloudService = this.manager.cloudService
  }

  async truncateTable(schema,tableName) {

    const dataFilePath = this.makeAbsolute(this.getDataFileName(tableName));
    await this.cloudService.deleteFile(dataFilePath)
	
  }

  async createInputStream(file) {
    // this.LOGGER.trace([this.constructor.name,this.DATABASE_VENDOR,],`Creating readable stream on ${file)}`)
    const stream = await this.cloudService.createReadStream(file)
	return stream
  }
    

  async loadInitializationVector(filename) {

    let inputStream
	try {
	  inputStream = await this.createInputStream(filename)
      const iv = new Uint8Array(this.IV_LENGTH);
      let bytesRead = 0;

      for await (const chunk of inputStream) {
        const chunkBytes = chunk.subarray(0, this.IV_LENGTH - bytesRead);
        iv.set(chunkBytes, bytesRead);
        bytesRead += chunkBytes.length;

        if (bytesRead >= this.IV_LENGTH) {
          break;
        }
      }

      if (bytesRead < this.IV_LENGTH) {
        throw new Error("Unexpected end of stream while reading Initialization Vector.");
      }
      return iv;
    } catch (e) {
      const cause = new FileError(this, new Error(`Unable to load Initialization Vector.`));
      cause.cause = e;
	  console.log(cause)
      throw cause;
    } finally {
      inputStream.destroy();
    }
  }

}

export {CloudDBI as default }