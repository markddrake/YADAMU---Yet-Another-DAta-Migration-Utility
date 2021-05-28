"use strict" 

const fs = require('fs')
const fsp = require('fs').promises
const path = require('path')
const crypto = require('crypto');
const { createGzip, createGunzip, createDeflate, createInflate } = require('zlib');
const { pipeline } = require('stream');

const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');
const LoaderDBI = require('../../../YADAMU/loader/node/loaderDBI.js');
const JSONParser = require('../../../YADAMU/loader/node/jsonParser.js');
const YadamuTest = require('../../common/node/yadamuTest.js');
const ArrayCounter = require('./arrayCounter.js');
const ArrayReader = require('./arrayReader.js');

class LoaderQA extends LoaderDBI {

  static #_YADAMU_DBI_PARAMETERS
	
  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,LoaderDBI.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[LoaderDBI.DATABASE_KEY] || {},{RDBMS: LoaderDBI.DATABASE_KEY}))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return LoaderQA.YADAMU_DBI_PARAMETERS
  }	
			
  async recreateSchema() {
    this.DIRECTORY = this.TARGET_DIRECTORY
    await fsp.rmdir(this.IMPORT_FOLDER,{recursive: true})
  }

  constructor(yadamu,settings,parameters) {
	super(yadamu,settings,parameters)
  }

  setMetadata(metadata) {
    super.setMetadata(metadata)
  }

  async initialize() {
	await super.initialize();
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
  }

  async getArray(filename) {
	const streams = await this.getInputStreams(filename)
	const arrayReader = new ArrayReader(this)
	streams.push(arrayReader)

	return new Promise((resolve,reject) => {
      pipeline(streams,(err) => {
		if (err) reject(err) 
		resolve(arrayReader.getArray());
	  })
	})
  }
    
  async calculateSortedHash(filename) {
    try {
    const array = await this.getArray(filename)
	array.sort((r1,r2) => {
	  for (const i in r1) {
        if (r1[i] < r2[i]) return -1
        if (r1[i] > r2[i]) return 1;
      }
	  return 0
    })
    return crypto.createHash('sha256').update(JSON.stringify(array)).digest('hex');
  } catch (e) { console.log(e) }
  } 
  
  calculateHash(file) {
	  
	return new Promise((resolve,reject) => {
	  const hash = crypto.createHash('sha256');
	  const is = fs.createReadStream(file)
	  pipeline([is,hash],(err) => {
		if (err) reject(err)
		hash.end();
		hash.setEncoding('hex');
		resolve(hash.read());
	  })
	})
  }	  
  
  async compareFiles(sourceFile,targetFile) {
	const sourceFileSize = fs.statSync(sourceFile).size
	const targetFileSize = fs.statSync(targetFile).size
	let sourceHash = ''
	let targetHash = ''
    if (sourceFileSize === targetFileSize) {
      sourceHash = this.calculateHash(sourceFile)
	  targetHash = this.calculateHash(targetFile)
	  if (sourceHash !== targetHash) {
		sourceHash = await this.calculateSortedHash(sourceFile);
		targetHash = await this.calculateSortedHash(targetFile)
	  }
	}
    return [sourceFileSize,targetFileSize,sourceHash,targetHash]
  }
  
  async compareSchemas(source,target) {
	 
    const report = {
      successful : []
    , failed     : []
    }

	this._BASE_DIRECTORY = undefined
    this.DIRECTORY = this.SOURCE_DIRECTORY
	let controlFilePath = path.join(this.BASE_DIRECTORY,source.schema,`${source.schema}.json`);
	
	let fileContents = await fsp.readFile(controlFilePath,{encoding: 'utf8'})
    const sourceControlFile = JSON.parse(fileContents)
	
	// Assume the source control file contains the correct options for both source and target
    this.controlFile = sourceControlFile
	
    this._BASE_DIRECTORY = undefined
    this.DIRECTORY = this.TARGET_DIRECTORY
	controlFilePath = path.join(this.BASE_DIRECTORY,target.schema,`${target.schema}.json`);
	
	fileContents = await fsp.readFile(controlFilePath,{encoding: 'utf8'})
    const targetControlFile = JSON.parse(fileContents)

    let results = await Promise.all(Object.keys(sourceControlFile.data).map((tableName) => {return this.compareFiles( sourceControlFile.data[tableName].file, targetControlFile.data[tableName].file)}))
	
    Object.keys(sourceControlFile.data).map((tableName,idx) => {
	  const result = results[idx]
	  if ((result[0] === result[1]) && (result[2] === result[3])) {
		report.successful.push([source.schema,target.schema,tableName,result[0]])
	  }
	  else {
		report.failed.push([source.schema,target.schema,tableName,result[0],result[1],result[2],result[3],null])
	  }
    })
	return report
  }
  
   async getInputStreams(filename) {
    
	const streams = []
    const is = await this.getInputStream(filename);
	streams.push(is)
	
	if (this.ENCRYPTED_INPUT) {
	  const iv = await this.loadInitializationVector(filename)
	  streams.push(new IVReader(this.IV_LENGTH))
  	  // console.log('Decipher',filename,this.controlFile.yadamuOptions.encryption,this.yadamu.ENCRYPTION_KEY,iv);
	  const decipherStream = crypto.createDecipheriv(this.controlFile.yadamuOptions.encryption,this.yadamu.ENCRYPTION_KEY,iv)
	  streams.push(decipherStream);
	}

	if (this.COMPRESSED_INPUT) {
      streams.push(this.controlFile.yadamuOptions.compression === 'GZIP' ? createGunzip() : createInflate())
	}
	
	const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, filename)
	streams.push(jsonParser);
	return streams
  }
  
  async getRowCount(tableInfo) {
    
	const filename = this.controlFile.data[tableInfo.TABLE_NAME].file
	const streams = await this.getInputStreams(filename)
	const arrayCounter = new ArrayCounter(this)
	streams.push(arrayCounter)

	return new Promise((resolve,reject) => {
      pipeline(streams,(err) => {
		if (err) reject(err) 
		resolve(arrayCounter.getRowCount());
	  })
	})
  }

  async getRowCounts(target) {

	this.DIRECTORY = this.TARGET_DIRECTORY
    const controlFilePath = path.resolve(`${path.join(this.BASE_DIRECTORY,target.schema,target.schema)}.json`)
	
	const fileContents = await fsp.readFile(controlFilePath,{encoding: 'utf8'})
    this.controlFile = JSON.parse(fileContents)
    const counts = await Promise.all(Object.keys(this.controlFile.data).map((k) => {
	  return this.getRowCount({TABLE_NAME:k})
	}))
	
    return Object.keys(this.controlFile.data).map((k,i) => {
	  return [target.schema,k,counts[i]]
    })	
  }    

  getYadamuOptions() {
    return this.controlFile.yadamuOptions
  }
  
  setYadamuOptions(options) {
	this.parameters.OUTPUT_FORMAT = options.contentType
	this.yadamu.parameters.COMPRESSION = options.compression
	this.yadamu.parameters.ENCRYPTION = options.encryption
  }
  
}
module.exports = LoaderQA