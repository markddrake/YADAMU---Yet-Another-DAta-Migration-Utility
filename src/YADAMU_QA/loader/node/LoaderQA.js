"use strict" 

const fs = require('fs')
const fsp = require('fs').promises
const path = require('path')
const crypto = require('crypto');
const { pipeline } = require('stream');

const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');
const LoaderDBI = require('../../../YADAMU/loader/node/loaderDBI.js');
const JSONParser = require('../../../YADAMU/loader/node/jsonParser.js');
const YadamuTest = require('../../common/node/yadamuTest.js');
const ArrayCounter = require('./arrayCounter.js');

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
	 await fsp.rmdir(this.IMPORT_FOLDER,{recursive: true})
  }

  constructor(yadamu) {
	super(yadamu)
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
  
  calculateSortedHash(file) {
  
    const array =  YadamuLibrary.loadJSON(file,this.yadamuLogger) 
	array.sort((r1,r2) => {
	  for (const i in r1) {
        if (r1[i] < r2[i]) return -1
        if (r1[i] > r2[i]) return 1;
      }
	  return 0
    })
    return crypto.createHash('sha256').update(JSON.stringify(array)).digest('hex');
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
  
  compareFiles(sourceFile,targetFile) {
	const sourceFileSize = fs.statSync(sourceFile).size
	const targetFileSize = fs.statSync(targetFile).size
	let sourceHash = ''
	let targetHash = ''
    if (sourceFileSize === targetFileSize) {
      sourceHash = this.calculateHash(sourceFile)
	  targetHash = this.calculateHash(targetFile)
	  if (sourceHash !== targetHash) {
		sourceHash = this.calculateSortedHash(sourceFile);
		targetHash = this.calculateSortedHash(targetFile)
	  }
	}
    return [sourceFileSize,targetFileSize,sourceHash,targetHash]
  }
  
  async compareSchemas(source,target) {
	 
    const report = {
      successful : []
    , failed     : []
    }
	
	let controlFilePath = path.join(this.ROOT_FOLDER,source.schema,`${source.schema}.json`);
	let fileContents = await fsp.readFile(controlFilePath,{encoding: 'utf8'})
    const sourceControlFile = JSON.parse(fileContents)

	controlFilePath = path.join(this.ROOT_FOLDER,target.schema,`${target.schema}.json`);
	fileContents = await fsp.readFile(controlFilePath,{encoding: 'utf8'})
    const targetControlFile = JSON.parse(fileContents)

    let results = Object.keys(sourceControlFile.data).map((tableName) => {return this.compareFiles( sourceControlFile.data[tableName].file, targetControlFile.data[tableName].file)})
    results = await Promise.all(results.map(async(result) => { return await Promise.all(result)}))
	
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
  
   async getInputStreams(tableInfo) {
    
	const streams = []
    const is = await this.getInputStream(tableInfo);
	streams.push(is)
	
	if (this.ENCRYPTED_INPUT) {
	  const iv = await this.loadInitializationVector(tableInfo)
	  streams.push(new IVReader(this.IV_LENGTH))
  	  // console.log('Decipher',this.controlFile.data[tableInfo.TABLE_NAME].file,this.controlFile.yadamuOptions.encryption,this.yadamu.ENCRYPTION_KEY,iv);
	  const decipherStream = crypto.createDecipheriv(this.controlFile.yadamuOptions.encryption,this.yadamu.ENCRYPTION_KEY,iv)
	  streams.push(decipherStream);
	}

	if (this.COMPRESSED_INPUT) {
      streams.push(this.controlFile.yadamuOptions.compression === 'GZIP' ? createGunzip() : createInflate())
	}
	
	const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, this.controlFile.data[tableInfo.TABLE_NAME].file)
	streams.push(jsonParser);
	return streams
  }
  
  async getRowCount(tableInfo) {
    
	const streams = await this.getInputStreams(tableInfo)
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

	const controlFilePath = path.join(this.ROOT_FOLDER,target.schema,`${target.schema}.json`);
	const fileContents = await fsp.readFile(controlFilePath,{encoding: 'utf8'})
    this.controlFile = JSON.parse(fileContents)
    const counts = await Promise.all(Object.keys(this.controlFile.data).map((k) => {
	  return this.getRowCount({TABLE_NAME:k})
	}))
	
    return Object.keys(this.controlFile.data).map((k,i) => {
	  return [target.schema,k,counts[i]]
    })	
  }       
  
}
module.exports = LoaderQA