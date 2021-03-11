"use strict" 

const assert = require('assert');
const path = require('path')
const crypto = require('crypto');
const { pipeline } = require('stream');

const AWSS3DBI = require('../../../YADAMU//loader/awsS3/awsS3DBI.js');
const AWSS3Error = require('../../../YADAMU/loader/awsS3/awsS3Exception.js')
const AWSS3Constants = require('../../../YADAMU/loader/awsS3/awsS3Constants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class AWSS3QA extends AWSS3DBI {

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,AWSS3Constants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AWSS3Constants.DATABASE_KEY] || {},{RDBMS: AWSS3Constants.DATABASE_KEY}))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return AWSS3QA.YADAMU_DBI_PARAMETERS
  }	
	 
  async recreateSchema() {
	await this.cloudService.createBucketContainer()
	await this.cloudService.deleteFolder(this.IMPORT_FOLDER)
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

  setConnectionProperties(connectionProperties) {
    if (connectionProperties.hasOwnProperty('yadamuOptions')) {
	  Object.assign(this.s3Options,connectionProperties.yadamuOptions)
	  delete connectionProperties.yadamuOptions
	}
    super.setConnectionProperties(connectionProperties)
  }
  
  async calculateSortedHash(file) {
  
    const fileContents = await this.cloudService.getObject(file)		
    const array = this.parseContents(fileContents)

    array.sort((r1,r2) => {
	  for (const i in r1) {
        if (r1[i] < r2[i]) return -1
        if (r1[i] > r2[i]) return 1;
      }
	  return 0
    })
    return crypto.createHash('sha256').update(JSON.stringify(array)).digest('hex');
  } 
  
  async calculateHash(file) {
	  
	return new Promise(async (resolve,reject) => {
	  const hash = crypto.createHash('sha256');
	  const is = await this.cloudService.createReadStream(file)
	  pipeline([is,hash],(err) => {
		if (err) reject(err)
		hash.end();
		hash.setEncoding('hex');
		resolve(hash.read());
	  })
	})
  }	  
  
  getRowCount(file) {
	return 0
  }

  async compareFiles(sourceFile,targetFile) {
	let props = await this.cloudService.getObjectProps(sourceFile)
	const sourceFileSize = props.ContentLength
	props = await this.cloudService.getObjectProps(targetFile)
	const targetFileSize = props.ContentLength
	let sourceHash = ''
	let targetHash = ''
    if (sourceFileSize === targetFileSize) {
      sourceHash = await this.calculateHash(sourceFile)
	  targetHash = await this.calculateHash(targetFile)
	  if (sourceHash !== targetHash) {
		sourceHash = await this.calculateSortedHash(sourceFile);
		targetHash = await this.calculateSortedHash(targetFile)
	  }
	}
    return [sourceFileSize,targetFileSize,sourceHash,targetHash]
  }
  
  async compareSchemas(source,target,rules) {
	 
    const report = {
      successful : []
    , failed     : []
    }

    let sourceControlFile
	let targetControlFile

  	const sourceControlPath = `${path.join(this.ROOT_FOLDER,source.schema,source.schema)}.json`.split(path.sep).join(path.posix.sep) 
    const targetControlPath = `${path.join(this.ROOT_FOLDER,target.schema,target.schema)}.json`.split(path.sep).join(path.posix.sep) 
	
    try {
	  assert.notEqual(sourceControlPath,targetControlPath,`Source & Target control files are identical: "${sourceControlPath}"`);
	} catch(e) {
	  report.failed.push([source.schema,target.schema,'',0,0,0,0,e.message])
      return report	 
	}
	
	// Load the Control Files..
    
    try {
      const fileContents = await this.cloudService.getObject(sourceControlPath)		
      sourceControlFile = this.parseContents(fileContents)
	} catch(e) {
	  report.failed.push([source.schema,target.schema,sourceControlPath,0,0,0,0,`Error reading source control file: ${e.message}`])
      return report	 
	}

    try {
      const fileContents = await this.cloudService.getObject(targetControlPath)		
      targetControlFile = this.parseContents(fileContents)
	} catch(e) {
	  report.failed.push([source.schema,target.schema,targetControlPath,0,0,0,0,`Error reading target control file: ${e.messge}`])
      return report	 
	}
	
    let results = await Promise.all(Object.keys(sourceControlFile.data).map(async (tableName) => {return await this.compareFiles( sourceControlFile.data[tableName].file, targetControlFile.data[tableName].file)}))
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
	        
  async getRowCounts(target) {
	
	return []  
  }       
  
}
module.exports = AWSS3QA