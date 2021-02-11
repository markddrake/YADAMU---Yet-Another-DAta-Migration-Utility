"use strict" 

const path=require('path')
const fs=require('fs')
const crypto = require('crypto');
const { pipeline } = require('stream');

const AzureDBI = require('../../../YADAMU//loader/azure/azureDBI.js');
const AzureError = require('../../../YADAMU/loader/azure/azureException.js')
const AzureConstants = require('../../../YADAMU/loader/azure/azureConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class AzureQA extends AzureDBI {
  
  static #_YADAMU_DBI_PARAMETERS
	
  static get YADAMU_DBI_PARAMETERS()  { 
    this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,AzureConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AzureConstants.DATABASE_KEY] || {},{RDBMS: AzureConstants.DATABASE_KEY}))
    return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return AzureQA.YADAMU_DBI_PARAMETERS
  }	
		
  async recreateSchema() {
	await this.cloudService.createBucketContainer()
	await this.cloudService.deleteFolder(this.IMPORT_FOLDER)
  }    
	
  constructor(yadamu) {
     super(yadamu)
  }
  
  async initialize() {
	await super.initialize();
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
  }
  
  setConnectionProperties(connectionProperties) {
	if (connectionProperties.hasOwnProperty('yadamuOptions')) {
	  Object.assign(this.azureOptions,connectionProperties.yadamuOptions)
	  delete connectionProperties.yadamuOptions
	}
    super.setConnectionProperties(connectionProperties)
  }
  
  async saveSortedFile(file,content) {
     const savePath = path.join(process.platform === 'win32' ? 'c:\\temp\\yadamu\\compare' : '/tmp/yadamu/compare',file.split(path.posix.sep).join(path.sep))
     fs.mkdirSync(path.dirname(savePath),{recursive:true})
     fs.writeFileSync(savePath,JSON.stringify(content))
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
    // this.saveSortedFile(file,array)
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
	const sourceFileSize = props.contentLength
	props = await this.cloudService.getObjectProps(targetFile)
	const targetFileSize = props.contentLength
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
	
	// Load the Control File...
    let controlFilePath = `${path.join(this.ROOT_FOLDER,source.schema,source.schema)}.json`.split(path.sep).join(path.posix.sep) 
    let fileContents = await this.cloudService.getObject(controlFilePath)		
    const sourceControlFile = this.parseContents(fileContents)
	
    controlFilePath = `${path.join(this.ROOT_FOLDER,target.schema,target.schema)}.json`.split(path.sep).join(path.posix.sep) 
    fileContents = await this.cloudService.getObject(controlFilePath)		
    const targetControlFile = this.parseContents(fileContents)
	
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
  
}
module.exports = AzureQA