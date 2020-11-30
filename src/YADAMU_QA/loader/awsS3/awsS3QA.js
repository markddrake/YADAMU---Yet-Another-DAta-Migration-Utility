"use strict" 

const AWSS3DBI = require('../../../YADAMU//loader/awsS3/awsS3DBI.js');

class AWSS3QA extends AWSS3DBI {
  
  async recreateSchema() {
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
	  Object.assign(this.s3Options,connectionProperties.yadamuOptions)
	  delete connectionProperties.yadamuOptions
	}
    super.setConnectionProperties(connectionProperties)
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
  
  getRowCount(file) {
	return 0
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
  }1
	        
  async getRowCounts(target) {
	
	return []  
  }       
  
}
module.exports = AWSS3QA