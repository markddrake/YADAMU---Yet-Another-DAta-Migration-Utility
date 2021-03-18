"use strict" 
const fs = require('fs');
const path = require('path');

const util = require('util')
const stream = require('stream')
const pipeline = util.promisify(stream.pipeline);

const DBWriter = require('../../../YADAMU/common/dbWriter.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');
const FileDBI = require('../../../YADAMU/file/node/fileDBI.js');
const JSONParser = require('../../../YADAMU/file/node/jsonParser.js');
const StatisticsCollector = require('./statisticsCollector.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class FileQA extends FileDBI {
   
  static #_YADAMU_DBI_PARAMETERS
	
  static get YADAMU_DBI_PARAMETERS()  { 
	 this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,FileDBI.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[FileDBI.DATABASE_KEY] || {},{RDBMS: FileDBI.DATABASE_KEY}))
	 return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return FileQA.YADAMU_DBI_PARAMETERS
  }	
	
  /*
  **
  ** Uncomment to pass Initialization Vector around via yadamu object
  	
  set INITIALIZATION_VECTOR(v) { 
    this._INITIALIZATION_VECTOR =  v 
	this.yadamu.INITIALIZATION_VECTOR = this._INITIALIZATION_VECTOR 
  }
  
  get INITIALIZATION_VECTOR()  { return this._INITIALIZATION_VECTOR || this.yadamu.INITIALIZATION_VECTOR }		

  **
  */
  
  sortRows(array) {
     
    array.sort((a,b) => {
      for (const i in array) {
        if (a[i] < b[i]) return -1
        if (a[i] > b[i]) return 1;
      }
    })
    return array
  }  

  deepCompare(content) {

    if (this.sort === true) {
      return crypto.createHash('sha256').update(JSON.stringify(sortRows(content))).digest('hex')
    }
    else {
     return crypto.createHash('sha256').update(JSON.stringify(content)).digest('hex');
    }
  }

  async getContentMetadata(file,sort) {
      
    const jsonParser  = new JSONParser(this.yadamuLogger, 'COMPARE', file)
    let readStream
    try {
      readStream = fs.createReadStream(file);         
    } catch (e) {
      if (err.code !== 'ENOENT') {
        throw err;
      } 
      files[fidx].size = -1;
    }

    const statisticsCollector = new StatisticsCollector(this,this.yadamuLogger)  	
     
    try {
	  await pipeline(readStream,jsonParser,statisticsCollector);
	  await statisticsCollector.finalize();
      return statisticsCollector.tableInfo
    } catch (err) {
	  console.log('Pipeline Failed',err)
      this.yadamuLogger.logException([`${this.constructor.name}.getConentMetadata()`],err);
    }
  }
  
  configureTest(recreateSchema) {
    super.configureTest(recreateSchema);
  }
    
  constructor(yadamu) {
     super(yadamu)
     this.deepCompare = false;
     this.sort = false;
  }
  
  setMetadata(metadata) {
    super.setMetadata(metadata)
  }
	 
  makeLowerCase(object) {
        
    Object.keys(object).forEach((key) => {
      if (key !== key.toLowerCase()) {
        object[key.toLowerCase()] = Object.assign({}, object[key])
        delete object[key]
      }
    })
  }
   
  remapTableNames(metrics,mappings) {
        
    Object.keys(mappings).forEach((table) => {
      if (metrics[table] && (mappings[table].tableName != table)) {
        metrics[mappings[table].tableName] =  Object.assign({}, metrics[table])
        delete metrics[table]
      }
    })
  }

  async recreateSchema(target,password) {
  }        
  
  async compareFiles(yadamuLogger,grandparent,parent,child,metrics) {
    let colSizes = [48, 18, 12, 12, 12, 12]
 
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    })
    
    const gStats = fs.statSync(path.resolve(grandparent));
    let pStats
    let cStats
    try {
      pStats = fs.statSync(path.resolve(parent));
    }
    catch (err)  {
      if (err.code !== 'ENOENT') {
          throw err;
      }
      pstats = {size : -1}
    }
    try {
      cStats = fs.statSync(path.resolve(child));
    }
    catch (err) {
      if (err.code !== 'ENOENT') {
          throw err;
      }
      cStats = {size : -1}
    }
    	 
    this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    this.yadamuLogger.writeDirect(`| ${'GRANDPARENT FILE'.padEnd(colSizes[0])} |`
                                + ` ${'GRANDPARENT SIZE'.padStart(colSizes[1])} |`
                                + ` ${'PARENT_SIZE'.padStart(colSizes[2])} |` 
                                + ` ${'DELTA'.padStart(colSizes[3])} |`
                                + ` ${'CHILD SIZE'.padStart(colSizes[4])} |` 
                                + ` ${'DELTA'.padStart(colSizes[5])} |`
                          + '\n');
    this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    this.yadamuLogger.writeDirect(`| ${grandparent.padEnd(colSizes[0])} |`
                                + ` ${gStats.size.toString().padStart(colSizes[1])} |`
                                + ` ${pStats.size.toString().padStart(colSizes[2])} |` 
                                + ` ${(gStats.size - pStats.size).toString().padStart(colSizes[3])} |` 
                                + ` ${cStats.size.toString().padStart(colSizes[4])} |` 
                                + ` ${(pStats.size - cStats.size).toString().padStart(colSizes[5])} |` 
                          + '\n');
    this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
 
    colSizes = [48, 18, 12, 12, 12, 12, 18, 12, 12, 12, 12, 48]
    seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    })
    
    const gMetadata = await this.getContentMetadata(grandparent)
    const pMetadata = await this.getContentMetadata(parent)
    const cMetadata = await this.getContentMetadata(child);
		
    if (this.parameters.TABLE_MATCHING === 'INSENSITIVE') {
      metrics = metrics.map((t) => {
        this.makeLowerCase(t)
        return t
      });
     
      this.makeLowerCase(gMetadata)
      this.makeLowerCase(pMetadata);
      this.makeLowerCase(cMetadata);
    }

    const tables = Object.keys(gMetadata).sort();     

    const failedOperations = []
 
    tables.forEach((table,idx) => {
	  const tableName = table;
 	  const mappedTableName = this.getMappedTableName(tableName,this.identifierMappings)
      const tableTimings = metrics[0][tableName].elapsedTime.padStart(10) 
                         + (metrics[1][mappedTableName] ? metrics[1][mappedTableName].elapsedTime : "-1").padStart(10)
                         + (metrics[2][mappedTableName] ? metrics[2][mappedTableName].elapsedTime : "-1").padStart(10) 
                         + (metrics[3][mappedTableName] ? metrics[3][mappedTableName].elapsedTime : "-1").padStart(10);
 
      if (idx === 0) {                            
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`| ${'TABLE NAME'.padStart(colSizes[0])} |`
                                    + ` ${'GRANDPARENT ROWS'.padStart(colSizes[1])} |`
                                    + ` ${'PARENT_ROWS'.padStart(colSizes[2])} |` 
                                    + ` ${'DELTA'.padStart(colSizes[3])} |`
                                    + ` ${'CHILD ROWS'.padStart(colSizes[4])} |` 
                                    + ` ${'DELTA'.padStart(colSizes[5])} |`
                                    + ` ${'GRANDPARENT BYTES'.padStart(colSizes[6])} |`
                                    + ` ${'PARENT_BYTES'.padStart(colSizes[7])} |` 
                                    + ` ${'DELTA'.padStart(colSizes[8])} |`
                                    + ` ${'CHILD BYTES'.padStart(colSizes[9])} |` 
                                    + ` ${'DELTA'.padStart(colSizes[10])} |`
                                    + ` ${'TIMINGS'.padStart(colSizes[11])} |`
                                    + '\n');
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }

      	
	  if (this.applyTableFilter(table)) {
        this.yadamuLogger.writeDirect(`| ${table.padStart(colSizes[0])} |`
                                    + ` ${gMetadata[table].rowCount.toString().padStart(colSizes[1])} |`
                                    + ` ${pMetadata[mappedTableName].rowCount.toString().padStart(colSizes[2])} |`
                                    + ` ${(gMetadata[table].rowCount - pMetadata[mappedTableName].rowCount).toString().padStart(colSizes[3])} |`
                                    + ` ${cMetadata[mappedTableName].rowCount.toString().padStart(colSizes[4])} |`
                                    + ` ${(pMetadata[mappedTableName].rowCount - cMetadata[mappedTableName].rowCount).toString().padStart(colSizes[5])} |`
                                    + ` ${gMetadata[table].byteCount.toString().padStart(colSizes[6])} |`
                                    + ` ${pMetadata[mappedTableName].byteCount.toString().padStart(colSizes[7])} |`
                                    + ` ${(pMetadata[mappedTableName].byteCount - gMetadata[table].byteCount).toString().padStart(colSizes[8])} |`
                                    + ` ${cMetadata[mappedTableName].byteCount.toString().padStart(colSizes[9])} |`
                                    + ` ${(cMetadata[mappedTableName].byteCount - pMetadata[mappedTableName].byteCount).toString().padStart(colSizes[10])} |`
                                    + ` ${tableTimings.padStart(colSizes[11])} |`
                                    + '\n');
	  }
	  
	  if (gMetadata[table].rowCount !== pMetadata[mappedTableName].rowCount) {
		failedOperations.push(['','',table,gMetadata[table].rowCount,pMetadata[mappedTableName].rowCount,gMetadata[table].rowCount > pMetadata[mappedTableName].rowCount ? gMetadata[table].rowCount - pMetadata[mappedTableName].rowCount : '' ,gMetadata[table].rowCount < pMetadata[mappedTableName].rowCount ? pMetadata[mappedTableName].rowCount - gMetadata[table].rowCount : '','Import #1'])
	  }
	  
	  if (pMetadata[mappedTableName].rowCount !== cMetadata[mappedTableName].rowCount) {
   	    failedOperations.push(['','',table,pMetadata[mappedTableName].rowCount,cMetadata[mappedTableName].rowCount,pMetadata[mappedTableName].rowCount > cMetadata[mappedTableName].rowCount ? pMetadata[mappedTableName].rowCount - cMetadata[mappedTableName].rowCount : '' ,pMetadata[mappedTableName].rowCount < cMetadata[mappedTableName].rowCount ? cMetadata[mappedTableName].rowCount - pMetadata[mappedTableName].rowCount : '','Import #2'])
      }
	  
      if (idx+1 === tables.length) {
          this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    })
	
	return failedOperations;
  }
  
}

module.exports = FileQA