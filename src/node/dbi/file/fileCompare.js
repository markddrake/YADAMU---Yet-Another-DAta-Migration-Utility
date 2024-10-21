
import fs from 'fs';
import path from 'path';

import {
  pipeline
}                          from 'stream/promises';

import YadamuCompare       from '../base/yadamuCompare.js'

import JSONParser          from '../../../node/dbi/file/jsonParser.js';
import DBIConstants                   from '../base/dbiConstants.js'

import StatisticsCollector from './statisticsCollector.js';

class FileCompare extends YadamuCompare {   
  
  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }
  
  constructor(dbi,configuration) {
	super(dbi,configuration)
	this.LOGGER = this.dbi.LOGGER
    this.deepCompare = false;
    this.sort = false;
  }
		
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
      
    const jsonParser  = new JSONParser('COMPARE', file, DBIConstants.PIPELINE_STATE, this.LOGGER)
    let readStream
    try {
      readStream = fs.createReadStream(file);         
    } catch (e) {
      if (err.code !== 'ENOENT') {
        throw err;
      } 
      files[fidx].size = -1;
    }

    const statisticsCollector = new StatisticsCollector()  	
     
    try {
	  await pipeline(readStream,jsonParser,statisticsCollector);
	  // await statisticsCollector.finalize();
      return statisticsCollector.getStatistics()
    } catch (err) {
	  console.log('Pipeline Failed',err)
      this.LOGGER.logException([`${this.constructor.name}.getConentMetadata()`],err);
    }
  }
  
  configureTest(recreateSchema) {
    super.configureTest(recreateSchema);
  }
    
  makeLowerCase(object) {
        
    Object.keys(object).forEach((key) => {
      if (key !== key.toLowerCase()) {
        object[key.toLowerCase()] = {
		  ...object[key]
		}
        delete object[key]
      }
    })
  }
   
  remapTableNames(metrics,mappings) {
        
    Object.keys(mappings).forEach((table) => {
      if (metrics[table] && (mappings[table].tableName != table)) {
        metrics[mappings[table].tableName] = {
		   ...metrics[table]
		}
        delete metrics[table]
      }
    })
  }

  async compareFiles(grandparent,parent,child,metrics) {

	let colSizes = [128, 18, 12, 12, 12, 12]
 
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
    	 
    this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    this.LOGGER.writeDirect(`| ${'GRANDPARENT FILE'.padEnd(colSizes[0])} |`
                                + ` ${'GRANDPARENT SIZE'.padStart(colSizes[1])} |`
                                + ` ${'PARENT_SIZE'.padStart(colSizes[2])} |` 
                                + ` ${'DELTA'.padStart(colSizes[3])} |`
                                + ` ${'CHILD SIZE'.padStart(colSizes[4])} |` 
                                + ` ${'DELTA'.padStart(colSizes[5])} |`
                          + '\n');
    this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    this.LOGGER.writeDirect(`| ${grandparent.padEnd(colSizes[0])} |`
                                + ` ${gStats.size.toString().padStart(colSizes[1])} |`
                                + ` ${pStats.size.toString().padStart(colSizes[2])} |` 
                                + ` ${(gStats.size - pStats.size).toString().padStart(colSizes[3])} |` 
                                + ` ${cStats.size.toString().padStart(colSizes[4])} |` 
                                + ` ${(pStats.size - cStats.size).toString().padStart(colSizes[5])} |` 
                          + '\n');
    this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
 
    colSizes = [48, 18, 12, 12, 12, 12, 18, 12, 12, 12, 12, 48]
    seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    })
    
	const gMetadata = await this.getContentMetadata(grandparent)
    const pMetadata = await this.getContentMetadata(parent)
    const cMetadata = await this.getContentMetadata(child);
	const tables = Object.keys(gMetadata).filter((tableName) => {
      return ((this.dbi.TABLE_FILTER.length === 0) || (this.dbi.TABLE_FILTER.includes(tableName)))
	}).sort();     
	
	const failedOperations = []
    tables.forEach((table,idx) => {
	  const tableName = table;
 	  const mappedTableName = this.dbi.getMappedTableName(tableName,this.dbi.IDENTIFIER_MAPPINGS)
      const tableTimings = (metrics[0][tableName].elapsedTime.toString() + "ms").padStart(10) 
                         + (metrics[1][mappedTableName] ? (metrics[1][mappedTableName].elapsedTime.toString() + "ms") : "N/A").padStart(10)
                         + (metrics[2][mappedTableName] ? (metrics[2][mappedTableName].elapsedTime.toString() + "ms") : "N/A").padStart(10) 
                         + (metrics[3][mappedTableName] ? (metrics[3][mappedTableName].elapsedTime.toString() + "ms") : "N/A").padStart(10);
 
      if (idx === 0) {                            
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.LOGGER.writeDirect(`| ${'TABLE NAME'.padStart(colSizes[0])} |`
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
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }

      	
      this.LOGGER.writeDirect(`| ${table.padStart(colSizes[0])} |`
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
	  
	  if (gMetadata[table].rowCount !== pMetadata[mappedTableName].rowCount) {
		failedOperations.push(['','',table,gMetadata[table].rowCount,pMetadata[mappedTableName].rowCount,gMetadata[table].rowCount > pMetadata[mappedTableName].rowCount ? gMetadata[table].rowCount - pMetadata[mappedTableName].rowCount : '' ,gMetadata[table].rowCount < pMetadata[mappedTableName].rowCount ? pMetadata[mappedTableName].rowCount - gMetadata[table].rowCount : '','Import #1'])
	  }
	  
	  if (pMetadata[mappedTableName].rowCount !== cMetadata[mappedTableName].rowCount) {
   	    failedOperations.push(['','',table,pMetadata[mappedTableName].rowCount,cMetadata[mappedTableName].rowCount,pMetadata[mappedTableName].rowCount > cMetadata[mappedTableName].rowCount ? pMetadata[mappedTableName].rowCount - cMetadata[mappedTableName].rowCount : '' ,pMetadata[mappedTableName].rowCount < cMetadata[mappedTableName].rowCount ? cMetadata[mappedTableName].rowCount - pMetadata[mappedTableName].rowCount : '','Import #2'])
      }
	  
      if (idx+1 === tables.length) {
          this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    })
	
	return failedOperations;
  }
  
}

export { FileCompare as default }
