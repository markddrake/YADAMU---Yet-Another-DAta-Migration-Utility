"use strict" 
const fs = require('fs');
const path = require('path');

const Yadamu = require('../../common/yadamu.js').Yadamu;
const DBWriter = require('../../common/dbWriter.js');
const FileWriter = require('../../file/node/fileWriter.js');
const FileParser = require('../../file/node/fileParser.js');
const FileStatistics = require('./fileStatistics.js');

class FileCompare extends FileWriter {
  
  sortRows(array) {
     
    array.sort(function (a,b){
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

  /* 
  **
  ** Do not use with large files
  **

  
  getContentMetadata(file,sort) {
      
    const metadata = {}
    
    const content = require(path.resolve(file));
    if (content.data) {
      const tables = Object.keys(content.data);
   
      // Do not use .forEach or .Map - Memory usage is too high
      
      for (const table of tables) {
        metadata[table] = {
          rowCount : content.data[table].length
        , byteCount: JSON.stringify(content.data[table]).length
        , hash     : ( this.deepCompare === true ? deepCompare(content.data[table]) : null )
        }
      }
    }
    return metadata;       
  }
  
  */

  async getContentMetadata(file,sort) {
      
    const nulLogger = fs.createWriteStream("\\\\.\\NUL");
    const logWriter = this.logWriter;
    const saxParser  = new FileParser(logWriter)
    let readStream
    try {
      readStream = fs.createReadStream(file);         
    } catch (e) {
      if (err.code !== 'ENOENT') {
        throw err;
      } 
      files[fidx].size = -1;
    }
     
    const fileStats = new FileStatistics()    
    const writer = new DBWriter(fileStats , null, null, null, nulLogger);
    
    const processMetadata = new Promise(function (resolve,reject) {
      try {
        writer.on('finish',function() {resolve(saxParser.checkState())});
        writer.on('error', function(err){logWriter.write(`${new Date().toISOString()}[FileCompare.error()]}: ${err.stack}\n`);reject(err)})      
        readStream.pipe(saxParser).pipe(writer);
      } catch (err) {
        logWriter.write(`${new Date().toISOString()}[FileCompare.getConentMetadata()]}: ${err.stack}\n`);
        reject(err);
      }
    })    
    
    await processMetadata;
    return fileStats.tableInfo
  }

   
  updateSettings(dbParameters,dbConnection,role,target) {
    dbParameters.FILE = target
  }
    
  constructor(yadamu,logger) {
     super(yadamu)
     this.logger = logger;
     this.deepCompare = false;
     this.sort = false;
  }
  
  makeLowerCase(object) {
        
    Object.keys(object).forEach(function(key) {
      if (key !== key.toLowerCase()) {
        object[key.toLowerCase()] = Object.assign({}, object[key])
        delete object[key]
      }
    },this)
  }
      
  async report(grandparent,parent,child,timings) {
        
    let colSizes = [48, 18, 12, 12, 12, 12]
 
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach(function(size) {
      seperatorSize += size;
    },this)
 
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
 
    this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    this.logger.write(`| ${'GRANDPARENT FILE'.padEnd(colSizes[0])} |`
                    + ` ${'GRANDPARENT SIZE'.padStart(colSizes[1])} |`
                    + ` ${'PARENT_SIZE'.padStart(colSizes[2])} |` 
                    + ` ${'DELTA'.padStart(colSizes[3])} |`
                    + ` ${'CHILD SIZE'.padStart(colSizes[4])} |` 
                    + ` ${'DELTA'.padStart(colSizes[5])} |`
                    + '\n');
    this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    this.logger.write(`| ${grandparent.padEnd(colSizes[0])} |`
                    + ` ${gStats.size.toString().padStart(colSizes[1])} |`
                    + ` ${pStats.size.toString().padStart(colSizes[2])} |` 
                    + ` ${(gStats.size - pStats.size).toString().padStart(colSizes[3])} |` 
                    + ` ${cStats.size.toString().padStart(colSizes[4])} |` 
                    + ` ${(pStats.size - cStats.size).toString().padStart(colSizes[5])} |` 
                    + '\n');
    this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
 
    colSizes = [48, 18, 12, 12, 12, 12, 18, 12, 12, 12, 12, 48]
    seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach(function(size) {
      seperatorSize += size;
    },this)
    
    
    const gMetadata = await this.getContentMetadata(grandparent)
    const pMetadata = await this.getContentMetadata(parent)
    const cMetadata = await this.getContentMetadata(child);
             
    if (this.parameters.TABLE_MATCHING === 'INSENSITIVE') {
      timings = timings.map(function (t) {
        this.makeLowerCase(t)
        return t
      },this);
      this.makeLowerCase(gMetadata)
      this.makeLowerCase(pMetadata);
      this.makeLowerCase(cMetadata);
    }
    
    const tables = Object.keys(gMetadata).sort();     
    tables.forEach(function (table,idx) {
      const tableTimings = timings[0][table].elapsedTime.padStart(10) 
                         + timings[1][table].elapsedTime.padStart(10) 
                         + timings[2][table].elapsedTime.padStart(10) 
                         + timings[3][table].elapsedTime.padStart(10);
 
      if (idx === 0) {                            
        this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.logger.write(`| ${'TABLE NAME'.padStart(colSizes[0])} |`
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
        this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
        this.logger.write(`| ${table.padStart(colSizes[0])} |`
                        + ` ${gMetadata[table].rowCount.toString().padStart(colSizes[1])} |`
                        + ` ${pMetadata[table].rowCount.toString().padStart(colSizes[2])} |`
                        + ` ${(gMetadata[table].rowCount - pMetadata[table].rowCount).toString().padStart(colSizes[3])} |`
                        + ` ${cMetadata[table].rowCount.toString().padStart(colSizes[4])} |`
                        + ` ${(pMetadata[table].rowCount - cMetadata[table].rowCount).toString().padStart(colSizes[5])} |`
                        + ` ${gMetadata[table].byteCount.toString().padStart(colSizes[6])} |`
                        + ` ${pMetadata[table].byteCount.toString().padStart(colSizes[7])} |`
                        + ` ${(gMetadata[table].byteCount - pMetadata[table].byteCount).toString().padStart(colSizes[8])} |`
                        + ` ${cMetadata[table].byteCount.toString().padStart(colSizes[9])} |`
                        + ` ${(pMetadata[table].byteCount - cMetadata[table].byteCount).toString().padStart(colSizes[10])} |`
                        + ` ${tableTimings.padStart(colSizes[11])} |`
                        + '\n')
      if (idx+1 === tables.length) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    },this)
  }
  
  async recreateTargetSchema(target,password) {
  }        
}

module.exports = FileCompare