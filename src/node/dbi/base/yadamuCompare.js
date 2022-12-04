
import fs                     from 'fs';

import { 
  dirname, 
  join 
}                             from 'path';

import { 
  fileURLToPath 
}                             from 'url';

import YadamuLibrary          from '../../lib/yadamuLibrary.js';

// import LoaderDBI              from '../../dbi/loader/loaderDBI.js';
const LoaderDBIPath           = '../../dbi/loader/loaderDBI.js'

const  __filename             = fileURLToPath(import.meta.url);
const __dirname               = dirname(__filename);
const CompareRules            = JSON.parse(fs.readFileSync(join(__dirname,'../../cfg/compareRules.json'),'utf-8'));

class YadamuCompare {

   static get COMPARE_RULES()      { return CompareRules };    

   get OPERATION_NAME()    { return 'COMPARE' }

   constructor(dbi,configuration) {
	 this.dbi = dbi
	 this.configuration = configuration
     this.yadamuLogger = this.dbi.yadamu.LOGGER
   }
  
  formatCompareRules(rules) {
	return {
      emptyStringIsNull    : rules.EMPTY_STRING_IS_NULL 
    , minBigIntIsNull      : rules.MIN_BIGINT_IS_NULL 
    , doublePrecision      : rules.DOUBLE_PRECISION || 18
    , numericScale         : rules.NUMERIC_SCALE || null
	, spatialPrecision     : rules.SPATIAL_PRECISION || 18
	, timestampPrecision   : rules.TIMESTAMP_PRECISION || 9
	, orderedJSON          : rules.hasOwnProperty("ORDERED_JSON") ? rules.ORDERED_JSON : false	
	, xmlRule              : rules.XML_COMPARISON_RULE || null
    , infinityIsNull       : rules.hasOwnProperty("INFINITY_IS_NULL") ? rules.INFINITY_IS_NULL : false 
    }
  }

  makeXML(rules) {
    return `<rules>${Object.keys(rules).map((tag) => { return `<${tag}>${rules[tag] === null ? '' : rules[tag]}</${tag}>` }).join()}</rules>`
  }
  
  getDefaultValue(parameter,defaults,sourceVendor, sourceVersion, targetVendor, targetVersion) {
      
    const parameterDefaults = defaults[parameter]
    const sourceVersionKey = sourceVendor + "#" + sourceVersion;
    const targetVersionKey = targetVendor + "#" + targetVersion;

    return {
		[parameter] : parameterDefaults?.[sourceVersionKey]?.[targetVersionKey] 
	                  || parameterDefaults?.[sourceVersionKey]?.[targetVendor] 
		              || parameterDefaults?.[sourceVersionKey]?.default
		              || parameterDefaults?.[sourceVendor]?.[targetVersionKey] 
	                  || parameterDefaults?.[sourceVendor]?.[targetVendor] 
		              || parameterDefaults?.[sourceVendor]?.default
		              || parameterDefaults.default
	}
  } 
  
  setCompareRules() {
	  
	const compareRules = {
      OPERATION        : this.OPERATION_NAME
    , MODE             : this.dbi.yadamu.MODE
    , TABLES           : this.configuration.parameters.TABLES || []
    }
	
    Object.assign(compareRules,YadamuCompare.COMPARE_RULES[this.configuration.target.vendor] || {})
    
    let versionSpecificKey = this.configuration.target.vendor + "#" + this.configuration.target.version;
    Object.assign(compareRules, YadamuCompare.COMPARE_RULES[versionSpecificKey] || {})
  
    Object.assign(compareRules, this.getDefaultValue('DOUBLE_PRECISION',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
    Object.assign(compareRules, this.getDefaultValue('NUMERIC_SCALE',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
    Object.assign(compareRules, this.getDefaultValue('SPATIAL_PRECISION',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
    Object.assign(compareRules, this.getDefaultValue('ORDERED_JSON',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
    Object.assign(compareRules, this.getDefaultValue('SERIALIZED_JSON',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
    Object.assign(compareRules, this.getDefaultValue('EMPTY_STRING_IS_NULL',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
    Object.assign(compareRules, this.getDefaultValue('MIN_BIGINT_IS_NULL',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
    Object.assign(compareRules, this.getDefaultValue('OBJECTS_COMPARISON_RULE',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))

    Object.assign(compareRules, this.getDefaultValue('INFINITY_IS_NULL',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version))
	compareRules.INFINITY_IS_NULL = compareRules.INFINITY_IS_NULL && (this.configuration.parameters.INFINITY_MANAGEMENT === 'NULLIFY')
    

    // For CSV based copy operations check if the database can differentiate NULL and EMPTY string when loading from CSV
	compareRules.EMPTY_STRING_IS_NULL = ( this.configuration.copyFromCSV && YadamuCompare.COMPARE_RULES.CSV_EMPTY_STRING_IS_NULL.hasOwnProperty(this.configuration.target.vendor) ) ? YadamuCompare.COMPARE_RULES.CSV_EMPTY_STRING_IS_NULL[this.configuration.target.vendor] : compareRules.EMPTY_STRING_IS_NULL 
	
	compareRules.DDL_COMPATBILITY = ((this.configuration.source.vendor === this.configuration.target.vendor) && (this.configuration.source.version <= this.configuration.target.version))
    
    if (YadamuCompare.COMPARE_RULES.TIMESTAMP_PRECISION[this.configuration.source.vendor] > YadamuCompare.COMPARE_RULES.TIMESTAMP_PRECISION[this.configuration.target.vendor]) {
      compareRules.TIMESTAMP_PRECISION = YadamuCompare.COMPARE_RULES.TIMESTAMP_PRECISION[this.configuration.target.vendor]
    }
	
	let xmlCompareRule = this.getDefaultValue('XML_COMPARISON_RULE',YadamuCompare.COMPARE_RULES,this.configuration.source.vendor,this.configuration.source.version,this.configuration.target.vendor,this.configuration.target.version).XML_COMPARISON_RULE
    compareRules.XML_COMPARISON_RULE = typeof xmlCompareRule === 'object' && xmlCompareRule !== null
	                                 ? xmlCompareRule.hasOwnProperty(this.configuration.parameters.XML_STORAGE_OPTION) 
									 ? xmlCompareRule[this.configuration.parameters.XML_STORAGE_OPTION]
									 : null 
									 : xmlCompareRule
	    
    if (compareRules.DOUBLE_PRECISION !== null) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Double precision limited to ${compareRules.DOUBLE_PRECISION} digits`);
    }
    
    if (compareRules.NUMERIC_SCALE !== null) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Numeric scale restricted to ${compareRules.NUMERIC_SCALE} digits`);
    }

    if (compareRules.SPATIAL_PRECISION !== 18) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Spatial precision limited to ${compareRules.SPATIAL_PRECISION} digits`);
    }
    
    if (compareRules.EMPTY_STRING_IS_NULL === true) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Empty Strings treated as NULL`);
    }
    
    if (compareRules.MIN_BIGINT_IS_NULL === true) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Minimum (Most Negative) BIGINT value treated as NULL`);
    }
    
    if (compareRules.INFINITY_IS_NULL === true) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Infinity, -Infinity and NaN treated as NULL`);
    }
    
    if (compareRules.XML_COMPARISON_RULE !== null) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Target XML storage model: "${this.configuration.parameters.XML_STORAGE_MODEL || 'XML'}". Using comparision rule "${compareRules.XML_COMPARISON_RULE}".`);
    }
    
    if (compareRules.TIMESTAMP_PRECISION) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Timestamp precision limited to ${compareRules.TIMESTAMP_PRECISION} digits`);
    }
    
    if (compareRules.ORDERED_JSON === true) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Using "Ordered JSON" when performing JSON comparisons.`);
    }

    if (compareRules.SERIALIZED_JSON === true) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Target does not support JSON data. Using JSON Parser when comparing JSON values.`);
    }

    if (compareRules.OBJECTS_COMPARISON_RULE !== null) {
      this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Comapring Oracle Objects using ${compareRules.OBJECTS_COMPARISON_RULE}.`);
    }
    this.configuration.rules = compareRules
    return compareRules;
  }
    
  unmapTableName(tableName,identifierMappings) {
    identifierMappings = identifierMappings || {}
    return Object.keys(identifierMappings)[Object.values(identifierMappings).findIndex((v) => { return v?.tableName === tableName})] || tableName
  }
  
  async reportRowCounts() {
     
	const rowCounts = await this.getRowCounts(this.configuration.target.schema)
	
    rowCounts.forEach((row,idx) => {          
      const unmappedTableName = this.unmapTableName(row[1],this.configuration.identifierMappings)
      const tableMetrics = this.configuration.metrics[unmappedTableName] || this.configuration.metrics[row[1]]
      const rowCounts = tableMetrics ? [(tableMetrics.rowCount + tableMetrics.rowsSkipped), tableMetrics?.rowsSkipped, tableMetrics?.rowCount ] : [-1,-1,-1]
      row.push(...rowCounts)
    })   

    const colSizes = [32, 48, 14, 14, 14, 14, 14]
      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
   
    this.yadamuLogger.writeDirect(`\n`)

    rowCounts.sort().forEach((row,idx) => {          
      if (idx === 0) {
       this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${'TARGET SCHEMA'.padStart(colSizes[0])} |` 
                                     + ` ${'TABLE_NAME'.padStart(colSizes[1])} |`
                                     + ` ${'ROWS READ'.padStart(colSizes[2])} |`
                                     + ` ${'SKIPPED'.padStart(colSizes[3])} |`
                                     + ` ${'WRITTEN'.padStart(colSizes[4])} |`
                                     + ` ${'COUNT'.padStart(colSizes[5])} |`
                                     + ` ${'DELTA'.padStart(colSizes[6])} |`
                                     + '\n');
       this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${row[0].padStart(colSizes[0])} |`
                                     + ` ${row[1].padStart(colSizes[1])} |`
                                     + ` ${row[3].toString().padStart(colSizes[2])} |` 
                                     + ` ${row[4].toString().padStart(colSizes[3])} |` 
                                     + ` ${row[5].toString().padStart(colSizes[4])} |` 
                                     + ` ${row[2].toString().padStart(colSizes[5])} |` 
                                     + ` ${(row[5] - row[2]).toString().padStart(colSizes[6])} |`
                                     + '\n');
      }
      else {
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${''.padStart(colSizes[0])} |`
                                     + ` ${row[1].padStart(colSizes[1])} |`
                                     + ` ${row[3].toString().padStart(colSizes[2])} |` 
                                     + ` ${row[4].toString().padStart(colSizes[3])} |` 
                                     + ` ${row[5].toString().padStart(colSizes[4])} |` 
                                     + ` ${row[2].toString().padStart(colSizes[5])} |` 
                                     + ` ${(row[5] - row[2]).toString().padStart(colSizes[6])} |`
                                     + '\n');         
      }
    })

    if (rowCounts.length > 0) {
     this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     

  }
  
  printCompareResults(results) {

    this.configuration.compareResults = results;

    if ((results?.successful.length === 0) && (results?.failed.length === 0)) {
	  return
	}
      
    let colSizes = [12, 32, 32, 48, 14, 14, 14]
    
    let seperatorSize = (colSizes.length *3) - 1
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
    
    this.yadamuLogger.writeDirect(`\n`)
    results.successful.sort().forEach((row,idx) => {
      if (idx === 0) {
       this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${'RESULT'.padEnd(colSizes[0])} |`
                                     + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                                     + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                                     + ` ${(results.isFileBased ? 'FILE_NAME' : 'TABLE_NAME').padStart(colSizes[3])} |`
                                     + ` ${(results.isFileBased ? 'BYTES' : 'ROWS').padStart(colSizes[4])} |`
                                     + ` ${'ELAPSED TIME'.padStart(colSizes[5])} |`
                                     + ` ${'THROUGHPUT'.padStart(colSizes[6])} |`
                                     + '\n');
       this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${'SUCCESSFUL'.padEnd(colSizes[0])} |`
                                     + ` ${row[0].padStart(colSizes[1])} |`
                                     + ` ${row[1].padStart(colSizes[2])} |`)
      }
      else {
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${''.padEnd(colSizes[0])} |`
                                     + ` ${''.padStart(colSizes[1])} |`
                                     + ` ${''.padStart(colSizes[2])} |` )
      }

     this.yadamuLogger.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                   + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                   + ` ${YadamuLibrary.stringifyDuration(parseInt(row[4])).padStart(colSizes[5])} |` 
                                   + ` ${(row[5] === 'NaN/s' ? '' : row[5]+"/s").padStart(colSizes[6])} |` 
                                   + '\n');
    })
        
    if (results.successful.length > 0) {
     this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }

    colSizes = [12, 32, 32, 48, 14, 14, 32, 32, 72]
      
    seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
   
    const notesIdx = colSizes.length-2
    const lineSize = colSizes[notesIdx+1]
    
    results.failed.forEach((row,idx) => {
      const lines = []
      if ((row[notesIdx] !== null) && ((row[notesIdx].length > lineSize) || (row[notesIdx].indexOf('\r\n') > -1))) {
        const blocks = row[notesIdx].split('\r\n')
        for (const block of blocks) {
          const words = block.split(' ')
          let line = ''
          for (let word of words) {
            if (line.length > 0) {
              word = ' ' + word
            }
            if (line.length + word.length < lineSize) {
              line = line + word
            }
            else {
              if (line.length > 0) {
                // Push Line and start new line
                lines.push(line)
                word = word.substring(1)
              }
              while (word.length > lineSize) {
                if (word[lineSize-1] === '-') {
                  lines.push(word.substring(0,lineSize))
                  word = word.substring(lineSize)
                }  
                else {
                  lines.push(word.substring(0,lineSize-1) + "-")
                  word = word.substring(lineSize-1)
                }
              }
              line = word
            }
          }
          if (line.length > 0) {
             lines.push(line)
          }
        }
        row[7] = lines.shift()
      } 
      if (idx === 0) {
       this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${'RESULT'.padEnd(colSizes[0])} |`
                                     + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                                     + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                                     + ` ${(results.isFileBased ? 'FILE_NAME' : 'TABLE_NAME').padStart(colSizes[3])} |`
                                     + ` ${(results.isFileBased ? 'SOURCE BYTES' : 'SOURCE ROWS').padStart(colSizes[4])} |`
                                     + ` ${(results.isFileBased ? 'TARGET BYTES' : 'TARGET ROWS').padStart(colSizes[5])} |`
                                     + ` ${(results.isFileBased ? 'SOURCE CHECKSUM' : 'MISSING ROWS').padStart(colSizes[6])} |`
                                     + ` ${(results.isFileBased ? 'TARGET CHECKSUM' : 'EXTRA ROWS').padStart(colSizes[7])} |`
                                     + ` ${'NOTES'.padEnd(colSizes[8])} |`
                                     + '\n');
       this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${'FAILED'.padEnd(colSizes[0])} |`
                                     + ` ${row[0].padStart(colSizes[1])} |`
                                     + ` ${row[1].padStart(colSizes[2])} |`) 
      }
      else {
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${''.padEnd(colSizes[0])} |`
                                     + ` ${''.padStart(colSizes[1])} |`
                                     + ` ${''.padStart(colSizes[2])} |`)
      }
                
     this.yadamuLogger.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                   + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                   + ` ${row[4].toString().padStart(colSizes[5])} |` 
                                   + ` ${row[5].toString().padStart(colSizes[6])} |` 
                                   + ` ${row[6].toString().padStart(colSizes[7])} |` 
                                   + ` ${(row[7] !== null ? row[7] :  '').padEnd(colSizes[8])} |` 
                                   + '\n');

                               
      lines.forEach((line) => {
       this.yadamuLogger.writeDirect(`|`
                                     + ` ${''.padEnd(colSizes[0])} |`
                                     + ` ${''.padStart(colSizes[1])} |`
                                     + ` ${''.padStart(colSizes[2])} |`
                                     + ` ${''.padStart(colSizes[3])} |` 
                                     + ` ${''.padStart(colSizes[4])} |` 
                                     + ` ${''.padStart(colSizes[5])} |` 
                                     + ` ${''.padStart(colSizes[6])} |` 
                                     + ` ${''.padStart(colSizes[7])} |` 
                                     + ` ${line.padEnd(colSizes[8])} |`
                                     + '\n');
      })          

    })
	
    if (results.failed.length > 0) {
     this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
  
  }
  
  async compare() {

    this.setCompareRules()

    try {
	  // Use dynamic import to avoid recursive import chain - LoaderDBI required for instanceof test
	  const LoaderDBI = (await import(LoaderDBIPath)).default
	  
      if (this.configuration.includeRowCounts) {
		await this.reportRowCounts() 
      } 
      const startTime = performance.now();
      const compareResults = await this.compareSchemas(this.configuration.source.schema,this.configuration.target.schema,this.configuration.rules);
      
      compareResults.elapsedTime = performance.now() - startTime;
      //this.yadamuLogger.qa([`COMPARE`,`${this.configuration.source.vendor}`,`${this.configuration.target.vendor}`],`Elapsed Time: ${YadamuLibrary.stringifyDuration(compareResults.elapsedTime)}s`);
     
      compareResults.successful.forEach((row,idx) => {          
        const mappedTableName = this.configuration.metrics.hasOwnProperty(row[2]) ? row[2] : this.dbi.getMappedTableName(row[2],this.configuration.identifierMappings)
        const tableMetrics = (this.configuration.metrics[mappedTableName] === undefined) ? { elapsedTime : 'N/A', throughput : "N/A ms" } : this.configuration.metrics[mappedTableName]
        row.push(tableMetrics.elapsedTime,tableMetrics.throughput)
      })     

      compareResults.failed = compareResults.failed.filter((row) => {return this.configuration.rules.TABLES === undefined || this.configuration.rules.TABLES.length === 0 || this.configuration.rules.TABLES.includes(row[2])})
      compareResults.isFileBased = (this.dbi instanceof LoaderDBI)

      this.printCompareResults(compareResults)
      return compareResults
	  
    } catch (e) {
     this.yadamuLogger.handleException([`COMPARE`],e)
      return {
	    success: []
	  , failed: []
      , error : e	  
	  }
      // throw e
    } 
  }  
  
  async doCompare() {
	 
	const yadamu = this.dbi.yadamu
	  
	this.configuration = {
	  source             : {
		vendor           : yadamu.RDBMS
	  , version          : this.dbi.DB_VERSION
	  , schema           : yadamu.parameters.FROM_USER
	  }
	, target             : {
		vendor           : yadamu.COMPARE_TARGET || yadamu.RDBMS
      , version          : yadamu.COMPARE_DB_VERSION || this.dbi.DB_VERSION
      , schema           : yadamu.parameters.TO_USER
	  }
    , parameters         : {}
	, metrics            : {}
	, includeRowCounts   : false
	, identifierMappings : {}
    }
	
	return await this.compare()
  }
  
}

export { YadamuCompare as default}