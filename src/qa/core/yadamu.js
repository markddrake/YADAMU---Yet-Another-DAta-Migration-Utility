
"use strict"

import fs                     from 'fs';

import { 
  dirname, 
  join 
}                             from 'path';

import { 
  fileURLToPath 
}                             from 'url';

import { 
  performance 
}                             from 'perf_hooks';

import _Yadamu                from '../../node/core/yadamu.js';
import YadamuConstants        from '../../node/lib/yadamuConstants.js';
import DBIConstants           from '../../node/dbi/base/dbiConstants.js';
import NullWriter             from '../../node/util/nullWriter.js';

import YadamuMetrics          from './yadamuMetrics.js';
import YadamuLogger           from './yadamuLogger.js';

// const YadamuDefaults = require('./yadamuDefaults.json')
// const CompareRules = require('./compareRules.json')

const  __filename             = fileURLToPath(import.meta.url);
const __dirname               = dirname(__filename);
const YadamuDefaults          = JSON.parse(fs.readFileSync(join(__dirname,'../cfg/yadamuDefaults.json'),'utf-8'));
const CompareRules            = JSON.parse(fs.readFileSync(join(__dirname,'../cfg/compareRules.json'),'utf-8'));

class Yadamu extends _Yadamu {
  
  static #_YADAMU_PARAMETERS
  static #_DBI_PARAMETERS
    
  static get QA_CONFIGURATION()   { return YadamuDefaults };    
  static get QA_DRIVER_MAPPINGS() { return this.QA_CONFIGURATION.drivers }
  static get COMPARE_RULES()      { return CompareRules };    
  
  static get YADAMU_PARAMETERS() { 
    this.#_YADAMU_PARAMETERS = this.#_YADAMU_PARAMETERS || Object.freeze(Object.assign({},{MODE: DBIConstants.MODE},_Yadamu.YADAMU_PARAMETERS,this.QA_CONFIGURATION.yadamu))
	return this.#_YADAMU_PARAMETERS
  }
  
  // YADAMU_PARAMETERS merged with the yadamuDBI section of the Master Configuration File and the yadamuDBI section of the QA Master Configuration File
  static get DBI_PARAMETERS()  {
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},this.YADAMU_PARAMETERS,DBIConstants.DBI_PARAMETERS,this.QA_CONFIGURATION.yadamuDBI))
    return this.#_DBI_PARAMETERS
  }

  get LOGGER() {
    this._LOGGER = this._LOGGER || (() => {
      const logger = this.LOG_FILE === undefined ? YadamuLogger.consoleLogger(this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX) : YadamuLogger.fileLogger(this.LOG_FILE,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      return logger
    })();
    return this._LOGGER
  }
  
  get QA_TEST()               { return true }
  
  get YADAMU_PARAMETERS()     { return Yadamu.YADAMU_PARAMETERS }
  get DBI_PARAMETERS()        { return Yadamu.DBI_PARAMETERS }

  get YADAMU_QA()             { return true }
  
  get MACROS()                { this._MACROS = this._MACROS || { timestamp: new Date().toISOString().replace(/:/g,'.')}; return this._MACROS }
  set MACROS(v)               { this._MACROS = v }
   
  get MODE()                  { return this.parameters.MODE  || this.DBI_PARAMETERS.MODE }

  get KILL_READER()           { return this.killConfiguration.process  === 'READER' }
  get KILL_WRITER()           { return this.killConfiguration.process  === 'WRITER' }
  get KILL_WORKER()           { return this.killConfiguration.worker }
  get KILL_DELAY()            { return this.killConfiguration.delay }

  get IDENTIFIER_MAPPINGS()   { return super.IDENTIFIER_MAPPINGS }
  set IDENTIFIER_MAPPINGS(v)  { this._IDENTIFIER_MAPPINGS = v || this.IDENTIFIER_MAPPINGS }

  constructor(configParameters) {
	
    super('TEST',configParameters)
	this.testMetrics = new YadamuMetrics();
    
	// console.log('Yadamu.YADAMU_PARAMETERS:',Yadamu.YADAMU_PARAMETERS)
	// console.log('YadamuTest this.YADAMU_PARAMETERS:',this.YADAMU_PARAMETERS)
	// console.log('Yadamu.DBI_PARAMETERS:',Yadamu.DBI_PARAMETERS)
	// console.log('YadamuTest this.DBI_PARAMETERS:',this.DBI_PARAMETERS)
	
  }
  
  initializeSQLTrace() {
	if ((this.STATUS.sqlLogger instanceof NullWriter) && this.parameters.SQL_TRACE) {
       this.STATUS.sqlLogger = undefined
	}
	super.initializeSQLTrace()
  } 
  
  async doExport(dbi,file) {
    this.parameters.FILE = file
    const metrics = await super.doExport(dbi);
    delete this.parameters.FILE
    return metrics;
  }
 
  async doImport(dbi,file) {
    this.parameters.FILE = file
    const metrics = await super.doImport(dbi);
    delete this.parameters.FILE
    return metrics;
  }

  async doServerImport(dbi,file) {    
    dbi.parameters.FILE = file
    const metrics = await super.doServerImport(dbi);
    delete this.parameters.FILE
    return metrics;
  }
		
  getCompareRules(rules) {
	return {
      emptyStringIsNull    : rules.EMPTY_STRING_IS_NULL 
    , minBigIntIsNull      : rules.MIN_BIGINT_IS_NULL 
    , doublePrecision      : rules.DOUBLE_PRECISION || 18
    , numericScale     : rules.NUMERIC_SCALE || null
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
  
  
  configureTermination(configuration) {
    // Kill Configuration is added by YADAMU-QA
    this.killConfiguration = configuration
  }

  terminateConnection(role,workerNumber) {
    return (this.killConfiguration && (this.killConfiguration.process === role) && ((this.killConfiguration.worker === workerNumber )  || ((this.killConfiguration.worker === undefined) && (workerNumber === 'Manager'))))
  } 
  
  recordMetrics(tableName,metrics) {    

    if (metrics.read - metrics.committed - metrics.lost - metrics.skipped !== 0) {
	  const tags = []
	  if (this.killConfiguration) {
		 tags.push('KILL',this.killConfiguration.process, this.PARALLEL ? 'SEQUENTIAL' : 'PARALLEL', this.ON_ERROR)
	  }
	  this.LOGGER.qaWarning([...tags,`${tableName}`,`${metrics.insertMode}`,`INCONSISTENT METRICS`],`read: ${metrics.read}, parsed: ${metrics.parsed}, received:  ${metrics.received}, committed: ${metrics.committed}, skipped: ${metrics.skipped}, lost: ${metrics.lost}, pending: ${metrics.pending}, written: ${metrics.written}, cached: ${metrics.cached}.`)  
    } 
	
	return super.recordMetrics(tableName,metrics)
  
  }
  
}  
     
export { Yadamu as default}
