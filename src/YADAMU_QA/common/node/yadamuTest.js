"use strict"

import fs from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

import { performance } from 'perf_hooks';

import Yadamu from '../../../YADAMU/common/yadamu.js';
import DBIConstants from '../../../YADAMU/common/dbiConstants.js';
import YadamuMetrics from './yadamuMetrics.js';
import YadamuLogger from './yadamuLogger.js';
import NullWriter from  '../../../YADAMU/common/nullWriter.js';

// const YadamuDefaults = require('./yadamuDefaults.json')
// const CompareRules = require('./compareRules.json')

const  __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const YadamuDefaults = JSON.parse(fs.readFileSync(join(__dirname,'./yadamuDefaults.json'),'utf-8'));
const CompareRules = JSON.parse(fs.readFileSync(join(__dirname,'./compareRules.json'),'utf-8'));

class YadamuTest extends Yadamu {
  
  static #_YADAMU_PARAMETERS
  static #_YADAMU_DBI_PARAMETERS
    
  static get QA_CONFIGURATION()   { return YadamuDefaults };    
  static get QA_DRIVER_MAPPINGS() { return this.QA_CONFIGURATION.drivers }
  static get COMPARE_RULES()      { return CompareRules };    
  
  static get YADAMU_PARAMETERS() { 
    this.#_YADAMU_PARAMETERS = this.#_YADAMU_PARAMETERS || Object.freeze(Object.assign({},{MODE: DBIConstants.MODE},Yadamu.YADAMU_PARAMETERS,this.QA_CONFIGURATION.yadamu))
	return this.#_YADAMU_PARAMETERS
  }
  
  // YADAMU_PARAMETERS merged with the yadamuDBI section of the Master Configuration File and the yadamuDBI section of the QA Master Configuration File
  static get YADAMU_DBI_PARAMETERS()  {
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},this.YADAMU_PARAMETERS,DBIConstants.YADAMU_DBI_PARAMETERS,this.QA_CONFIGURATION.yadamuDBI))
    return this.#_YADAMU_DBI_PARAMETERS
  }

  get LOGGER() {
    this._LOGGER = this._LOGGER || (() => {
      const logger = this.LOG_FILE === undefined ? YadamuLogger.consoleLogger(this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX) : YadamuLogger.fileLogger(this.LOG_FILE,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      return logger
    })();
    return this._LOGGER
  }
  
  get YADAMU_PARAMETERS()     { return YadamuTest.YADAMU_PARAMETERS }
  get YADAMU_DBI_PARAMETERS() { return YadamuTest.YADAMU_DBI_PARAMETERS }

  get YADAMU_QA()             { return true }
  
  get MACROS()                { this._MACROS = this._MACROS || { timestamp: new Date().toISOString().replace(/:/g,'.')}; return this._MACROS }
  set MACROS(v)               { this._MACROS = v }
   
  get MODE()                  { return this.parameters.MODE  || this.YADAMU_DBI_PARAMETERS.MODE }

  get IDENTIFIER_MAPPINGS()   { return super.IDENTIFIER_MAPPINGS }
  set IDENTIFIER_MAPPINGS(v)  { this._IDENTIFIER_MAPPINGS = v || this.IDENTIFIER_MAPPINGS }

  constructor(configParameters,activeConnections,encryptionKey) {
	
    super('TEST',configParameters)
	this.activeConnections = activeConnections
	// this.ENCRYPTION_KEY = encryptionKey
	this.testMetrics = new YadamuMetrics();
    
	// console.log('YadamuTest.YADAMU_PARAMETERS:',YadamuTest.YADAMU_PARAMETERS)
	// console.log('YadamuTest this.YADAMU_PARAMETERS:',this.YADAMU_PARAMETERS)
	// console.log('YadamuTest.YADAMU_DBI_PARAMETERS:',YadamuTest.YADAMU_DBI_PARAMETERS)
	// console.log('YadamuTest this.YADAMU_DBI_PARAMETERS:',this.YADAMU_DBI_PARAMETERS)
	
  }
  
  setTeradataWorker(worker) {
	this.teradateWorker = worker
  }
  
  getTeradataWorker() {
	 return this.teradateWorker
  }
  
  async reset(testParameters) {
	
    this._IDENTIFIER_MAPPINGS = undefined
    this._REJECTION_MANAGER = undefined;
    this._WARNING_MANAGER = undefined;
	
    this.STATUS.startTime     = performance.now()
    this.STATUS.warningRaised = false;
    this.STATUS.errorRaised   = false;
    this.STATUS.statusMsg     = 'successfully'
	this.metrics = {}
    	
	this.initializeParameters(testParameters)
	this.processParameters();
	
    if (testParameters.PASSPHRASE || testParameters.ENCRYPTION === true) {		
	  await this.generateCryptoKey()
	}
  }

  initializeSQLTrace() {
	if ((this.STATUS.sqlTrace instanceof NullWriter) && this.parameters.SQL_TRACE) {
       this.STATUS.sqlTrace = undefined
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
    , doublePrecision      : rules.DOUBLE_PRECISION || 18
	, spatialPrecision     : rules.SPATIAL_PRECISION || 18
	, timestampPrecision   : rules.TIMESTAMP_PRECISION || 9
	, orderedJSON          : rules.hasOwnProperty("ORDERED_JSON") ? rules.ORDERED_JSON : false	
	, xmlRule              : rules.XML_COMPARISSON_RULE || null
    , infinityIsNull       : rules.hasOwnProperty("INFINITY_IS_NULL") ? rules.INFINITY_IS_NULL : false 
    }
  }

  makeXML(rules) {
    return `<rules>${Object.keys(rules).map((tag) => { return `<${tag}>${rules[tag] === null ? '' : rules[tag]}</${tag}>` }).join()}</rules>`
  }
  
  recordMetrics(tableName,metrics) {    

    if (metrics.read - metrics.committed - metrics.lost - metrics.skipped !== 0) {
	  this.LOGGER.qaWarning([`${tableName}`,`${metrics.insertMode}`,`INCONSISTENT METRICS`],`read: ${metrics.read}, parsed: ${metrics.pared}, received:  ${metrics.received}, committed: ${metrics.committed}, skipped: ${metrics.skipped}, lost: ${metrics.lost}, pending: {metrics.pending}, written: ${metrics.written}, cached: ${metrics.cached}.`)  
    } 
	
	return super.recordMetrics(tableName,metrics)
  
  }
  
}  
     
export { YadamuTest as default}
