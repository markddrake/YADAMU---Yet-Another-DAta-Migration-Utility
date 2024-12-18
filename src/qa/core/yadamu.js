
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

import YadamuLogger           from './yadamuLogger.js';

// const YadamuDefaults = require('./yadamuDefaults.json')
// const CompareRules = require('./compareRules.json')

const  __filename             = fileURLToPath(import.meta.url);
const __dirname               = dirname(__filename);
const YadamuDefaults          = JSON.parse(fs.readFileSync(join(__dirname,'../cfg/yadamuDefaults.json'),'utf-8'));

class Yadamu extends _Yadamu {
  
  static #YADAMU_PARAMETERS
  static #DBI_PARAMETERS
  
  #TERMINATION_CONFIGURATION = undefined
    
  static get QA_CONFIGURATION()   { return YadamuDefaults };    
  static get QA_DRIVER_MAPPINGS() { return this.QA_CONFIGURATION.drivers }
  
  static get YADAMU_PARAMETERS() { 
    this.#YADAMU_PARAMETERS = this.#YADAMU_PARAMETERS || Object.freeze({
	  MODE: DBIConstants.MODE
	, ..._Yadamu.YADAMU_PARAMETERS
	, ...this.QA_CONFIGURATION.yadamu
	})
	return this.#YADAMU_PARAMETERS
  }
  
  // YADAMU_PARAMETERS merged with the yadamuDBI section of the Master Configuration File and the yadamuDBI section of the QA Master Configuration File
  static get DBI_PARAMETERS()  {
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
	  ...this.YADAMU_PARAMETERS
	, ...DBIConstants.DBI_PARAMETERS
    , ...this.QA_CONFIGURATION.yadamuDBI
    })
    return this.#DBI_PARAMETERS
  }

  get LOGGER() {
    this._LOGGER = this._LOGGER || (() => {
      const logger = this.LOG_FILE === undefined ? YadamuLogger.consoleLogger(this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX) : YadamuLogger.fileLogger(this.LOG_FILE,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      return logger
    })();
    return this._LOGGER
  }
  
  set LOGGER(v) {
	 this._LOGGER = v 
  }
 
  get QA_TEST()               { return true }
  
  get YADAMU_PARAMETERS()     { return Yadamu.YADAMU_PARAMETERS }
  get DBI_PARAMETERS()        { return Yadamu.DBI_PARAMETERS }

  get YADAMU_QA()             { return true }
  
  get MACROS()                { this._MACROS = this._MACROS || { timestamp: new Date().toISOString().replace(/:/g,'.')}; return this._MACROS }
  set MACROS(v)               { this._MACROS = v }
   
  get MODE()                  { return this.parameters.MODE  || this.DBI_PARAMETERS.MODE }

  get KILL_READER()           { return this.#TERMINATION_CONFIGURATION.process  === 'READER' }
  get KILL_WRITER()           { return this.#TERMINATION_CONFIGURATION.process  === 'WRITER' }
  get KILL_WORKER()           { return this.#TERMINATION_CONFIGURATION.worker }
  get KILL_DELAY()            { return this.#TERMINATION_CONFIGURATION.delay }

  get IDENTIFIER_MAPPINGS()   { return super.IDENTIFIER_MAPPINGS }
  set IDENTIFIER_MAPPINGS(v)  { this._IDENTIFIER_MAPPINGS = v || this.IDENTIFIER_MAPPINGS }

  constructor(configParameters) {
	
    super('TEST',configParameters)
	
	// console.log('Yadamu.YADAMU_PARAMETERS:',Yadamu.YADAMU_PARAMETERS)
	// console.log('YadamuTest this.YADAMU_PARAMETERS:',this.YADAMU_PARAMETERS)
	// console.log('Yadamu.DBI_PARAMETERS:',Yadamu.DBI_PARAMETERS)
	// console.log('YadamuTest this.DBI_PARAMETERS:',this.DBI_PARAMETERS)
	
  }
  
  async close(testsComplete) {
    if (testsComplete === true) {
	  await super.close()
	}
  }

  async doServerImport(dbi,file) {    
    dbi.parameters.FILE = file
    const metrics = await super.doServerImport(dbi);
    delete this.parameters.FILE
    return metrics;
  }
		
  set TERMINATION_CONFIGURATION(configuration) {
    // Termination Configuration is added by YADAMU-QA
	this.#TERMINATION_CONFIGURATION = configuration
  }
 
  get TERMINATION_CONFIGURATION() { return this.#TERMINATION_CONFIGURATION }
  
  scheduleLostConnectionTest(role,workerNumber) {
	return (this.TERMINATION_CONFIGURATION && (this.TERMINATION_CONFIGURATION.process === role) && ((this.TERMINATION_CONFIGURATION.worker === workerNumber )  || ((this.TERMINATION_CONFIGURATION.worker === undefined) && (workerNumber === 'Manager'))))
  } 
  
  invalidParameter(parameterName) {
    switch (parameterName.toUpperCase()) {
      case 'CONN':		  
      case '--CONN':
      case 'CONNECTION':		  
      case '--CONNECTION':
        break;   
	  default:
 	   super.invalidParameter(parameterName);
   	}
  }

  recordMetrics(tableName,metrics) {    

    if (metrics.read - metrics.committed - metrics.lost - metrics.skipped !== 0) {
	  const tags = []
	  if (this.TERMINATION_CONFIGURATION) {
		 tags.push('KILL',this.TERMINATION_CONFIGURATION.process, this.PARALLEL ? 'SEQUENTIAL' : 'PARALLEL', this.ON_ERROR)
	  }
	  this.LOGGER.qaWarning([...tags,`${tableName}`,`${metrics.insertMode}`,`INCONSISTENT METRICS`],`read: ${metrics.read}, parsed: ${metrics.parsed}, received:  ${metrics.received}, committed: ${metrics.committed}, skipped: ${metrics.skipped}, lost: ${metrics.lost}, pending: ${metrics.pending}, written: ${metrics.written}, cached: ${metrics.cached}.`)  
    } 
	
	return super.recordMetrics(tableName,metrics)
  
  }
  
}  
     
export { Yadamu as default}
