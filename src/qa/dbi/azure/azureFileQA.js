

import Yadamu              from '../../core/yadamu.js';

import AzureFileDBI        from '../../../node/dbi/azure/azureFileDBI.js';
import AzureConstants      from '../../../node/dbi/azure/azureConstants.js';
import YadamuQALibrary     from '../../lib/yadamuQALibrary.js'

class AzureFileQA extends YadamuQALibrary.fileQAMixin(AzureFileDBI)  {
   
  static #DBI_PARAMETERS
  
  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,AzureConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[AzureConstants.DATABASE_KEY] || {},{RDBMS: AzureConstants.DATABASE_KEY}))
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return AzureFileQA.DBI_PARAMETERS
  }	

  constructor(yadamu,role,connectionSettings,parameters) {
     super(yadamu,role,connectionSettings,parameters)
  }
  
  async finalize() { /* OVERRIDE */ }
  
}

export { AzureFileQA as default }