"use strict" 

import AzureDBI          from '../../../node/dbi/azure/azureDBI.js';
import AzureConstants    from '../../../node/dbi/azure/azureConstants.js';

import YadamuTest        from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'

class AzureQA extends YadamuQALibrary.loaderQAMixin(AzureDBI) {
  
  static #DBI_PARAMETERS
	
  static get DBI_PARAMETERS()  { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.DBI_PARAMETERS,AzureConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AzureConstants.DATABASE_KEY] || {},{RDBMS: AzureConstants.DATABASE_KEY}))
    return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return AzureQA.DBI_PARAMETERS
  }	
			
  constructor(yadamu,manager,connectionSettings,parameters) {
     super(yadamu,manager,connectionSettings,parameters)
  }
  
  async initializeImport() {
    if (this.options.recreateSchema === true) {
 	  await this.recreateSchema();
	}
    await super.initializeImport();
  }	
  
  classFactory(yadamu) {
    return new AzureQA(yadamu,this,this.connectionParameters,this.parameters)
  }
  
}
export { AzureQA as default }