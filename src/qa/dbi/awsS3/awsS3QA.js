"use strict" 

import AWSS3DBI          from '../../../node/dbi/awsS3/awsS3DBI.js';
import AWSS3Constants    from '../../../node/dbi/awsS3/awsS3Constants.js';

import YadamuTest        from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'

class AWSS3QA extends YadamuQALibrary.loaderQAMixin(AWSS3DBI) {

  static #DBI_PARAMETERS
  
  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.DBI_PARAMETERS,AWSS3Constants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AWSS3Constants.DATABASE_KEY] || {},{RDBMS: AWSS3Constants.DATABASE_KEY}))
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return AWSS3QA.DBI_PARAMETERS
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
    return new AWSS3QA(yadamu,this,this.connectionParameters,this.parameters)
  }}
 
export { AWSS3QA as default }