

import Yadamu              from '../../core/yadamu.js';

import AWSS3FileDBI        from '../../../node/dbi/awsS3/awsS3FileDBI.js';
import AWSS3Constants      from '../../../node/dbi/awsS3/awsS3Constants.js';

class AWSS3FileQA extends AWSS3FileDBI {
   
  static #DBI_PARAMETERS
  
  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,AWSS3Constants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[AWSS3Constants.DATABASE_KEY] || {},{RDBMS: AWSS3Constants.DATABASE_KEY}))
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return AWSS3FileQA.DBI_PARAMETERS
  }	

  constructor(yadamu,role,connectionSettings,parameters) {
     super(yadamu,role,connectionSettings,parameters)
  }
  
  async finalize() { /* OVERRIDE */ }
  
}

export { AWSS3FileQA as default }