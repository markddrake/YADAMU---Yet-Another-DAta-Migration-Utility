"use strict" 

import TeradataDBI       from '../../../node/dbi/teradata/teradataDBI.js';
import {TeradataError}   from '../../../node/dbi/teradata/teradataException.js'
import TeradataConstants from '../../../node/dbi/teradata/teradataConstants.js';

import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'
import Yadamu            from '../../core/yadamu.js';

class TeradataQA extends YadamuQALibrary.qaMixin(TeradataDBI) {
	
	static #DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,TeradataConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[TeradataConstants.DATABASE_KEY] || {},{RDBMS: TeradataConstants.DATABASE_KEY}))
	   return this.#DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return TeradataQA.DBI_PARAMETERS
    }	
		
	constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }
	
    async recreateSchema() {		
	  const sqlStatement = `{call YADAMU.SP_RECREATE_DATABASE(?,?,?)}`
	  const results = await this.executeSQL(sqlStatement,[this.parameters.TO_USER,1*1024*1024*1024,null])
    }   
	   
    classFactory(yadamu) {
	  return new TeradataQA(yadamu,this,this.connectionParameters,this.parameters)
    } 
      
}

export { TeradataQA as default }