"use strict" 
const SnowflakeDBI = require('../../../YADAMU/snowflake/node/snowflakeDBI.js');

class SnowflakeQA extends SnowflakeDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }
	
    async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	  }
	}

    async recreateSchema() {
       
      try {
        const dropSchema = `drop schema if exists "${this.paramteters.TO_USER}"`;
        await this.executeSQL(dropSchema,[]);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createSchema = `create schema "${this.paramteters.TO_USER}"`;
      await this.executeSQL(createSchema,[]);      
    }   

    async getRowCounts(target) {
    }
    
    async report(source,target) {

      const report = {
        successful : []
       ,failed     : []
      }

      return report
    }
      
}

module.exports = SnowflakeQA