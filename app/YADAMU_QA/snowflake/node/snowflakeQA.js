"use strict" 
const SnowFlakeDBI = require('../../../YADAMU/snowflake/node/snowflakeDBI.js');

class SnowFlakeQA extends SnowFlakeDBI {
    
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
        const createDatabase = `create database if not exists "${this.parameters.SNOWFLAKE_SCHEMA_DB}"`;
        await this.executeSQL(createDatabase,[]);      
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}"`;
        await this.executeSQL(dropSchema,[]);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createSchema = `create schema "${this.parameters.TO_USER}"`;
      await this.executeSQL(createSchema,[]);      
    }   

    async getRowCounts(target) {
    }
	
    async compareSchemas(source,target) {

     const sqlStatement = 'call COMPARE_SCHEMAS(:1,:2);
     results = await this.executeSQL(sqlStatement,[source,target]);
	 console.log(results);

      const report = {
        successful : []
       ,failed     : []
      }

      return report
    }
      
}

module.exports = SnowFlakeQA