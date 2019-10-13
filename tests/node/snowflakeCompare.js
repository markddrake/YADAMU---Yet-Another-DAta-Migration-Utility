"use strict" 
const SnowflakeDBI = require('../../snowflake/node/snowflakeDBI.js');

class SnowflakeCompare extends SnowflakeDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }
    

    async recreateSchema(schema,password) {
       
      try {
        const dropSchema = `drop schema if exists "${schema.schema}"`;
        await this.executeSQL(dropSchema,[]);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createSchema = `create schema "${schema.schema}"`;
      await this.executeSQL(createSchema,[]);      
    }   

    async importResults(target,timingsArray) {
    }
    
    async report(source,target,timingsArray) {

      const report = {
        successful : []
       ,failed     : []
      }

      return report
    }
      
}

module.exports = SnowflakeCompare