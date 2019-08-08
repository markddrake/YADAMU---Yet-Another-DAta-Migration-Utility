"use strict" 
const MongoDBI = require('../../mongodb/node/mongoDBI.js');

class MongoCompare extends MongoDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }

    configureTest(connectionProperties,testParameters,target,tableMappings) {
      testParameters.HOSTNAME = connectionProperties.host
      testParameters.DATABASE = target.schema
      super.configureTest(connectionProperties,testParameters,this.DEFAULT_PARAMETERS,tableMappings);
    }
    
    async recreateSchema(target,password) {
      try {
        this.db = await this.client.db(target.schema)
        await this.db.dropDatabase()
      } catch (e) {
        throw e;
      }
      this.db = await this.client.db(target.schema)
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

module.exports = MongoCompare