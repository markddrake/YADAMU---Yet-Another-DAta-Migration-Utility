"use strict" 
const MongoDBI = require('../../../YADAMU/mongodb/node/mongoDBI.js');

class MongoQA extends MongoDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }

    async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateDatabase();
	  }
	}

    
    async recreateDatabase() {

      try {
        await this.useDatabase(this.parameters.TO_USER)
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`db.dropDatabase()\n`)      
        }
        await this.db.dropDatabase()
        await this.useDatabase(this.parameters.TO_USER)
      } catch (e) {
        throw e;
      }
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

module.exports = MongoQA