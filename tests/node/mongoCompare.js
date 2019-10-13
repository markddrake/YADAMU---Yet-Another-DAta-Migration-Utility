"use strict" 
const MongoDBI = require('../../mongodb/node/mongoDBI.js');

class MongoCompare extends MongoDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }
    
    async recreateSchema(target,password) {

      const database = target.schema ? target.schema : ( target.owner === 'dbo' ? target.database : target.owner)

      try {
        if (this.status.sqlTrace) {
           this.status.sqlTrace.write(`use ${database}\n`)      
        }
        this.db = await this.client.db(database)
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`db.dropDatabase()\n`)      
        }
        await this.db.dropDatabase()
      } catch (e) {
        throw e;
      }
      if (this.status.sqlTrace) {
         this.status.sqlTrace.write(`use ${database}\n`)      
      }
      this.db = await this.client.db(database)
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