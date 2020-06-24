"use strict" 
const MongoDBI = require('../../../YADAMU/mongodb/node/mongoDBI.js');
const {ConnectionError, MongodbError, DatabaseError} = require('../../../YADAMU/common/yadamuError.js')

class MongoQA extends MongoDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }

    async recreateDatabase() {

      const operation = this.traceMongo(`dropDatabase()`)
      try {
        await this.use(this.parameters.TO_USER)
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(operation)      
        }
        await this.dropDatabase()
        await this.use(this.parameters.TO_USER)
      } catch (e) {
        throw new MongodbError(e,operation)
      }
    }

	async scheduleTermination() {
      const killOperation = this.parameters.KILL_READER_AFTER ? 'Reader'  : 'Writer'
	  const killDelay = this.parameters.KILL_READER_AFTER ? this.parameters.KILL_READER_AFTER  : this.parameters.KILL_WRITER_AFTER
	  const timer = setTimeout(async () => {
    	  if (this.client !== undefined) {
			const currentOp = {
				currentOp : true
			  , $all      : false
			  , $ownOps   : true
			}
		    this.yadamuLogger.qa(['KILL',this.yadamu.parameters.ON_ERROR,this.DATABASE_VENDOR,killOperation,killDelay,null,this.getWorkerNumber()],`Killing connection.`);
			let operation
			try {
	          const dbAdmin = await this.client.db('admin',{returnNonCachedInstance:true});	 
		      operation = `mongoClient.db('admin').command(${currentOp})`
   		      const ops = await dbAdmin.command(currentOp)
			  const hostList = []
			  ops.inprog.forEach((op) => {
				if (op.client && (op.client.startsWith('172.18.0.1'))) {
				  hostList.push(op.client);
				}
			  })
			  const dropConnections = {
				  dropConnections: 1,
                    hostAndPort : hostList
              }
		      operation = `mongoClient.db('admin').command(${JSON.stringify(dropConnections)})`
			  console.log(operation)
   		      const res = await  await dbAdmin.command(dropConnections)
			  // await dbAdmin.close()
			} catch (e) {
			  throw new MongodbError(e,operation);
			}
	      }
		  else {
		    this.yadamuLogger.qa(['KILL',this.yadamu.parameters.ON_ERROR,this.DATABASE_VENDOR,killOperation,killDelay,pid,this.getWorkerNumber()],`Unable to Kill Connection: Connection Pool no longer available.`);
		  }
		},
		killDelay
      )
	  timer.unref()
	}

    async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateDatabase();
	  }
	  if (this.testLostConnection()) {
		this.scheduleTermination();
	  }
	}

    async getRowCounts(target) {
	   await this.use(target.schema);
	   const collections = await this.collections();
	   const results = await Promise.all(collections.map(async (collection) => {
		 return [target.schema,collection.collectionName,await this.collectionCount(collection)]
	   }))
	   return results;
    }
	
    async compareSchemas(source,target) {

      await this.use(source.schema);
	  const sourceHash = await this.dbHash()
	  const sourceCounts = await this.getRowCounts(source)
	  await this.use(target.schema);
	  const targetHash = await this.dbHash(source.schema)
      const targetCounts =  await this.getRowCounts(target)
	  
	  
	   
	  const report = {
        successful : []
       ,failed     : []
      }
	  
	  Object.keys(sourceHash.collections).forEach((collectionName,idx) => {
		 if (targetHash.collections[collectionName] && (targetHash.collections[collectionName] === sourceHash.collections[collectionName])) {
           report.successful.push([source.schema,target.schema,collectionName,targetCounts.find(element => element[1] === collectionName)[2]])
		 }
		 else {
		   report.failed.push([source.schema,target.schema,collectionName, sourceCounts.find(element => element[1] === collectionName)[2],targetCounts.find(element => element[1] === collectionName)[2],NaN,NaN,null,null])
		 }
	  })
		   

      // 'SUCCESSFUL' "RESULTS", SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
      //  'FAILED' "RESULTS", SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME,SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM "NOTES"
      
      return report
    }
      
}

module.exports = MongoQA