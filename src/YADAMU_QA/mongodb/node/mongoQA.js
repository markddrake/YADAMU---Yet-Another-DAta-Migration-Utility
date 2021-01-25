"use strict" 
const MongoDBI = require('../../../YADAMU/mongodb/node/mongoDBI.js');
const {ConnectionError, MongodbError, DatabaseError} = require('../../../YADAMU/common/yadamuException.js')

class MongoQA extends MongoDBI {
	
	get QA_COMPARE_DBNAME() { return 'YADAMU_QA' }
    
    constructor(yadamu) {
       super(yadamu)
    }

    async recreateDatabase() {

      const operation = this.traceMongo(`dropDatabase()`)
      try {
        await this.use(this.parameters.TO_USER)
        this.status.sqlTrace.write(operation)      
        await this.dropDatabase()
        await this.use(this.parameters.TO_USER)
      } catch (e) {
        throw new MongodbError(e,operation)
      }
    }

	async scheduleTermination(workerId) {
	  this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay],`Termination Scheduled.`);
	  const timer = setTimeout(
        async () => {
    	  if (this.client !== undefined) {
			const currentOp = {
				currentOp : true
			  , $all      : false
			  , $ownOps   : true
			}
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,this.killConfiguration.delay,null,this.getWorkerNumber()],`Killing connection.`);
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
   		      const res = await  await dbAdmin.command(dropConnections)
			  // await dbAdmin.close()
			} catch (e) {
			  throw new MongodbError(e,operation);
			}
	      }
		  else {
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Unable to Kill Connection: Connection Pool no longer available.`);
		  }
		},
		this.killConfiguration.delay
      )
	  timer.unref()
	}

    async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateDatabase();
	  }
	  if (this.terminateConnection()) {
	    this.scheduleTermination(this.getWorkerNumber());
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
	
	async compareCollections(sourceDB,targetDB,collectionName) {
			
		const comparePipeline = [{
		  "$replaceRoot": { 
            newRoot: { _id: "$_id", source: {"$objectToArray": "$$ROOT"}}
          }
        },{
          "$lookup": {
            from: "target", 
            localField: "_id", 
            foreignField: "_id",  
            as: "target"
          }
	    },{
          "$replaceRoot": { 
            newRoot: { _id: "$_id", source: "$source", target : {"$objectToArray": { "$first" : "$target"}}}
          }  
        },{
          "$replaceRoot": {
             newRoot: { _id: "$_id", source: {"$setDifference": ["$source","$target"]}, target: { "$setDifference": ["$target","$source"]}}
          }
        },{
		  "$unwind" : "$source"
		},{
		  "$project" : {
		    _id : 1,
			key : "$source.k",
		    source : "$source.v",
		    target : { $first : { "$filter" : { input : "$target", cond: { "$eq": [ "$$this.k", "$source.k" ]}}}}
		  }
        },{
		  "$unwind" : "$target"
		},{
		  "$project" : {
		    _id    : 1,
			key    : 1,
			sType  : { $type : "$source"},
			tType  : { $type : "$target.v"},
		    source : 1,
		    target : "$target.v"
		  }
        }]
     
        if ( this.parameters.EMPTY_STRING_IS_NULL === true) {
		  comparePipeline.push({
			"$match" : { 
		      "$expr": { 
		        "$not": {
		          "$and": [ 
			        { "$eq": ["$source",""]}, 
			        { "$eq": ["$target",null]}
			      ]
			    }
	          }
		    }
		  })
		}
		
		if ( this.parameters.DOUBLE_PRECISION !== null) {		
		  comparePipeline.push({
		    "$match": { 
		      "$expr": { 
		        "$not": {
		          "$and": [ 
			        { "$eq": ["$sType","double"]}, 
			        { "$eq": ["$tType","double"]},
				    { "$eq": [
				         {"$round": ["$source",this.parameters.DOUBLE_PRECISION]},
					     {"$round": ["$target",this.parameters.DOUBLE_PRECISION]} 
				    ]}
			      ]
			    }
	          }
		    }
		  })
		}
       
		if ( this.parameters.ORDERED_JSON === true) {		
		  comparePipeline.push({
		    "$match": { 
		      "$expr": { 
		        "$not": {
		          "$and": [ 
			        { "$eq": ["$sType","object"]}, 
			        { "$eq": ["$tType","object"]},
				    { "$setEquals": [ {$objectToArray : "$source"}, {$objectToArray : "$target"}]}
				  ]
			    }
			  }
		    }
	      })
        }		  
		
		if ( this.parameters.SERIALIZED_JSON === true) {		
		  comparePipeline.push({
		    "$match": { 
		      "$expr": { 
		        "$not": {
		          "$and": [ 
			        { "$eq": ["$sType","object"]}, 
			        { "$eq": ["$tType","string"]},
				    { "$setEquals": [ 
					     {$objectToArray : "$source"}, 
					     {$objectToArray : {
						    $function : {
                              body: `function(jsonString) {
                                return JSON.parse(jsonString)
							  }`,
                              args: [ "$target"],
                              lang: "js"
                           }
					     }
					   }
					]}
		          ]
			    }
		      }
			}
	      })
		  comparePipeline.push({
		    "$match": { 
		      "$expr": { 
		        "$not": {
		          "$and": [ 
			        { "$eq": ["$sType","array"]}, 
			        { "$eq": ["$tType","string"]},
				    { "$setEquals": [ 
					     "$source", 
					     { $function : {
                            body: `function(jsonString) {
                              return JSON.parse(jsonString)
						    }`,
                            args: [ "$target"],
                            lang: "js"
						 }}
					]}
		          ]
			    }
		      }
			}
	      })
		}

		if ( this.parameters.SPATIAL_PRECISION !== 18) {		
		  comparePipeline.push({
            "$project" : {
		      _id    : 1,
			  key    : 1,
			  sType  : 1,
			  tType  : 1,
		      source : { $function : {
                           body: `function(obj,precision) {
                             const geomRound = (obj,precision) => {
                               switch (typeof obj) {
                                 case "number":
	                               return Number(Math.round(obj + "e" + precision) + "e-" + precision)
	                             case "object":
	                               if (Array.isArray(obj)) {
  	                                 obj.forEach((v,i) => {
	                                   obj[i] = geomRound(v,precision)
                                     })
                                   }
	                               else {
	                                 Object.keys(obj).forEach((key) => {
	                                   obj[key] = geomRound(obj[key],precision)
                                     })
	                               }
	                               return obj
	                             default:
	                               return obj
                               }
                             }
				   	 	     return geomRound(obj,precision)
					      }`,
                          args: [ "$source",this.parameters.SPATIAL_PRECISION],
                          lang: "js"
                       }},
		      target : { $function : {
                           body: `function(obj,precision) {
                             const geomRound = (obj,precision) => {
                               switch (typeof obj) {
                                 case "number":
	                               return Number(Math.round(obj + "e" + precision) + "e-" + precision)
	                             case "object":
	                               if (Array.isArray(obj)) {
  	                                 obj.forEach((v,i) => {
	                                   obj[i] = geomRound(v,precision)
                                     })
                                   }
	                               else {
	                                 Object.keys(obj).forEach((key) => {
	                                   obj[key] = geomRound(obj[key],precision)
                                     })
	                               }
   								   return obj
	                             default:
	                               return obj
                               }
                             }
							 return geomRound(obj,precision)
						   }`,
                           args: [ "$target",this.parameters.SPATIAL_PRECISION],
                           lang: "js"
					   }}                           
		    }
	      })
		  comparePipeline.push({
		    "$match": { 
		      "$expr": { 
		        "$not": {
		          "$and": [ 
			        { "$eq": ["$sType","object"]}, 
			        { "$eq": ["$tType","object"]},
				    { "$setEquals": [ {$objectToArray : "$source"}, {$objectToArray : "$target"}]}
				  ]
			    }
			  }
		    }
	      })
		}
		   
		let results;
		await this.use(sourceDB);
		const source = await this.collection(collectionName)
		results = await source.aggregate([{"$out" : { "db": this.QA_COMPARE_DBNAME, coll: "source" }}]).toArray()

		await this.use(targetDB);
		const target = await this.collection(collectionName)
		results = await target.aggregate([{"$out" : { "db": this.QA_COMPARE_DBNAME, coll: "target" }}]).toArray()
	    
		await this.use(this.QA_COMPARE_DBNAME)
		const compare = await this.collection("source")
		results = await compare.aggregate(comparePipeline).toArray()

		// await this.dropDatabase()

		return results
	}
	
	
    async compareSchemas(source,target) {

      await this.use(source.schema);
	  const sourceHash = await this.dbHash()
	  const sourceCounts = await this.getRowCounts(source)
	  await this.use(target.schema);
	  const targetHash = await this.dbHash()
      const targetCounts =  await this.getRowCounts(target)
	  
	  const mismatchedHashList = []
	  
	  const report = {
        successful : []
       ,failed     : []
      }
	  
	  Object.keys(sourceHash.collections).forEach(async (collectionName,idx) => {
		 if (targetHash.collections[collectionName] && (targetHash.collections[collectionName] === sourceHash.collections[collectionName])) {
           report.successful.push([source.schema,target.schema,collectionName,targetCounts.find(element => element[1] === collectionName)[2]])
		 }
		 else {
           if (targetHash.collections.hasOwnProperty(collectionName)) {
			 mismatchedHashList.push(collectionName)
           }
           else {
		     report.failed.push([source.schema,target.schema,collectionName, sourceCounts.find(element => element[1] === collectionName)[2],-1,sourceHash.collections[collectionName],'','Collection Not Found',null])
           }
		 }
	  })
	  
	  if ((this.parameters.EMPTY_STRING_IS_NULL === true) || (this.parameters.DOUBLE_PRECISION !== null) || (this.parameters.ORDERED_JSON === true)) {		
        for (const collectionName of mismatchedHashList) {
		  const results = await this.compareCollections(source.schema,target.schema,collectionName)
		  if (results.length === 0) {
		    report.successful.push([source.schema,target.schema,collectionName,sourceCounts.find(element => element[1] === collectionName)[2]])
		  }
		  else {
  		    // console.dir(results,{depth:null})
 	        report.failed.push([source.schema,target.schema,collectionName, sourceCounts.find(element => element[1] === collectionName)[2],targetCounts.find(element => element[1] === collectionName)[2],results.length,results.length,null,null])
		  }
	    }
	  }
	  else {
        for (const collectionName of mismatchedHashList) {
  		  const results = await this.compareCollections(source.schema,target.schema,collectionName)
 	      report.failed.push([source.schema,target.schema,collectionName, sourceCounts.find(element => element[1] === collectionName)[2],targetCounts.find(element => element[1] === collectionName)[2],results.length,results.length,null,null])
		}
	  }1
      // 'SUCCESSFUL' "RESULTS", SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
      //  'FAILED' "RESULTS", SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME,SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM "NOTES"
      
      return report
    }
      
}

module.exports = MongoQA