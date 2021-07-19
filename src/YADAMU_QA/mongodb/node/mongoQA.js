"use strict" 

const MongoDBI = require('../../../YADAMU/mongodb/node/mongoDBI.js');
const {MongodbError, ConnectionError, DatabaseError} = require('../../../YADAMU/mongodb/node/mongoException.js')
const MongoConstants = require('../../../YADAMU/mongodb/node/mongoConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class MongoQA extends MongoDBI {
    
    get QA_COMPARE_DBNAME() { return 'YADAMU_QA' }
    
    static #_YADAMU_DBI_PARAMETERS
    
    static get YADAMU_DBI_PARAMETERS()  { 
       this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,MongoConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[MongoConstants.DATABASE_KEY] || {},{RDBMS: MongoConstants.DATABASE_KEY}))
       return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return MongoQA.YADAMU_DBI_PARAMETERS
    }   
        
    constructor(yadamu,settings,parameters) {
       super(yadamu,settings,parameters)
    }

    setMetadata(metadata) {
      super.setMetadata(metadata)
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

    async recreateDatabase() {
        await this.use(this.parameters.TO_USER)
        await this.dropDatabase()
        await this.use(this.parameters.TO_USER)
    }

    async getRowCounts(target) {
       await this.use(target.schema);
       const collections = await this.collections();
       const results = await Promise.all(collections.map(async (collection) => {
         return [target.schema,collection.collectionName,await this.collectionCount(collection)]
       }))
       return results;
    }
    
    async compareCollections(sourceDB,targetDB,collectionName,rules) {
            
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
     
        if ( rules.EMPTY_STRING_IS_NULL === true) {
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
        
        if ( rules.INFINITY_IS_NULL === true) {
          comparePipeline.push({
            "$match" : { 
              "$expr": { 
                "$not": {
                  "$and": [ 
                    { "$eq": ["$source",Infinity]}, 
                    { "$eq": ["$target",null]}
                  ]
                }
              }
            }
          })
        }

        if ( rules.INFINITY_IS_NULL === true) {
          comparePipeline.push({
            "$match" : { 
              "$expr": { 
                "$not": {
                  "$and": [ 
                    { "$eq": ["$source",-Infinity]}, 
                    { "$eq": ["$target",null]}
                  ]
                }
              }
            }
          })
        }
        
        if ( rules.INFINITY_IS_NULL === true) {
          comparePipeline.push({
            "$match" : { 
              "$expr": { 
                "$not": {
                  "$and": [ 
                    { "$eq": ["$source",NaN]}, 
                    { "$eq": ["$target",null]}
                  ]
                }
              }
            }
          })
        }

        if ( rules.DOUBLE_PRECISION !== null) {     
          comparePipeline.push({
            "$match": { 
              "$expr": { 
                "$not": {
                  "$and": [ 
                    { "$eq": ["$sType","double"]}, 
                    { "$eq": ["$tType","double"]},
                    { "$eq": [
                         {"$round": ["$source",rules.DOUBLE_PRECISION]},
                         {"$round": ["$target",rules.DOUBLE_PRECISION]} 
                    ]}
                  ]
                }
              }
            }
          })
        }
       
	   function sortKeys(x) {
}

	   
        if ( rules.ORDERED_JSON === true) {     
          comparePipeline.push({
            "$match": { 
              "$expr": { 
                "$not": {
                  "$and": [ 
                    { "$eq": ["$sType","object"]}, 
                    { "$eq": ["$tType","object"]},
                    { "$setEquals": [{ $objectToArray : {
						                 $function : {
						                    body: `function(obj) {
										             const sortKeys = (j) => {
                                                       if (typeof j !== 'object' || !j)
                                                         return j;
                                                       if (Array.isArray(j))
                                                         return j.map(sortKeys);
                                                       return Object.keys(j).sort().reduce((o, k) => ({...o, [k]: sortKeys(j[k])}), {});
												     }
													 return sortKeys(obj)
												   }`,
											args: [ "$source" ],
											lang: "js"
										 }
					                   }
					                 },
                                     {  $objectToArray : {
						                 $function : {
						                    body: `function(obj) {
										             const sortKeys = (j) => {
                                                       if (typeof j !== 'object' || !j)
                                                         return j;
                                                       if (Array.isArray(j))
                                                         return j.map(sortKeys);
                                                       return Object.keys(j).sort().reduce((o, k) => ({...o, [k]: sortKeys(j[k])}), {});
												     }
													 return sortKeys(obj)
												   }`,
											args: [ "$source" ],
											lang: "js"
										 }
					                   }
									 }									 
								    ]
                    }
				  ]
                }
              }
			}
          })
        }         
        
        if ( rules.SERIALIZED_JSON === true) {      
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

        if ( rules.SPATIAL_PRECISION !== 18) {      
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
                                     if (obj !== null) {
                                       Object.keys(obj).forEach((key) => {
                                         obj[key] = geomRound(obj[key],precision)
                                       })
                                     }
                                   }
                                   return obj
                                 default:
                                   return obj
                               }
                             }
                             return geomRound(obj,precision)
                          }`,
                          args: [ "$source",rules.SPATIAL_PRECISION],
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
                                     if (obj !== null) {
                                       Object.keys(obj).forEach((key) => {
                                         obj[key] = geomRound(obj[key],precision)
                                       })
                                     }
                                   }
                                   return obj
                                 default:
                                   return obj
                               }
                             }
                             return geomRound(obj,precision)
                           }`,
                           args: [ "$target",rules.SPATIAL_PRECISION],
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
    
    
    async compareSchemas(source,target,rules) {
      
      await this.use(source.schema);
      const sourceHash = await this.dbHash()
      const sourceHashValues = sourceHash.collections;
      if (this.TABLE_FILTER.length > 0) {
        Object.keys(sourceHashValues).forEach((collectionName) => {
          if (!this.TABLE_FILTER.includes(collectionName)) {
            delete sourceHashValues[collectionName]
          }
        })
      }
	  
      const sourceCounts = await this.getRowCounts(source)
      
      
      await this.use(target.schema);
      const targetHash = await this.dbHash()
      const targetHashValues = targetHash.collections;
      if (this.TABLE_FILTER.length > 0) {
        Object.keys(targetHashValues).forEach((collectionName) => {
          if (!this.TABLE_FILTER.includes(collectionName)) {
            delete targetHashValues[collectionName]
          }
        })
      }

      const targetCounts =  await this.getRowCounts(target)
      
      const mismatchedHashList = []
      
      const report = {
        successful : []
       ,failed     : []
      }
      
      Object.keys(sourceHashValues).forEach(async (collectionName,idx) => {
         if (targetHashValues[collectionName] && (targetHashValues[collectionName] === sourceHashValues[collectionName])) {
           report.successful.push([source.schema,target.schema,collectionName,targetCounts.find(element => element[1] === collectionName)[2]])
         }
         else {
           if (targetHashValues.hasOwnProperty(collectionName)) {
             mismatchedHashList.push(collectionName)
           }
           else {
             report.failed.push([source.schema,target.schema,collectionName, sourceCounts.find(element => element[1] === collectionName)[2],-1,sourceHashValues[collectionName],'','Collection Not Found',null])
           }
         }
      })
      
      for (const collectionName of mismatchedHashList) {
        const results = await this.compareCollections(source.schema,target.schema,collectionName,rules)
        if (results.length === 0) {
          report.successful.push([source.schema,target.schema,collectionName,sourceCounts.find(element => element[1] === collectionName)[2]])
        }
        else {
          // console.dir(results,{depth:null})
          report.failed.push([source.schema,target.schema,collectionName, sourceCounts.find(element => element[1] === collectionName)[2],targetCounts.find(element => element[1] === collectionName)[2],results.length,results.length,null,null])
        }
      }
      return report
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

}

module.exports = MongoQA