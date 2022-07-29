
import {
  setTimeout 
}                      from "timers/promises"

import {
  networkInterfaces 
}                      from 'os';

import mongodb from 'mongodb'
const { MongoClient } = mongodb;

import MongoDBI        from '../../../node/dbi//mongodb/mongoDBI.js';
import MongoError      from '../../../node/dbi//mongodb/mongoException.js'
import MongoConstants  from '../../../node/dbi//mongodb/mongoConstants.js';

import Yadamu          from '../../core/yadamu.js';
import YadamuQALibrary from '../../lib/yadamuQALibrary.js'

class MongoQA extends YadamuQALibrary.qaMixin(MongoDBI) {
    
	get QA_COMPARE_DBNAME() { return 'YADAMU_QA' }
    
    static #_DBI_PARAMETERS
    
    static get DBI_PARAMETERS()  { 
       this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,MongoConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[MongoConstants.DATABASE_KEY] || {},{RDBMS: MongoConstants.DATABASE_KEY}))
       return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return MongoQA.DBI_PARAMETERS
    }   
        
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }
	
    async recreateSchema() {
        await this.use(this.parameters.TO_USER)
        await this.dropDatabase()
        await this.use(this.parameters.TO_USER)
    }

    async getRowCounts(target) {

       await this.use(target.schema);
	   
       const collections = (await this.collections()).filter((collection) => {
         return ((this.TABLE_FILTER.length === 0) || (this.TABLE_FILTER.includes(collection.collectionName)))
	   })
	
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
      	const operation = `${compare.collectionName}.aggregate(${JSON.stringify(comparePipeline," ",2)})`
		this.SQL_TRACE.trace(this.traceMongo(operation))    
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
	  
      const sourceCounts = await this.getRowCounts(source)
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
		const sourceRowCount = sourceCounts.find((row) => { return row[1] === collectionName })[2]
		const targetRowCount = targetCounts.find((row) => { return row[1] === collectionName })[2]
	    if (sourceRowCount === targetRowCount) {
          const results = await this.compareCollections(source.schema,target.schema,collectionName,rules)
		  if (results.length === 0) {
            report.successful.push([source.schema,target.schema,collectionName,targetRowCount])
          }
          else {
            report.failed.push([source.schema,target.schema,collectionName, sourceRowCount, targetRowCount, results.length, results.length, null,null])
          }
		}
		else {
  		  report.failed.push([source.schema,target.schema,collectionName,sourceRowCount, targetRowCount, -1, -1,null,null])
		}
      }
      return report
    }

    classFactory(yadamu) {
      return new MongoQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	async getConnectionID() {
	  let stack
      let operation
		  const currentOp = {
            currentOp : true
          , $all      : false
          , $ownOps   : true
          }
		  stack = new Error().stack
          const dbAdmin = await this.client.db('admin',{returnNonCachedInstance:true});  
          operation = `mongoClient.db('admin').command(${currentOp})`
          const ops = await dbAdmin.command(currentOp)
          const cmd = ops.inprog.filter((op) => {
			 // Filter by IP Address matching value from op.networkInterfaces()
			 return op.command.hasOwnProperty('currentOp')
		  })
		  const pid = cmd[0].client
		  return pid
	}
	
	async listCurrentOps() {
	
      const currentOp = {
        currentOp : true
      , $all      : true
	  , $ownOps   : true
      }
      const dbAdmin = await this.client.db('admin',{returnNonCachedInstance:true});  
      const ops = await dbAdmin.command(currentOp)
      console.log(ops)     
    }
	
    async scheduleTermination(pid,workerId) {
      let stack
      let operation
	  const tags = this.getTerminationTags(workerId,pid)
	  this.yadamuLogger.qa(tags,`Termination Scheduled.`);
	  setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
        if (this.client !== undefined) {
		  
		  // this.listCurrentOps()
		  
		  this.yadamuLogger.log(tags,`Killing connection.`);
          const killClient = await new MongoClient(this.getMongoURL(),{ useUnifiedTopology: true});
          await killClient.connect();
          const dbAdmin = await killClient.db('admin',{returnNonCachedInstance:true});  
          const dropConnections = {
            dropConnections: 1
          , hostAndPort : [pid]
          }
		  stack = new Error().stack
          operation = `mongoClient.db('admin').command(${JSON.stringify(dropConnections)})`
          const res = await dbAdmin.command(dropConnections)
		  await killClient.close()
        }
        else {
          this.yadamuLogger.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
        }
      }).catch((e) => {
          this.yadamu.LOGGER.handleException(tags,new MongoError(this.DRIVER_ID,e,stack,operation));
      })
    }
}

export { MongoQA as default }