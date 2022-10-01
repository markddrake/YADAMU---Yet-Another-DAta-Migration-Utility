
import YadamuCompare   from '../base/yadamuCompare.js'

class MongoCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async getRowCounts(target) {

       await this.dbi.use(target);
	   
       const collections = (await this.dbi.collections()).filter((collection) => {
         return ((this.dbi.TABLE_FILTER.length === 0) || (this.dbi.TABLE_FILTER.includes(collection.collectionName)))
	   })
	
       const results = await Promise.all(collections.map(async (collection) => {
  	     return [target,collection.collectionName,await this.dbi.collectionCount(collection)]
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
        await this.dbi.use(sourceDB);
        const source = await this.dbi.collection(collectionName)
        results = await source.aggregate([{"$out" : { "db": this.dbi.QA_COMPARE_DBNAME, coll: "source" }}]).toArray()

        await this.dbi.use(targetDB);
        const target = await this.dbi.collection(collectionName)
        results = await target.aggregate([{"$out" : { "db": this.dbi.QA_COMPARE_DBNAME, coll: "target" }}]).toArray()
        
        await this.dbi.use(this.dbi.QA_COMPARE_DBNAME)
        const compare = await this.dbi.collection("source")
      	const operation = `${compare.collectionName}.aggregate(${JSON.stringify(comparePipeline," ",2)})`
		this.dbi.SQL_TRACE.trace(this.dbi.traceMongo(operation))    
        results = await compare.aggregate(comparePipeline).toArray()
        
        // await this.dropDatabase()
        return results
    }
    
    
    async compareSchemas(source,target,rules) {
      
      await this.dbi.use(source);
      const sourceHash = await this.dbi.dbHash()
      const sourceHashValues = sourceHash.collections;
      if (this.dbi.TABLE_FILTER.length > 0) {
        Object.keys(sourceHashValues).forEach((collectionName) => {
          if (!this.dbi.TABLE_FILTER.includes(collectionName)) {
            delete sourceHashValues[collectionName]
          }
        })
      }
	  
      await this.dbi.use(target);
      const targetHash = await this.dbi.dbHash()
      const targetHashValues = targetHash.collections;
      if (this.dbi.TABLE_FILTER.length > 0) {
        Object.keys(targetHashValues).forEach((collectionName) => {
          if (!this.dbi.TABLE_FILTER.includes(collectionName)) {
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
           report.successful.push([source,target,collectionName,targetCounts.find(element => element[1] === collectionName)[2]])
         }
         else {
           if (targetHashValues.hasOwnProperty(collectionName)) {
             mismatchedHashList.push(collectionName)
           }
           else {
             report.failed.push([source,target,collectionName, sourceCounts.find(element => element[1] === collectionName)[2],-1,sourceHashValues[collectionName],'','Collection Not Found',null])
           }
         }
      })
	  
      for (const collectionName of mismatchedHashList) {
		const sourceRowCount = sourceCounts.find((row) => { return row[1] === collectionName })[2]
		const targetRowCount = targetCounts.find((row) => { return row[1] === collectionName })[2]
	    if (sourceRowCount === targetRowCount) {
          const results = await this.compareCollections(source,target,collectionName,rules)
		  if (results.length === 0) {
            report.successful.push([source,target,collectionName,targetRowCount])
          }
          else {
            report.failed.push([source,target,collectionName, sourceRowCount, targetRowCount, results.length, results.length, null,null])
          }
		}
		else {
  		  report.failed.push([source,target,collectionName,sourceRowCount, targetRowCount, -1, -1,null,null])
		}
      }
      return report
    }
}

export { MongoCompare as default }
