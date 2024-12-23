
import fs                             from 'fs';

import { 
  performance 
}                                     from 'perf_hooks';
							          
/* Database Vendors API */                                    

import mongodb from 'mongodb'
const { MongoClient } = mongodb;

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                   

import Comparitor                    from './mongoCompare.js'
import DatabaseError                 from './mongoException.js'
import DataTypes 			     	 from './mongoDataTypes.js'
import Parser                        from './mongoParser.js'
import StatementGenerator            from './mongoStatementGenerator.js'
import OutputManager                 from './mongoOutputManager.js'
import Writer                        from './mongoWriter.js'
					
import MongoConstants 				 from './mongoConstants.js'

/*
**
**  IMPORT : Implemented in TableWriter.js. 
**           MongoDB support allows data from a relational table to be imported using one of the following mappings
**
**  DOCUMENT: The source material must consist of a single JSON Object. 
**            The export format is still an array of arrays. The Array representing each row will have one member which must be valid JSON. e.g. "CollectionName" : [[JSON],[{JSON}]]
**            If the JSON is an object the object becomes the mongo document.
**            If the JSON is a scalar or an array then then a mongo document with a single key of "yadamu" will be inserted. The JSON will be the value assocoated with yadamuValue
**            ### ToDo : Allow name of key to be supplied by user
**  
**  OBJECT:   The source material consists of a one or more non-JSON columns, or more than one JSON column
**            An object is geneated from each input rows. Key names are generated from the corresponding metadata, values from the row. 
**
**  ARRAY:    A object is generated from row. The object contains a single key 'yadamuRow' and it's value is the array.
**
**  BSON:     ### A BOSN object is contructed based on the available metadata. 
**  
**
**  EXPORT: Implimented in dbParser.js
**          MongoDB support allows data from a MongoDB collection to be exported using one of the following mappings
**
**  DOCUMENT: MongoDB support allows the document to be exported as a single column of JSON. The _id column can be stripped from the generated documents
**
**  ARRAY :   A deep scan of the table is performed to derive the name and datatype of each key. 
**            Keys are mapped into an Array based on the results of the deep scan.
**
*/

class MongoDBI extends YadamuDBI {
    
  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...MongoConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return MongoDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  get DATABASE_VERSION()             { return this._DATABASE_VERSION }

  // Override YadamuDBI

  get DATABASE_KEY()           { return MongoConstants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return MongoConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return MongoConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return MongoConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters
 
  get DEFAULT_DATABASE()       { return this.parameters.DEFAULT_DATABASE      || MongoConstants.DEFAULT_DATABASE };
  get PORT()                   { return this.parameters.PORT                  || MongoConstants.PORT}
  get MONGO_SAMPLE_LIMIT()     { return this.parameters.MONGO_SAMPLE_LIMIT    || MongoConstants.MONGO_SAMPLE_LIMIT}
  get MONGO_STORAGE_FORMAT()   { return this.parameters.MONGO_STORAGE_FORMAT  || MongoConstants.MONGO_STORAGE_FORMAT}
  get MONGO_EXPORT_FORMAT()    { return this.parameters.MONGO_EXPORT_FORMAT   || MongoConstants.MONGO_EXPORT_FORMAT}
  get MONGO_STRIP_ID()         { return this.parameters.MONGO_STRIP_ID       === false ? false : this.parameters.MONGO_STRIP_ID      || MongoConstants.MONGO_STRIP_ID}
  get MONGO_PARSE_STRINGS()    { return this.parameters.MONGO_PARSE_STRINGS  === false ? false : this.parameters.MONGO_PARSE_STRINGS || MongoConstants.MONGO_PARSE_STRINGS}
  get DEFAULT_STRING_LENGTH()  { return this.parameters.DEFAULT_STRING_LENGTH || MongoConstants.DEFAULT_STRING_LENGTH}
  get MAX_STRING_LENGTH()      { return MongoConstants.MAX_STRING_LENGTH}
  get MAX_DOCUMENT_SIZE()      { return MongoConstants.MAX_DOCUMENT_SIZE}

  get PASS_THROUGH_ENABLED()   { return this.yadamu.HOMOGENEOUS_OPERATION}

  get ID_TRANSFORMATION() {
    this._ID_TRANSFORMATION = this._ID_TRANSFORMATION || ((this.MONGO_STRIP_ID === true) ? 'STRIP' : 'PRESERVE')
    return this._ID_TRANSFORMATION 
  }
    
  get READ_TRANSFORMATION() { 
    this._READ_TRANSFORMATION = this._READ_TRANSFORMATION || (() => { 
      switch (true) {
		case this.PASS_THROUGH_ENABLED:
		  this._READ_TRANSFORMATION = 'PASS_THROUGH'
		  break
        case ((this.MONGO_STORAGE_FORMAT === 'DOCUMENT') && (this.MONGO_EXPORT_FORMAT === 'ARRAY')):
          this._READ_TRANSFORMATION = 'DOCUMENT_TO_ARRAY'
          break;
        case ((this.MONGO_STORAGE_FORMAT === 'ARRAY') && (this.MONGO_EXPORT_FORMAT === 'DOCUMENT')):
          this._READ_TRANSFORMATION  = 'ARRAY_TO_DOCUMENT'
          break;
        default:
          this._READ_TRANSFORMATION  = 'PRESERVE'
      } 
    return this._READ_TRANSFORMATION
    })()
    return this._READ_TRANSFORMATION
  }

  get WRITE_TRANSFORMATION() { 
    this._WRITE_TRANSFORMATION = this._WRITE_TRANSFORMATION || (this.PASS_THROUGH_ENABLED ? 'PASS_THROUGH' : this._WRITE_TRANSFORMATION || 'ARRAY_TO_DOCUMENT')
    return this._WRITE_TRANSFORMATION 
  }
      
  addVendorExtensions(connectionProperties) {

    connectionProperties.connection = `mongodb://${connectionProperties.host}:${connectionProperties.port}/${connectionProperties.database !== undefined ? connectionProperties.database : ''}`;
    return connectionProperties

  }

  constructor(yadamu,manager,connectionSettings,parameters) {	  
    super(yadamu,manager,connectionSettings,parameters)
	
	this.COMPARITOR_CLASS = Comparitor
	this.DATABASE_ERROR_CLASS = DatabaseError
    this.PARSER_CLASS = Parser
    this.STATEMENT_GENERATOR_CLASS = StatementGenerator
    this.OUTPUT_MANAGER_CLASS = OutputManager
    this.WRITER_CLASS = Writer	
    this.DATA_TYPES = DataTypes
  }
           
  async _executeDDL(collectionList) {
	  
	const existingCollections = await this.listCollections({},{nameOnly: true})
	const existsingCollectionList = Object.values(existingCollections).map((c) => { return c.name })
    const newCollectionList = collectionList.filter((collection) => {return existsingCollectionList.indexOf(collection) < 0})
	
	let results = []
	try {
      results = await Promise.all(newCollectionList.map(async (collectionName) => {
        return this.createCollection(collectionName)
      }))
    } catch (e) { 
	  this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'DDL'],e)
	  results = e
    }
    return results;
  }    
  
   async truncateTable(schema,collectionName) {	 
	await this.deleteMany(schema,collectionName)
  }
   
   traceMongo(apiCall) {
    return `MongoClient.db(${this.connection?.databaseName || this.CONNECTION_PROPERTIES.database }).${apiCall}\n`
  }

  /*
  **
  ** Wrap all Mongo API calls used my YADAMU. Add tracing capability. No need generate meaningful stack traces, the ones generated by MongoError clases are acurate and complete
  **
  */

  async connect(options) {

    // Wrapper for client.db()
	let stack
    options.useUnifiedTopology = true
    const operation = `new MongoClient.connect(${this.CONNECTION_PROPERTIES.connection}))\n`
    try {   
      this.SQL_TRACE.trace(operation)
      let sqlStartTime = performance.now()
	  stack = new Error().stack
      this.client = new MongoClient(this.CONNECTION_PROPERTIES.connection,options)
      await this.client.connect()   
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
     } catch (e) {
       throw this.getDatabaseException(e,stack,this.traceMongo(operation))
     }
  }

  async use(dbname,options) {

    // Wrapper for client.db(). db becomes the YADAMU connection. Needs to set this.connection for when it is used to change databases, and but also needs to return the connection for when it invoked by getConnectionFromPool()

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`mongoClient.db(${dbname})`)
	let stack
    const operation = `MongoClient.db(${dbname})\n`
    try {   
      this.SQL_TRACE.trace(operation)
      options = options === undefined ? {returnNonCachedInstance:true} : (options.returnNonCachedInstance === undefined ? Object.assign (options, {returnNonCachedInstance:true} ) : options)
      let sqlStartTime = performance.now()
	  stack =  new Error().stack
      this.connection = await this.client.db(dbname,options)    
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return this.connection
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }

  async command(command,options) {

    // Wrapper for db.command().

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`mongoClient.db(${dbname})`)
    
	let stack
    const operation = `command(${JSON.stringify(command)})`
    try {   
      this.SQL_TRACE.trace(this.traceMongo(operation)) 
      let sqlStartTime = performance.now()
	  stack =  new Error().stack
      const results = await this.connection.command(command,options)    
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return results
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }

  async dbHash() {
     const dbHash = {dbhash:1}
     return await this.command(dbHash)
  }

  async dropDatabase(options) {

    // Wrapper for db.dropDatabase()
    
	let stack
    const operation = `dropDatabase()`
    try {   
      this.SQL_TRACE.trace(this.traceMongo(operation)) 
      let sqlStartTime = performance.now()
   	  stack =  new Error().stack
      let results = await this.connection.dropDatabase() 
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
     } catch (e) {
       throw this.getDatabaseException(e,stack,this.traceMongo(operation))
     }
  }

  async buildInfo() {
      
    //  Wrapper for db.admin().buildInfo()  
      
    
	let stack
    const operation = `admin().buildInfo()`
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      const sqlStartTime = performance.now()
      
      const results = await this.connection.admin().buildInfo()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return results
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }

  async serverInfo() {
      
    //  Wrapper for db.admin().serverInfo()    
    
	let stack
    const operation = `admin().serverInfo()`
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      const sqlStartTime = performance.now()
      
      const results = await this.connection.admin().serverInfo()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return results
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }
  
  async stats(options) {
      
    // Wrapper for db.stats()

     
	let stack
    const operation = `stats()`
    try {  
      this.SQL_TRACE.trace(this.traceMongo(operation)) 
      let sqlStartTime = performance.now()
	  stack =  new Error().stack      
      const results = await this.connection.stats(options)    
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	  return results
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }

  async createCollection(collectionName,options) {

    // Wrapper for db.createCollection()

	let stack
    const operation = `createCollection(${collectionName})`
    try {       
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      let sqlStartTime = performance.now()
	  stack =  new Error().stack
      const collection = await this.connection.createCollection(collectionName)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return collection
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }

  async collection(collectionName,options) {

    // Wrapper for db.collection() - Note this return a Promise so has to be manually 'Promisfied'
  
    let stack   
    const operation = `collection(${collectionName})`
    try {       
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      let sqlStartTime = performance.now()
	  stack =  new Error().stack
      const collection = await this.connection.collection(collectionName,options)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return collection
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }


  async collectionCount(collection,query,options) {

    // Wrapper for db.collection.countDocuments()
    
    
	let stack
    const operation = `collection.countDocuments(${collection.namespace})`
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      let sqlStartTime = performance.now()
  	  stack =  new Error().stack    
      const count = await collection.countDocuments(query,options)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return count
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }

  async collections(options)  {
  
    // Wrapper for db.collections()

    
	let stack
    const operation = `collections()`
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      let sqlStartTime = performance.now()
	  stack =  new Error().stack      
      const collections = await this.connection.collections(options)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return collections;
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }
  
  async listCollections(filter,options)  {
  
    // Wrapper for db.collections()

    
	let stack
    const operation = `listCollections()`
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      let sqlStartTime = performance.now()
	  stack =  new Error().stack      
      const collectionList = await this.connection.listCollections(filter,options).toArray()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return collectionList;
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }
  
  async insertOne(collectionName,doc,options) {

    // Wrapper for db.collection().insertOne()

    
	let stack
    const operation = `${collectionName}.insertOne()`
    const writeConcern = options?.writeConcern || {w:1} 
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      options = options === undefined ? {writeConcern: writeConcern} : (options.writeConcern === undefined ? Object.assign (options, {writeConcern:writeConcern} ) : options)
      let sqlStartTime = performance.now()
	  stack =  new Error().stack      
      const results = await this.connection.collection(collectionName).insertOne(doc,options)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return results;
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }

  async insertMany(collectionName,array,options) {
      
    // Wrapper for db.collection().insertMany()

	let stack
    const operation = `${collectionName}.insertMany(${array.length})`
	const writeConcern = options?.writeConcern || {w:1} 
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      options = options === undefined ? {writeConcern: writeConcern} : (options.writeConcern === undefined ? Object.assign (options, {writeConcern:writeConcern} ) : options)
      let sqlStartTime = performance.now()
	  stack =  new Error().stack
      const results = await this.connection.collection(collectionName).insertMany(array,options)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return results;
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }
    
  async deleteMany(schema,collectionName,options) {
	 
    // Wrapper for db.collection().deleteMany({})

	let stack
    const operation = `${collectionName}.deletetMany({})`
	const writeConcern = options?.writeConcern || {w:1} 
    try {
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      options = options === undefined ? {writeConcern: writeConcern} : (options.writeConcern === undefined ? Object.assign (options, {writeConcern:writeConcern} ) : options)
      let sqlStartTime = performance.now()
	  stack =  new Error().stack
      const results = await this.connection.collection(collectionName).deleteMany({},options)
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return results;
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }
	
  async testConnection() {   
    // ### Test Database connection
    let stack
	try {
      stack = new Error().stack
	  await this.connect()
      await this.closePool()
	} catch (e) {
      throw this.createDatabaseError(e,stack,'testConnection.getConnection()')
	}
	
  }
        
  async createConnectionPool() {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`createConnectionPool()`)
    this.logConnectionProperties()
    const poolSize = this.yadamu.PARALLEL ? parseInt(this.yadamu.PARALLEL) + 1 : 5
    this.CONNECTION_PROPERTIES.options = typeof this.CONNECTION_PROPERTIES.options === 'object' ? this.CONNECTION_PROPERTIES.options : {}
    if (poolSize > 5) {
      this.CONNECTION_PROPERTIES.options.poolSize = poolSize
    }
    await this.connect(this.CONNECTION_PROPERTIES.options)
	// The client object is the source of connections.
	this.pool = this.client
  }
  
  async getConnectionFromPool() {
	 return await this.use(this.CURRENT_SCHEMA || this.CONNECTION_PROPERTIES.database || this.DEFAULT_DATABASE)
  } 
  
  async configureConnection() {
      
    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`configureConnection()`)

    const serverInfo = await this.serverInfo()
    this._DATABASE_VERSION = serverInfo.version
  }
  
  async closeConnection() {
    // this.db.close() 
  }
  
  async closePool(options) {
   	let stack
	let operation = 'MongoClient.close()'
    try {
      let sqlStartTime = performance.now()
	  stack =  new Error().stack
      await this.client.close()   
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }
  
  async yadamuInstanceId() {
      
	let stack
    const operation = `db().collection('system.js').mapReduce()`
    try {
	  const ydb = await this.client.db('yadamu')
      this.SQL_TRACE.trace(this.traceMongo(operation))    
      const sqlStartTime = performance.now()
   
      const results = await ydb.collection('system.js').aggregate(
	  [
       { "$match"   : { _id : "yadamu_instance_id"}},
       { "$project" : { _id : 0, instance_id : { "$function" : { body: `function(f) { return f() }`, args : ["$value"], lang: "js"}}}},
       { "$lookup"  : {
           from: "system.js",
           pipeline: [
              { "$match"   : { _id : "yadamu_installation_timestamp" } },
              { "$project" : { _id : 0, timestamp : { "$function" : { body: `function(f) { return f() }`, args : ["$value"], lang: "js"}}}}
           ],
           as: "timestamp"
       }},
       { "$project" : { _id : 0, instance_id: "$instance_id", timestamp: { $first : "$timestamp.timestamp"}}}
      ]).toArray()
	  this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      return results[0]
    } catch (e) {
      throw this.getDatabaseException(e,stack,this.traceMongo(operation))
    }
  }
      
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize() {   

    // TODO : Support for Mongo Authentication ???
	
	await super.initialize(false)   
    this.LOGGER.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`"Document ID Tranformation":"${this.ID_TRANSFORMATION}", "Read Tranformation":"${this.READ_TRANSFORMATION}","Write Tranformation":"${this.WRITE_TRANSFORMATION}"`)    
  }

  /*
  **
  ** Begin the current transaction
  **
  */
  
  async initializeExport() {
     super.initializeExport()
     await this.use(this.CURRENT_SCHEMA)
  }
  
  async initializeImport() {
     super.initializeImport()
     await this.use(this.CURRENT_SCHEMA)
  }

  // ### ToDO Support Mongo Transactions
  
  async commitTransaction() { /* OVERRIDE */ }
    
  async rollbackTransaction(cause)  { /* OVERRIDE */ }

  async createSavePoint()  { /* OVERRIDE */ }
  
  async restoreSavePoint(cause)  { /* OVERRIDE */ }

  async releaseSavePoint(cause)  { /* OVERRIDE */ }
  
  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
  
  /*
  **
  **  Generate the SystemInformation object for an Export operation
  **
  */
  
  async getSystemInformation() {     
 
 
   const results = await this.yadamuInstanceId()

   const stats = await this.stats()
   const serverInfo = await this.serverInfo()
   const buildInfo = await this.buildInfo()
   const clientMetadata = this.client.topology.s.options.metadata;
   // Object.getOwnPropertySymbols(this.client).forEach((k) => {console.log(this.client[k]); clientMetadata = clientMetadata || (this.client[k]?.metadata)})
   return Object.assign(
	  super.getSystemInformation()
	, {
	    yadamuInstanceID            : results.instance_id
	  , yadamuInstallationTimestamp : results.timestamp
      , buildInfo                   : buildInfo
	  , serverInfo                  : serverInfo
      , stats                       : stats
	  , clientMetadata              : clientMetadata
      }
	)
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    return undefined
  }

  coalesceTypeInfo(typeInfo) {
    // Check for NULL and something. Remove NULL and returns what's left Note we _ARE_ searching for the string 'null' not the value null
	const nullIdx = typeInfo.indexOf('null')
	if (nullIdx >= 0) {
      typeInfo.splice( nullIdx, 1 )
	  if (typeInfo.length === 1) {
		return typeInfo[0]
	  }
	}
	// if one of the possible types is string then it's string
	if (typeInfo.includes('string')) {
	   return 'string'
	}
	if (typeInfo.includes('object')) {
	   return 'object'
	}
	// Numerics in the following order decimal, double, long, int
	if (typeInfo.includes('decimal'))  {
	  return 'decimal'
	}
	if (typeInfo.includes('double'))  {
	  return 'double'
	}
	if (typeInfo.includes('long'))  {
	  return 'long'
	}
	if (typeInfo.includes('int'))  {
	  return 'int'
	}	
	
	return 'object'
  }	  

  roundP2(s) {
    let result = s === 0 ? MongoConstants.DEFAULT_STRING_LENGTH :  Math.pow(2, Math.ceil(Math.log(s)/Math.log(2)))
    result = ((result === 4096) && (s < 4001)) ? 4000 : result
    result = ((result === 32768) && (s < 32768)) ? 32767 : result
    return result
  }

  async getSchemaMetadata() {
      
    const collections = (await this.collections()).filter((collection) => {
      return ((this.TABLE_FILTER.length === 0) || (this.TABLE_FILTER.includes(collection.collectionName)))
	})
	
    const loopStartTime = performance.now()
    const schemaInfo = await Promise.all(collections.map(async (collection,idx) => {    // const dbMetadata = await Promise.all(collections.map(async (collection) => {    
      const tableInfo = {TABLE_SCHEMA: this.connection.databaseName, TABLE_NAME: collection.collectionName, COLUMN_NAME_ARRAY: ["DOCUMENT"], DATA_TYPE_ARRAY: ["json"], SIZE_CONSTRAINT_ARRAY: [[]]}
      if ((this.MONGO_STORAGE_FORMAT === 'DOCUMENT') && (this.MONGO_EXPORT_FORMAT === 'ARRAY')) {       
        let stack
        let operation
        try {
			         
		  let sampleSize 
          switch (this.MONGO_SAMPLE_LIMIT) {
            case 0:
	    	  break;
		    case undefined:
		      sampleSize = 1000
		      break;
		    default:
		      sampleSize : this.MONGO_SAMPLE_LIMIT
	      }

          const keyNameAggregation = [
            {"$project":{"arrayofkeyvalue":{"$objectToArray":"$$ROOT"}}},
            {"$unwind":"$arrayofkeyvalue"},
            {"$group":{"_id":collection.collectionName,"columnNames":{"$addToSet":"$arrayofkeyvalue.k"}}}
		  ]

          if (this.MONGO_SAMPLE_LIMIT !== 0) {
		    keyNameAggregation.push( {"$sample" : { "size" : this.MONGO_SAMPLE_LIMIT || 1000 }})
		  }
		  
		  operation = `${collection.collectionName}.aggregate()`;
          this.SQL_TRACE.trace(this.traceMongo(operation))    
          let sqlStartTime = performance.now()
    	  stack =  new Error().stack
          const columnNames = await collection.aggregate(keyNameAggregation).toArray()
          this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		  
          if (columnNames.length === 1 ) {
            // Uncomnent to debug MapReduce results
            // console.log(collection.collectionName,util.inspect(collectionMetadata,{depth:null}))
            tableInfo.COLUMN_NAME_ARRAY = columnNames[0].columnNames
			
			const dataTypePipeline = [{ 
              $group : {
              "_id": null
              }
            },{
              $project : {
              }
            }]
            for (const i in tableInfo.COLUMN_NAME_ARRAY) {
              const col_type = `C${i}_type`
              const col_size = `C${i}_size`
              const col_name = tableInfo.COLUMN_NAME_ARRAY[i]
              dataTypePipeline[0]["$group"][col_type] = {
                "$addToSet": {
                   "$type" : `$${col_name}`
                }
              }
              dataTypePipeline[0]["$group"][col_size] = {
                "$max": {
                   "$switch": {
                      branches: [
                          { case: { $eq: [{"$type" : `$${col_name}`} , 'string' ] }, then: {"$strLenBytes": { $ifNull: [`$${col_name}`,""]}}},
                          { case: { $eq: [{"$type" : `$${col_name}`} , 'binData' ] }, then: {"$binarySize": { $ifNull: [`$${col_name}`,""]}}}
                      ],
                      default : 0
                   }
                }
              }
              dataTypePipeline[1]["$project"][col_name] = {
                  type : `$${col_type}`
                 ,size : `$${col_size}`
              }           
            }

  			operation = `${collection.collectionName}.aggregate(${JSON.stringify(dataTypePipeline," ",2)})`
            this.SQL_TRACE.trace(this.traceMongo(operation))    
            let sqlStartTime = performance.now()
    	    stack =  new Error().stack
   	        const typeInformation = await collection.aggregate(dataTypePipeline).toArray()
			// console.dir(typeInformation,{depth:null})
			if (typeInformation.length > 0) { 
			  tableInfo.DATA_TYPE_ARRAY.length = 0
			  tableInfo.SIZE_CONSTRAINT_ARRAY.length = 0 
			  tableInfo.COLUMN_NAME_ARRAY.forEach((col,idx) => {
				const colTypeInformation = typeInformation[0][col]
			    let dataType = colTypeInformation.type.length == 1 ? colTypeInformation.type[0] : this.coalesceTypeInfo(colTypeInformation.type)
				switch (dataType) {
				   case 'null':
			         dataType = 'string' 
                     tableInfo.SIZE_CONSTRAINT_ARRAY.push([MongoConstants.DEFAULT_STRING_LENGTH]) 
					 break
				   case 'string':
				   case 'binData':
				     tableInfo.SIZE_CONSTRAINT_ARRAY.push([this.roundP2(colTypeInformation.size)])
					 break
				   case 'objectId':
				     tableInfo.SIZE_CONSTRAINT_ARRAY.push([12]) 
					 break
				   case 'int':
				   case 'long':
				   case 'decimal':
				   case 'double':
				   case 'bool':
				   case 'object':
				   case 'array':
				     tableInfo.SIZE_CONSTRAINT_ARRAY.push([]) 
					 break
				   default:
				     tableInfo.SIZE_CONSTRAINT_ARRAY.push([colTypeInformation.size])
				}
			    tableInfo.DATA_TYPE_ARRAY.push(dataType)
			  })
            }
            else {
              tableInfo.SIZE_CONSTRAINT_ARRAY = new Array(tableInfo.COLUMN_NAME_ARRAY.length).fill([])
            }
          }
        } catch(e) {		
          throw this.getDatabaseException(e,stack,this.traceMongo(operation))
        }
      }
	  
	  // console.dir(tableInfo,{depth:null})
	  
      if (this.ID_TRANSFORMATION === 'STRIP') {
        const idx = tableInfo.COLUMN_NAME_ARRAY.indexOf('_id')
        if (idx > -1) {
          tableInfo.COLUMN_NAME_ARRAY.splice(idx,1)
          tableInfo.DATA_TYPE_ARRAY.splice(idx,1)
          tableInfo.SIZE_CONSTRAINT_ARRAY.splice(idx,1)
        }
      }
	  
	  // Suppress Column Ordering
	  tableInfo.SKIP_COLUMN_REORDERING = true
	  return tableInfo
    }))
    // this.LOGGER.trace([`${this.constructor.name}.getSchemaMetadata()`,],`Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - loopStartTime)}s.`)
	
	// console.dir(schemaInfo,{depth:null})
    return schemaInfo
  }

  getParser(tableInfo,pipelineState) {
    tableInfo.READ_TRANSFORMATION = this.READ_TRANSFORMATION
    tableInfo.ID_TRANSFORMATION = this.ID_TRANSFORMATION
    return super.getParser(tableInfo,pipelineState)
  }  

  generateQueryInformation(tableMetadata) {
    const tableInfo = super.generateQueryInformation(tableMetadata)
	tableInfo.SQL_STATEMENT = this.traceMongo(`${tableMetadata.TABLE_NAME}.find().stream()`) 
    return tableInfo
  }   
  
  async _getInputStream(collectionInfo,parser) {
            
    const collectionName = collectionInfo.TABLE_NAME
    const operation = `${collectionName}.find().stream()`

	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.trace(this.traceMongo(operation))
		stack = new Error().stack
        const sqlStartTime = performance.now()
        const mongoStream = await this.connection.collection(collectionName).find().stream()
	    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return mongoStream;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,sqlStatement)
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          continue;
        }
        throw cause
      }      
    } 	
  }       
   
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
      
  async createSchema(schema) {  
    // ### Create a Database Schema or equivilant
    await this.use(schema)
  }

  async setWorkerConnection() {    
    // ### Should worker share client with manager or create a new one ?
    this.client = this.manager.client
	await this.getConnectionFromPool()
  }

  classFactory(yadamu) {
	return new MongoDBI(yadamu)
  }
  
  generateDatabaseMappings(metadata) {
    
    const dbMappings = {}

    Object.keys(metadata).forEach((table) => {
      const mappedTableName = metadata[table].tableName.indexOf('$') > -1 ? metadata[table].tableName.replace(/\$/g,'') : undefined
      if (mappedTableName) {
		this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE,this.DATABASE_VERSION,'IDENTIFIER INVALID',metadata[table].tableName],`Identifier contains invalid character "$". Identifier re-mapped as "${mappedTableName}".`)
        dbMappings[table] = {
  	      tableName : mappedTableName
		}
      }
    })
    return dbMappings;    
  } 
  
}

export { MongoDBI as default }
