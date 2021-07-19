
"use strict" 
const fs = require('fs');
const util = require('util')
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const MongoClient = require('mongodb').MongoClient

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')

const MongoConstants = require('./mongoConstants.js')
const MongoError = require('./mongoException.js')
const MongoWriter = require('./mongoWriter.js');
const MongoParser = require('./mongoParser.js');
const StatementGenerator = require('./statementGenerator.js');

/*
**
**  IMPORT : Implemented in TableWriter.js. 
**           MongoDB support allows data from a relational table to be imported using one of the following mappings
**
**  DOCUMENT: The source material must consist of a single JSON Object. 
**            The export format is still an array of arrays. The Array representing each row will have one member which must be valid JSON. e.g. "CollectionName" : [[JSON],[{JSON}]]
**            If the JSON is an object the object becomes the mongo document.
**            If the JSON is a scalar or an array then then a mongo document with a single key of "yadamuValue" will be inserted. The JSON will be the value assocoated with yadamuValue
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
    
  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,MongoConstants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return MongoDBI.YADAMU_DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  get DB_VERSION()             { return this._DB_VERSION }

  // Override YadamuDBI

  get DATABASE_KEY()           { return MongoConstants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return MongoConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return MongoConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return MongoConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters
 
  get MONGO_STRIP_ID()         { return this.parameters.MONGO_STRIP_ID        || MongoConstants.MONGO_STRIP_ID}
  get DEFAULT_DATABASE()       { return this.parameters.DEFAULT_DATABASE      || MongoConstants.DEFAULT_DATABASE };
  get PORT()                   { return this.parameters.PORT                  || MongoConstants.PORT}
  get MONGO_SAMPLE_LIMIT()     { return this.parameters.MONGO_SAMPLE_LIMIT    || MongoConstants.MONGO_SAMPLE_LIMIT}
  get MONGO_STORAGE_FORMAT()   { return this.parameters.MONGO_STORAGE_FORMAT  || MongoConstants.MONGO_STORAGE_FORMAT}
  get MONGO_EXPORT_FORMAT()    { return this.parameters.MONGO_EXPORT_FORMAT   || MongoConstants.MONGO_EXPORT_FORMAT}
  get MONGO_STRIP_ID()         { return this.parameters.MONGO_STRIP_ID        || MongoConstants.MONGO_STRIP_ID}
  get MONGO_PARSE_STRINGS()    { return this.parameters.MONGO_PARSE_STRINGS  === false ? false : this.parameters.MONGO_PARSE_STRINGS || MongoConstants.MONGO_PARSE_STRINGS}
  get DEFAULT_STRING_LENGTH()  { return this.parameters.DEFAULT_STRING_LENGTH || MongoConstants.DEFAULT_STRING_LENGTH}
  get MAX_STRING_LENGTH()      { return MongoConstants.MAX_STRING_LENGTH}
  get MAX_DOCUMENT_SIZE()      { return MongoConstants.MAX_DOCUMENT_SIZE}

  get ID_TRANSFORMATION() {
    this._ID_TRANSFORMATION = this._ID_TRANSFORMATION || ((this.MONGO_STRIP_ID === true) ? 'STRIP' : 'PRESERVE')
    return this._ID_TRANSFORMATION 
  }
    
  get READ_TRANSFORMATION() { 
    this._READ_TRANSFORMATION = this._READ_TRANSFORMATION || (() => { 
      switch (true) {
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
    })();
    return this._READ_TRANSFORMATION
  }

  get WRITE_TRANSFORMATION() { 
    this._WRITE_TRANSFORMATION  = this._WRITE_TRANSFORMATION || 'ARRAY_TO_DOCUMENT';
    return this._WRITE_TRANSFORMATION 
  }
    
  constructor(yadamu,settings,parameters) {	  
    super(yadamu,settings,parameters)
  }
                                                             ;
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
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'DDL'],e)
	  results = e
    }
    return results;
  }    
   
   traceMongo(apiCall) {
    return `MongoClient.db(${this.connection.databaseName}).${apiCall}\n`
  }

  getMongoURL() {
    
    return `mongodb://${this.vendorProperties.host}:${this.vendorProperties.port}/${this.vendorProperties.database !== undefined ? this.vendorProperties.database : ''}`;
    
  }

  /*
  **
  ** Wrap all Mongo API calls used my YADAMU. Add tracing capability. No need generate meaningful stack traces, the ones generated by MongoError clases are acurate and complete
  **
  */

  async connect(options) {

    // Wrapper for client.db()
    
	let stack
    const operation = `new MongoClient.connect(${this.getMongoURL()}))\n`
    try {   
      this.status.sqlTrace.write(operation)
      let sqlStartTime = performance.now();
	  stack = new Error().stack
      this.client = new MongoClient(this.getMongoURL(),options);
      await this.client.connect();   
      this.traceTiming(sqlStartTime,performance.now())
     } catch (e) {
       throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
     }
  }

  async use(dbname,options) {

    // Wrapper for client.db(). db becomes the YADAMU connection. Needs to set this.connection for when it is used to change databases, and but also needs to return the connection for when it invoked by getConnectionFromPool()

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`mongoClient.db(${dbname})`)
	let stack
    const operation = `MongoClient.db(${dbname})\n`
    try {   
      this.status.sqlTrace.write(operation)
      options = options === undefined ? {returnNonCachedInstance:true} : (options.returnNonCachedInstance === undefined ? Object.assign (options, {returnNonCachedInstance:true} ) : options)
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      this.connection = await this.client.db(dbname,options);    
      this.traceTiming(sqlStartTime,performance.now())
      this.dbname = dbname;
      return this.connection
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }

  async command(command,options) {

    // Wrapper for db.command().

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`mongoClient.db(${dbname})`)
    
	let stack
    const operation = `command(${JSON.stringify(command)})`
    try {   
      this.status.sqlTrace.write(this.traceMongo(operation)) 
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const results = await this.connection.command(command,options);    
      this.traceTiming(sqlStartTime,performance.now())
      return results
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
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
      this.status.sqlTrace.write(this.traceMongo(operation)) 
      let sqlStartTime = performance.now();
   	  stack =  new Error().stack
      let results = await this.connection.dropDatabase() 
      this.traceTiming(sqlStartTime,performance.now())
     } catch (e) {
       throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
     }
  }

  async buildInfo() {
      
    //  Wrapper for db.admin().buildInfo()  
      
    
	let stack
    const operation = `admin().buildInfo()`
    try {
      this.status.sqlTrace.write(this.traceMongo(operation))    
      const sqlStartTime = performance.now();
      
      const results = await this.connection.admin().buildInfo()
      this.traceTiming(sqlStartTime,performance.now())
      return results
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }
  
  async stats(options) {
      
    // Wrapper for db.stats()

     
	let stack
    const operation = `stats()`
    try {  
      this.status.sqlTrace.write(this.traceMongo(operation)) 
      let sqlStartTime = performance.now();
	  stack =  new Error().stack      
      this.stats = await this.connection.stats(options);    
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }

  async createCollection(collectionName,options) {

    // Wrapper for db.createCollection()

	let stack
    const operation = `createCollection(${collectionName})`
    try {       
      this.status.sqlTrace.write(this.traceMongo(operation))    
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const collection = await this.connection.createCollection(collectionName);
      this.traceTiming(sqlStartTime,performance.now())
      return collection
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }

  async collection(collectionName,options) {

    // Wrapper for db.collection() - Note this return a Promise so has to be manually 'Promisfied'
  
    let stack   
    const operation = `collection(${collectionName})`
    try {       
      this.status.sqlTrace.write(this.traceMongo(operation))    
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const collection = await this.connection.collection(collectionName,options);
      this.traceTiming(sqlStartTime,performance.now())
      return collection
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }


  async collectionCount(collection,query,options) {

    // Wrapper for db.collection.count()
    
    
	let stack
    const operation = `collection.count(${collection.namespace})`
    try {
      this.status.sqlTrace.write(this.traceMongo(operation))    
      let sqlStartTime = performance.now();
  	  stack =  new Error().stack    
      const count = await collection.count(query,options)
      this.traceTiming(sqlStartTime,performance.now())
      return count
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }

  async collections(options)  {
  
    // Wrapper for db.collections()

    
	let stack
    const operation = `collections()`
    try {
      this.status.sqlTrace.write(this.traceMongo(operation))    
      let sqlStartTime = performance.now();
	  stack =  new Error().stack      
      const collections = await this.connection.collections(options)
      this.traceTiming(sqlStartTime,performance.now())
      return collections;
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }
  
  async listCollections(filter,options)  {
  
    // Wrapper for db.collections()

    
	let stack
    const operation = `listCollections()`
    try {
      this.status.sqlTrace.write(this.traceMongo(operation))    
      let sqlStartTime = performance.now();
	  stack =  new Error().stack      
      const collectionList = await this.connection.listCollections(filter,options).toArray()
      this.traceTiming(sqlStartTime,performance.now())
      return collectionList;
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }
  
  async insertOne(collectionName,doc,options) {

    // Wrapper for db.collection().insertOne()

    
	let stack
    const operation = `${collectionName}.insertOne()`
    try {
      this.status.sqlTrace.write(this.traceMongo(operation))    
      options = options === undefined ? {w:1} : (options.w === undefined ? Object.assign (options, {w:1} ) : options)
      let sqlStartTime = performance.now();
	  stack =  new Error().stack      
      const results = await this.connection.collection(collectionName).insertOne(doc,{w:1});
      this.traceTiming(sqlStartTime,performance.now())
      return results;
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }

  async insertMany(collectionName,array,options) {
      
    // Wrapper for db.collection().insertMany()

	let stack
    const operation = `${collectionName}.insertMany(${array.length})`
    try {
      this.status.sqlTrace.write(this.traceMongo(operation))    
      options = options === undefined ? {w:1} : (options.w === undefined ? Object.assign (options, {w:1} ) : options)
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const results = await this.connection.collection(collectionName).insertMany(array,options);
      this.traceTiming(sqlStartTime,performance.now())
      return results;
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }
    
  async testConnection(connectionProperties) {   
    super.setConnectionProperties(connectionProperties);
    // ### Test Database connection
	try {
      await this.connect()
      await this.closePool()
	} catch (e) {
      throw e;
	}
	
  }
        
  async createConnectionPool() {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`createConnectionPool()`)
      
    this.logConnectionProperties();
    const poolSize = this.yadamu.PARALLEL ? parseInt(this.yadamu.PARALLEL) + 1 : 5
    this.vendorProperties.options = typeof this.vendorProperties.options === 'object' ? this.vendorProperties.options : {}
    if (poolSize > 5) {
      this.vendorProperties.options.poolSize = poolSize
    }
    await this.connect(this.vendorProperties.options)
  }
  
  async getConnectionFromPool() {
     return await this.use(this.dbname || this.vendorProperties.database || this.DEFAULT_DATABASE)
  } 
  
  async configureConnection() {
      
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`configureConnection()`)

    this.buildInformation = await this.buildInfo()
    this._DB_VERSION = this.buildInformation.version
  }
  
  async closeConnection() {
    // this.db.close() ?
  }
  
  async closePool(options) {

   	let stack
	let operation = 'MongoClient.close()'
    try {
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      await this.client.close();   
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }

  async _reconnect() {
    // Default code for databases that support reconnection
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()

  }
  
  updateVendorProperties(vendorProperties) {

    vendorProperties.host             = this.parameters.HOSTNAME || vendorProperties.host 
    vendorProperties.port             = this.parameters.PORT     || vendorProperties.port

  }
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize() {   

    // TODO : Support for Mongo Authentication ???
	
	await super.initialize(false);   
    
    this.yadamuLogger.info([this.DATABASE_VENDOR,this.DB_VERSION,`Configuration`],`Document ID Tranformation: ${this.ID_TRANSFORMATION}.`)
    this.yadamuLogger.info([this.DATABASE_VENDOR,this.DB_VERSION,`Configuration`],`Read Tranformation: ${this.READ_TRANSFORMATION}.`)
    this.yadamuLogger.info([this.DATABASE_VENDOR,this.DB_VERSION,`Configuration`],`Write Tranformation: ${this.WRITE_TRANSFORMATION}.`)    
  }

  /*
  **
  ** Begin the current transaction
  **
  */
  
  async initializeExport() {
     super.initializeExport();
     await this.use(this.parameters.FROM_USER);
  }
  
  async initializeImport() {
     super.initializeImport();
     await this.use(this.parameters.TO_USER);
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
 
   const stats = await this.stats();

	return Object.assign(
	  super.getSystemInformation()
	, {
        buildInfo          : this.buildInformation
      , stats              : stats
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

  normalizeTypeInfo(typeInfo) {
    // Check for NULL and something. Remove NULL and returns what's left Note we _ARE_ searching for the string 'null' not the value null
	const nullIdx = typeInfo.indexOf('null')
	if (nullIdx >= 0) {
      typeInfo.splice( nullIdx, 1 );
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
    let result = s === 0 ? MongoConstants.DEFAULT_STRING_LENGTH :  Math.pow(2, Math.ceil(Math.log(s)/Math.log(2)));
    result = ((result === 4096) && (s < 4001)) ? 4000 : result
    result = ((result === 32768) && (s < 32768)) ? 32767 : result
    return result.toString()
  }

  async getSchemaInfo(keyName) {
      
    const collections = await this.collections();
    const loopStartTime = performance.now();
    const schemaInfo = await Promise.all(collections.map(async (collection,idx) => {    // const dbMetadata = await Promise.all(collections.map(async (collection) => {    
      const tableInfo = {TABLE_SCHEMA: this.connection.databaseName, TABLE_NAME: collection.collectionName, COLUMN_NAME_ARRAY: ["JSON_DATA"], DATA_TYPE_ARRAY: ["json"], SIZE_CONSTRAINT_ARRAY: [""]}
      if ((this.MONGO_STORAGE_FORMAT === 'DOCUMENT') && (this.MONGO_EXPORT_FORMAT === 'ARRAY')) {       
        let stack
        let operation
        try {
		
          /*
          **
          ** Aggreation Pipeline cannot be used to get (ordered) list of keys since $addToSet() does not maintain the order of the rows generated by the $unwind
          **

		  const keyNamePipeline = [{
			"$project" : {
			  "docAsKeyValue" : {
				"$objectToArray" : "$$ROOT"
			  }
			}
		  },{
			"$unwind" : "$docAsKeyValue"
		  },{
			"$group" : {
			  "_id"  : null
			, "keys" : {
			     "$addToSet" : "$docAsKeyValue.k"
			   }
			}
	      }]

          switch (this.MONGO_SAMPLE_LIMIT) {
			case 0:
			  break;
			case undefined:
			  keyNamePipeline.unshift({ "$sample" : { size: 1000}})
			  break;
			default:
			  keyNamePipeline.unshift({ "$sample" : { size : this.MONGO_SAMPLE_LIMIT}})
		  }
			
          operation = `db(${this.connection.databaseName}).collection(${collection.collectionName}.aggregate(${JSON.stringify(keyNamePipeline," ",2)})`
          this.status.sqlTrace.write(this.traceMongo(operation))    
          let sqlStartTime = performance.now();
    	  stack =  new Error().stack
		  
		  const keys = await collection.aggregate(keyNamePipeline).toArray()
		  this.traceTiming(sqlStartTime,performance.now())
          
		  if (keys.length > 0) {
   		    const collectionMetadata = {COLUMN_NAME_ARRAY : keys[0].keys, DATA_TYPE_ARRAY: [], SIZE_CONSTRAINT_ARRAY: []}         
		    Object.assign(tableInfo,collectionMetadata)

          **
          */		  
	
          operation = `${collection.collectionName}.mapReduce()`;
          this.status.sqlTrace.write(this.traceMongo(operation))    
          let sqlStartTime = performance.now();
    	  stack =  new Error().stack
          const collectionMetadata = await collection.mapReduce(
            // ### Do not try to use arrow functions or other ES2015+ features here...
            function () { 
              // Emit 2 entries for this collection to force the reduce function to be invoked for this key..
              if (Object.keys(keys).length === 0) {
                emit('metadata',null)
                emit('metadata',null)
              }
              Object.keys(this).forEach(function(key) { 
                if (!keys.hasOwnProperty(key)) {
                  // Add an entry for this key to the set of known keys. There will be on entry for each key found in the collection.
                  // The value associated with the dataType key will be '' or the maximum length of the dataype based on the sample size.
                  // The value for this key will be an objet containing one key for each possible data type
                  keys[key] = []
                }
              },this)
            },
            function(collectionName,values) {
              // Uncomment next line to debug map function results
              // return JSON.stringify(keys," ",2)
              const columnNames = []
              Object.keys(keys).forEach(function(key) {
                columnNames.push(key)
              })
              return {COLUMN_NAME_ARRAY : columnNames, DATA_TYPE_ARRAY: [], SIZE_CONSTRAINT_ARRAY: []}
            },
            {
              out                : { inline: 1}
             ,limit              : this.MONGO_SAMPLE_LIMIT === undefined ? 1000 : (this.MONGO_SAMPLE_LIMIT === 0 ? null : this.MONGO_SAMPLE_LIMIT)
             ,scope              : {
               keys              : {}
            }
          })
          this.traceTiming(sqlStartTime,performance.now())
		  
          if (collectionMetadata.length === 1) {
            // Uncomnent to debug MapReduce results
            // console.log(collection.collectionName,util.inspect(collectionMetadata,{depth:null}))
            Object.assign(tableInfo,collectionMetadata[0].value)
            
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

            /*
            switch (this.MONGO_SAMPLE_LIMIT) {
      		  case 0:
	    	    break;
		      case undefined:
		        dataTypePipeline.unshift({ "$sample" : { size: 1000}})
		        break;
		      default:
		        dataTypePipeline.unshift({ "$sample" : { size : this.MONGO_SAMPLE_LIMIT}})
	        }
			*/

  			operation = `${collection.collectionName}.aggregate(${JSON.stringify(dataTypePipeline," ",2)})`
            this.status.sqlTrace.write(this.traceMongo(operation))    
            let sqlStartTime = performance.now();
    	    stack =  new Error().stack
   	        const typeInformation = await collection.aggregate(dataTypePipeline).toArray();
			// console.dir(typeInformation,{depth:null})
			if (typeInformation.length > 0) {
			  tableInfo.COLUMN_NAME_ARRAY.forEach((col,idx) => {
			    let dataType = typeInformation[0][col].type.length == 1 ? typeInformation[0][col].type[0] : this.normalizeTypeInfo(typeInformation[0][col].type)
                let size = typeInformation[0][col].size
                size = dataType ===  'null' ? MongoConstants.DEFAULT_STRING_LENGTH : size                  
			    dataType = dataType ===  'null' ? 'string' : dataType                  
			    tableInfo.DATA_TYPE_ARRAY.push(dataType);
			    tableInfo.SIZE_CONSTRAINT_ARRAY.push (dataType === 'string' || dataType === 'binData' ? this.roundP2(size) : '')
			  })
            }
            else {
              tableInfo.SIZE_CONSTRAINT_ARRAY = new Array(tableInfo.COLUMN_NAME_ARRAY.length).fill('');
            }
          }
        } catch(e) {		
          throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
        }
      }
      if (this.ID_TRANSFORMATION === 'STRIP') {
        const idx = tableInfo.COLUMN_NAME_ARRAY.indexOf('_id');
        if (idx > -1) {
          tableInfo.COLUMN_NAME_ARRAY.splice(idx,1);
          tableInfo.DATA_TYPE_ARRAY.splice(idx,1);
          tableInfo.SIZE_CONSTRAINT_ARRAY.splice(idx,1);
        }
      }
	  tableInfo.JSON_KEY_NAME_ARRAY = [...tableInfo.COLUMN_NAME_ARRAY]
      return tableInfo
    }))
    // this.yadamuLogger.trace([`${this.constructor.name}.getSchemaInfo()`,],`Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - loopStartTime)}s.`)
    return schemaInfo
  }

  createParser(tableInfo) {
    tableInfo.READ_TRANSFORMATION = this.READ_TRANSFORMATION
    tableInfo.ID_TRANSFORMATION = this.ID_TRANSFORMATION
    return new MongoParser(tableInfo,this.yadamuLogger);
  }  

  generateQueryInformation(tableMetadata) {
    const tableInfo = super.generateQueryInformation(tableMetadata)
	tableInfo.SQL_STATEMENT = this.traceMongo(`${tableMetadata.TABLE_NAME}.find().stream()`); 
    return tableInfo
  }   
  
  inputStreamError(cause,sqlStatement) {
    return this.trackExceptions(new MongoError(cause,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(collectionInfo,parser) {
            
    const collectionName = collectionInfo.TABLE_NAME
    const readStream = new Readable({objectMode: true });
    readStream._read = () => {};

    let stack
    const operation = `${collectionName}.find().stream()`
    try {
      this.status.sqlTrace.write(this.traceMongo(operation))
      let sqlStartTime = performance.now();
      stack =  new Error().stack
      const mongoStream = await this.connection.collection(collectionName).find().stream();
      this.traceTiming(sqlStartTime,performance.now())
      mongoStream.on('data',(data) => {readStream.push(data)})
      mongoStream.on('end',(result) => {readStream.push(null)});
      mongoStream.on('error',(e) => {
        this.streamingStackTrace = new Error().stack;
        readStream.emit('error',e)
      });
      return readStream;      
    } catch (e) {
      throw this.trackExceptions(new MongoError(e,stack,this.traceMongo(operation)))
    }
  }      
   
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
    
  async generateStatementCache(schema) {
    return await super.generateStatementCache(StatementGenerator,schema) 
  }
  
  getOutputStream(collectionName,ddlComplete) {
     return super.getOutputStream(MongoWriter,collectionName,ddlComplete)
  }
  
  async createSchema(schema) {  
    // ### Create a Database Schema or equivilant
    await this.use(schema)
  }

  classFactory(yadamu) {
	return new MongoDBI(yadamu)
  }
  
  generateDatabaseMappings(metadata) {
    
    const dbMappings = {}

    Object.keys(metadata).forEach((table) => {
      const mappedTableName = metadata[table].tableName.indexOf('$') > -1 ? metadata[table].tableName.replace(/\$/g,'') : undefined
      if (mappedTableName) {
		this.yadamuLogger.warning([this.DATABASE_VENDOR,this.DB_VERSION,'IDENTIFIER INVALID',metadata[table].tableName],`Identifier contains invalid character "$". Identifier re-mapped as "${mappedTableName}".`)
        dbMappings[table] = {
  	      tableName : mappedTableName
		}
      }
    })
    return dbMappings;    
  }    
}

module.exports = MongoDBI
