
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

const Yadamu = require('../../common/yadamu.js');
const YadamuDBI =  require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')
const MongoConstants = require('./mongoConstants.js')
const MongoError = require('./mongoError.js')
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
    
  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
 
  get DB_VERSION()             { return this._DB_VERSION }

  // Override YadamuDBI

  get DATABASE_VENDOR()        { return MongoConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return MongoConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return MongoConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters
 
  get SPATIAL_FORMAT()         { return this.parameters.SPATIAL_FORMAT        || MongoConstants.SPATIAL_FORMAT };
  get PORT()                   { return this.parameters.PORT                  || MongoConstants.PORT}
  get MONGO_SAMPLE_LIMIT()     { return this.parameters.MONGO_SAMPLE_LIMIT    || MongoConstants.MONGO_SAMPLE_LIMIT}
  get MONGO_STORAGE_FORMAT()   { return this.parameters.MONGO_STORAGE_FORMAT  || MongoConstants.MONGO_STORAGE_FORMAT}
  get MONGO_EXPORT_FORMAT()    { return this.parameters.MONGO_EXPORT_FORMAT   || MongoConstants.MONGO_EXPORT_FORMAT}
  get MONGO_STRIP_ID()         { return this.parameters.MONGO_STRIP_ID        || MongoConstants.MONGO_STRIP_ID}
  get DEFAULT_STRING_LENGTH()  { return this.parameters.DEFAULT_STRING_LENGTH || MongoConstants.DEFAULT_STRING_LENGTH}
  get MAX_STRING_LENGTH()      { return this.parameters.MAX_STRING_LENGTH     || MongoConstants.MAX_STRING_LENGTH}

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
    

  constructor(yadamu) {	  
    super(yadamu,MongoConstants.DEFAULT_PARAMETERS)
  }
                                                             ;
  async executeDDLImpl(collectionList) {
    const results = await Promise.all(collectionList.map(async (collectionName) => {
      await this.createCollection(collectionName)
    }));

  }    
   
   traceMongo(apiCall) {
    return `MongoClient.db(${this.connection.databaseName}).${apiCall}\n`
  }

  getMongoURL() {
    
    return `mongodb://${this.connectionProperties.host}:${this.connectionProperties.port}/${this.connectionProperties.database !== undefined ? this.connectionProperties.database : ''}`;
    
  }

  /*
  **
  ** Wrap all Mongo API calls used my YADAMU. Add tracing capability. No need generate meaningful stack traces, the ones generated by MongoError clases are acurate and complete
  **
  */

  async connect(options) {

    // Wrapper for client.db()
    
	let stack
    const operation = `new MongoClient.connect ${this.getMongoURL()}))\n`
    try {   
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(operation)
      }
      let sqlStartTime = performance.now();
	  stack = new Error().stack
      this.client = new MongoClient(this.getMongoURL(),options);
      await this.client.connect();   
      this.traceTiming(sqlStartTime,performance.now())
     } catch (e) {
       throw this.captureException(new MongoError(e,stack,operation))
     }
  }

  async use(dbname,options) {

    // Wrapper for client.db(). db becomes the YADAMU connection. Needs to set this.connection for when it is used to change databases, and but also needs to return the connection for when it invoked by getConnectionFromPool()

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`mongoClient.db(${dbname})`)
    
	let stack
    const operation = `client.db(${dbname})\n`
    try {   
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(operation)
      }
      options = options === undefined ? {returnNonCachedInstance:true} : (options.returnNonCachedInstance === undefined ? Object.assign (options, {returnNonCachedInstance:true} ) : options)
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      this.connection = await this.client.db(dbname,options);    
      this.traceTiming(sqlStartTime,performance.now())
      this.dbname = dbname;
      return this.connection
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }

  async command(command,options) {

    // Wrapper for db.command().

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`mongoClient.db(${dbname})`)
    
	let stack
    const operation = `command(${JSON.stringify(command)})`
    try {   
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation)) 
      }
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const results = await this.connection.command(command,options);    
      this.traceTiming(sqlStartTime,performance.now())
      return results
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
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
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation)) 
      }
      let sqlStartTime = performance.now();
   	  stack =  new Error().stack
      await this.connection.dropDatabase() 
      this.traceTiming(sqlStartTime,performance.now())
     } catch (e) {
       throw this.captureException(new MongoError(e,stack,operation))
     }
  }

  async buildInfo() {
      
    //  Wrapper for db.admin().buildInfo()  
      
    
	let stack
    const operation = `admin().buildInfo()`
    try {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))    
      }
      const sqlStartTime = performance.now();
      
      const results = await this.connection.admin().buildInfo()
      this.traceTiming(sqlStartTime,performance.now())
      return results
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }
  
  async stats(options) {
      
    // Wrapper for db.stats()

     
	let stack
    const operation = `stats()`
    try {  
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation)) 
      }
      let sqlStartTime = performance.now();
	  stack =  new Error().stack      
      this.stats = await this.connection.stats(options);    
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }

  async createCollection(collectionName,options) {

    // Wrapper for db.createCollection()

	let stack
    const operation = `createCollection(${collectionName})`
    try {       
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))    
      }      
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const collection = await this.connection.createCollection(collectionName);
      this.traceTiming(sqlStartTime,performance.now())
      return collection
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }

  async collection(collectionName,options) {

    // Wrapper for db.collection() - Note this return a Promise so has to be manually 'Promisfied'
  
    let stack   
    const operation = `collection(${collectionName})`
    try {       
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))    
      }      
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const collection = await this.connection.collection(collectionName,options);
      this.traceTiming(sqlStartTime,performance.now())
      return collection
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }


  async collectionCount(collection,query,options) {

    // Wrapper for db.collection.count()
    
    
	let stack
    const operation = `collection.count(${collection.namespace})`
    try {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))    
      }      
      let sqlStartTime = performance.now();
  	  stack =  new Error().stack    
      const count = await collection.count(query,options)
      this.traceTiming(sqlStartTime,performance.now())
      return count
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }

  async collections(options)  {
  
    // Wrapper for db.collections()

    
	let stack
    const operation = `collections()`
    try {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))    
      }
      let sqlStartTime = performance.now();
	  stack =  new Error().stack      
      const collections = await this.connection.collections(options)
      this.traceTiming(sqlStartTime,performance.now())
      return collections;
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }
  
  async insertOne(collectionName,doc,options) {

    // Wrapper for db.collection().insertOne()

    
	let stack
    const operation = `collection(${collectionName}).insertOne()`
    try {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))    
      }
      options = options === undefined ? {w:1} : (options.w === undefined ? Object.assign (options, {w:1} ) : options)
      let sqlStartTime = performance.now();
	  stack =  new Error().stack      
      const results = await this.connection.collection(collectionName).insertOne(doc,{w:1});
      this.traceTiming(sqlStartTime,performance.now())
      return results;
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }

  async insertMany(collectionName,array,options) {
      
    // Wrapper for db.collection().insertMany()

	let stack
    const operation = `collection(${collectionName}).insertMany(${array.length})`
    try {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))    
      }
      options = options === undefined ? {w:1} : (options.w === undefined ? Object.assign (options, {w:1} ) : options)
      let sqlStartTime = performance.now();
	  stack =  new Error().stack
      const results = await this.connection.collection(collectionName).insertMany(array,options);
      this.traceTiming(sqlStartTime,performance.now())
      return results;
    } catch (e) {
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }


  validateIdentifiers(metadata) {
        
    // ### Todo Add better algorthim than simply stripping the invalid characters from the name
    // E.g. Check for Duplicates and use counter when duplicates are detected.
    
    const tableMappings = {}
    let mapTables = false;
    const tables = Object.keys(metadata)    
    tables.forEach((table,idx) => {
      const tableName = metadata[table].tableName
      if (tableName.indexOf('$') > -1) {
        mapTables = true;
        const newTableName = tableName.replace(/\$/g,'')
        this.yadamuLogger.warning([this.DATABASE_VENDOR,tableName],`Mapped to "${newTableName}".`)
        tableMappings[table] = {tableName : newTableName}
        metadata[table].tableName = newTableName;
      }
    })        
    return mapTables ? tableMappings : undefined
  }
    
  async testConnection(connectionProperties) {   
    super.setConnectionProperties(connectionProperties);
    // ### Test Database connection
  }
        
  async createConnectionPool() {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`createConnectionPool()`)
      
    this.logConnectionProperties();
    const poolSize = this.yadamu.PARALLEL ? parseInt(this.yadamu.PARALLEL) + 1 : 5
    this.connectionProperties.options = typeof this.connectionProperties.options === 'object' ? this.connectionProperties.options : {}
    if (poolSize > 5) {
      this.connectionProperties.options.poolSize = poolSize
    }
    await this.connect(this.connectionProperties.options)
  }
  
  async getConnectionFromPool() {
     return await this.use(this.dbname)
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
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }

  async reconnectImpl() {
    // Default code for databases that support reconnection
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()

  }
  
  getConnectionProperties() {
  
    return{
      host             : this.parameters.HOSTNAME
     ,port             : this.parameters.PORT
    }
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

  async finalize() {
    await super.finalize()
  } 

  /*
  **
  **  Abort the database connection and pool.
  **
  */

  async abort() {
                                       
    await super.abort();
      
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
  
  /*
  **
  ** Begin a transaction
  **
  */
  
  async beginTransaction() {

     // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
     super.beginTransaction();
                            
  }

  // ### ToDO Support Mongo Transactions
  
  async commitTransaction() {
  }
    
  async rollbackTransaction(cause) {
  }

  async createSavePoint() {
  }
  
  async restoreSavePoint(cause) {
  }  

  async releaseSavePoint(cause) {
  } 

  /*
  **
  **  Upload a JSON File to the server. Optionally return a handle that can be used to process the file
  **
  */
  
  async uploadFile(importFilePath) {
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */

  async processFile(hndl) {
  }
  
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
   return {
     date               : new Date().toISOString()
    ,timeZoneOffset     : new Date().getTimezoneOffset()
    ,vendor             : this.DATABASE_VENDOR
    ,spatialFormat      : this.SPATIAL_FORMAT 
    ,schema             : this.parameters.FROM_USER
    ,softwareVendor     : this.SOFTWARE_VENDOR
    ,exportVersion      : Yadamu.EXPORT_VERSION
    ,nodeClient         : {
       version              : process.version
      ,architecture     : process.arch
      ,platform         : process.platform
     }
    ,buildInfo          : this.buildInformation
    ,stats              : stats
  }
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
    let result = s === 0 ? this.MIN_STRING_LENGTH :  Math.pow(2, Math.ceil(Math.log(s)/Math.log(2)));
    result = ((result === 4096) && (s < 4001)) ? 4000 : result
    result = ((result === 32768) && (s < 32768)) ? 32767 : result
    return result
  }

  async getSchemaInfo(keyName) {
      
    const collections = await this.collections();
    const loopStartTime = performance.now();
    const schemaInfo = await Promise.all(collections.map(async (collection,idx) => {    // const dbMetadata = await Promise.all(collections.map(async (collection) => {    
      const tableInfo = {TABLE_SCHEMA: this.connection.databaseName, TABLE_NAME: collection.collectionName, COLUMN_NAME_ARRAY: ["JSON_DATA"], DATA_TYPE_ARRAY: ["JSON"], SIZE_CONSTRAINT_ARRAY: [""]}
      if ((this.MONGO_STORAGE_FORMAT === 'DOCUMENT') && (this.MONGO_EXPORT_FORMAT === 'ARRAY')) {       
        let stack
        let operation
		let aggPipeline
        try {
          operation = `db(${this.connection.databaseName}).collection(${collection.collectionName}.mapReduce()`;
          if (this.status.sqlTrace) {
            this.status.sqlTrace.write(this.traceMongo(operation))    
          }
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
            if (true) {
              aggPipeline = [{ 
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
                aggPipeline[0]["$group"][col_type] = {
                  "$addToSet": {
                     "$type" : `$${col_name}`
                  }
                }
                aggPipeline[0]["$group"][col_size] = {
                  "$max": {
                     "$switch": {
                        branches: [
                            { case: { $eq: [{"$type" : `$${col_name}`} , 'string' ] }, then: {"$strLenBytes": { $ifNull: [`$${col_name}`,""]}}}
                        ],
                        default : 0
                     }
                  }
                }
                aggPipeline[1]["$project"][col_name] = {
                    type : `$${col_type}`
                   ,size : `$${col_size}`
                }           
              }
              operation = `db(${this.connection.databaseName}).collection(${collection.collectionName}.aggregate(${JSON.stringify(aggPipeline," ",2)})`
              if (this.status.sqlTrace) {
                this.status.sqlTrace.write(this.traceMongo(operation))    
              }
              let sqlStartTime = performance.now();
    	      stack =  new Error().stack
   	          const typeInformation = await collection.aggregate(aggPipeline).toArray();
			  if (typeInformation.length > 0) {
				tableInfo.COLUMN_NAME_ARRAY.forEach((col,idx) => {
				  let dataType = typeInformation[0][col].type.length == 1 ? typeInformation[0][col].type[0] : this.normalizeTypeInfo(typeInformation[0][col].type)
                  let size = typeInformation[0][col].size
                  size = dataType ===  'null' ? MongoConstants.DEFAULT_STRING_LENGTH : size                  
				  dataType = dataType ===  'null' ? 'string' : dataType                  
				  tableInfo.DATA_TYPE_ARRAY.push(dataType);
				  tableInfo.SIZE_CONSTRAINT_ARRAY.push (dataType === 'string' ? this.roundP2(size) : '')
			    })
              }
              else {
                tableInfo.SIZE_CONSTRAINT_ARRAY = new Array(tableInfo.COLUMN_NAME_ARRAY.length).fill('');
              }
            }
          }
        } catch(e) {		
          throw this.captureException(new MongoError(e,stack,operation))
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
	tableInfo.SQL_STATEMENT = `db.collection(${tableMetadata.TABLE_NAME}.find().stream()`; 
    return tableInfo
  }   
  
  streamingError(cause,sqlStatement) {
    return this.captureException(new MongoError(cause,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(collectionInfo,parser) {
            
    const collectionName = collectionInfo.TABLE_NAME
    const readStream = new Readable({objectMode: true });
    readStream._read = () => {};

    let stack
    const operation = `collection(${collectionName}).find().stream()`
    try {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceMongo(operation))
      }
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
      throw this.captureException(new MongoError(e,stack,operation))
    }
  }      
   
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
    
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL) 
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
  
}

module.exports = MongoDBI
