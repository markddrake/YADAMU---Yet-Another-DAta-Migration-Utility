
"use strict" 
const fs = require('fs');
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
const {ConnectionError, MongodbError} = require('../../common/yadamuError.js')
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const DBParser = require('./dbParser.js');

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
	
  traceMongo(apiCall) {
	return `MongoClient.${apiCall}\n`
  }
    
  getMongoURL() {
    
    return `mongodb://${this.connectionProperties.host}:${this.connectionProperties.port}`;
    
  }
  
  async getDatabaseConnectionImpl() {
    await this.createConnectionPool();
  }
  
  async createConnectionPool() {
      
	
    this.logConnectionProperties();
	const poolSize = this.parameters.PARALLEL ? parseInt(this.parameters.PARALLEL) + 1 : 5
    this.connectionProperties.options = typeof this.connectionProperties.options === 'object' ? this.connectionProperties.options : {}
    if (poolSize > 5) {
	  this.connectionProperties.options.poolSize = poolSize
	}
	if (this.status.sqlTrace) {
       this.status.sqlTrace.write(this.traceComment(`connect ${this.getMongoURL()})`))      
    }
	
	let stack
	try {
	  let sqlStartTime = performance.now();
	  stack = new Error().stack
      this.client = new MongoClient(this.getMongoURL(),this.connectionProperties.options);
      await this.client.connect();   
      this.traceTiming(sqlStartTime,performance.now())
	} catch (e) {
      throw new MongodbError(e,stack,'MongoClient()');
	}
  }
  
  releaseConnection() {
	// this.db.close() ?
  }
  
  isValidDDL() {
    return (this.systemInformation.vendor === this.DATABASE_VENDOR)
  }
  
  objectMode() {
    return true;
  }
    
  get DATABASE_VENDOR() { return 'MongoDB' };
  get SOFTWARE_VENDOR() { return 'MongoDB Corporation' };
  get SPATIAL_FORMAT()  { return 'GeoJSON' };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().mongodb }

  getCollection(tableName) {
    
    const self = this;  
    return new Promise(function(resolve, reject) {
	  const operation = `db.collection(${tableName}`
      if (self.status.sqlTrace) {
        self.status.sqlTrace.write(self.traceMongo(operation))      
      }      
      const stack = new Error().stack
      self.db.collection(tableName,{strict: true},function(err,collection) {
        if (err != null) {
          reject(new MondogdbError(err,stack,operation))
        }
        resolve(collection);
      })
    })
  }
                                                                    ;
  async executeDDLImpl(collectionList) {
     
    const results = await Promise.all(collectionList.map(function(collectionName) {
       let stack	
       let operation
       try {		
	     operation = `db.createCollection(${collectionName})`
         if (this.status.sqlTrace) {
           this.status.sqlTrace.write(this.traceMongo(opertaion))      
         }      
         let sqlStartTime = performance.now();
		 stack = new Error().stack
         const result = this.db.createCollection(collectionName);
         this.traceTiming(sqlStartTime,performance.now())
         return result
	   } catch (e) {
		 throw new MongodbError(e,stack,operation);
	   }
    },this));

  }    
    
  validateIdentifiers() {
        
    // ### Todo Add better algorthim than simple tuncation. Check for Duplicates and use counter when duplicates are detected.

    const tableMappings = {}
    let mapTables = false;
    const tables = Object.keys(this.metadata)    
    tables.forEach(function(table,idx){
      const tableName = this.metadata[table].tableName
      if (tableName.indexOf('$') > -1) {
        mapTables = true;
        const newTableName = tableName.replace(/\$/g,'')
        tableMappings[table] = {tableName : newTableName}
        this.metadata[table].tableName = newTableName;
      }
    },this)        
    
    if (mapTables) {
      this.tableMappings = tableMappings;
    }

  }    
  
  async useDatabase(database) {

	 let stack
	 let operation
	 try {	
	 operation = `client.db(${database})`
       if (this.status.sqlTrace) {
         this.status.sqlTrace.write(this.traceMongo(operation))   
	   }
       let sqlStartTime = performance.now();
	   stack = new Error().stack;
       this.db = await this.client.db(database,{returnNonCachedInstance:true});	 
	   operation = `db.stats()`
       if (this.status.sqlTrace) {
         this.status.sqlTrace.write(this.traceMongo(operation))   
	   }
       sqlStartTime = performance.now();
	   stack = new Error().stack;
       this.stats = await this.db.stats();	 
       this.traceTiming(sqlStartTime,performance.now())
	 } catch (e) {
	   throw new MongodbError(e,stack,operation)
	 }
  }

  getConnectionProperties() {
  
    return{
      host             : this.parameters.HOSTNAME
     ,port             : this.parameters.PORT
    }
  }
  
  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().mongodb)
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize() {   
	  // TODO : Support for Mongo Authentication ???
     await super.initialize(false);   
  }

  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async finalize() {
    if (this.client) {
      this.client.close();
    }
  }

  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    if (this.client) {
      try {
        this.client.close();
      } catch (e) {
        console.log(e)
	  }
    }      
  }

  /*
  **
  ** Begin the current transaction
  **
  */
  
  async initializeExport() {
	 super.initializeExport();
	 await this.useDatabase(this.parameters.FROM_USER);
  }
  
  async initializeImport() {
	 super.initializeImport();
	 await this.useDatabase(this.parameters.TO_USER);
  }
  
  async beginTransaction() {
  }


  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
  }
  
  /*
  **
  ** The following methods are used by JSON_TABLE() style import operations  
  **
  */

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
  
  async getSystemInformation(EXPORT_VERSION) {     
  
	
	let stack
	let operation
    try {
      operation = `db.admin().buildInfo()`
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(traceMongo(operation))      
      }
	  let sqlStartTime = performance.now();
	  stack = new Error().stack
      const buildInfo = await this.db.admin().buildInfo()

	  operation = 'db.admin().stats()';
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(traceMongo(operation))      
      }
	  stack = new Error().stack
	  const stats = await this.db.stats();
      this.traceTiming(sqlStartTime,performance.now())
      return {
        date               : new Date().toISOString()
       ,timeZoneOffset     : new Date().getTimezoneOffset()
       ,vendor             : this.DATABASE_VENDOR
       ,spatialFormat      : this.SPATIAL_FORMAT 
       ,schema             : this.parameters.FROM_USER
       ,softwareVendor     : this.SOFTWARE_VENDOR
       ,exportVersion      : EXPORT_VERSION
       ,nodeClient         : {
          version          : process.version
         ,architecture     : process.arch
         ,platform         : process.platform
        }
       ,buildInfo          : buildInfo
	   ,stats              : stats
      }
	} catch (e) {
      throw new MongodbError(e,stack,operation);
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
  
  async getSchemaInfo(schema) {
	  
	let stack
	let operation
	try {
	  operation = `db.listCollections().toArray()`;
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceComment(operation))      
      }
	  let sqlStartTime = performance.now();
	  operation = `MongoClient.db.listCollections().toArray()`;
	  stack = new Error().stack	
	  const schemaInfo = await this.db.listCollections().toArray();  
      this.traceTiming(sqlStartTime,performance.now())
      if ((this.parameters.MONGO_STORAGE_FORMAT === 'DOCUMENT') && (this.parameters.MONGO_EXPORT_FORMAT === 'ARRAY')) {
        const promises =  schemaInfo.map(function(collection) {     
		  operation = `MongoClient.db.collection(${collection.name}.mapReduce()`;
          if (this.status.sqlTrace) {
            this.status.sqlTrace.write(traceMongo(operation))      
          }
          let sqlStartTime = performance.now();
		  stack = new Error().stack
          const results = this.db.collection(collection.name).mapReduce(
            function() {
              if (keys.length === 0) {
                emit('metadata',null)
                emit('metadata',null)
              }
              newKeys = Object.keys(this)
              keys = Array.from(new Set([].concat(...[keys,newKeys])))
              dataTypeLength = ''
              newKeys.forEach(function (key,idx) {
                const dataType = this[key] === null ? null : typeof this[key]
                if (dataType === 'string') {
                  dataTypeLength = this[key].length
                }
                if (!types[key]) {
                  types[key] = [dataType]
                  sizes[key] = '' + dataTypeLength
                }
                else {
                  if ((dataType !== null) && (types[key].indexOf(dataType) === -1)) {
                    types[key].push(dataType);
                  }
                  sizes[key] = dataTypeLength > sizes[key] ? '' + dataTypeLength : sizes[key]
                }
              },this)
            },
            function(key,values) {
              const dataTypes = keys.map(function(key) {
                const keyTypes = types[key]
                if (keyTypes[0] === null) {
                  if (keyTypes.length === 1) {
                    return 'string'
                  }
                  else {
                    keyTypes.shift()
                  }
                }    
                if (keyTypes.length > 1) {
                  return  'JSON'
                }
                else {
                 return keyTypes[0]
                }
              },this)
              const sizeConstraints = keys.map(function(key) {
                return sizes[key]
              },this)
             
              return {key : keys, types : dataTypes, sizes: sizeConstraints}
            },
            {
              out       : { inline: 1}
             ,scope     : { 
                keys    : []
               ,types   : {}
               ,sizes   : []
            }
          })
          this.traceTiming(sqlStartTime,performance.now())
		  return results;
        },this)
      
        const results = await Promise.all(promises);
        schemaInfo.forEach(function(collection,idx) {
          schemaInfo[idx].TABLE_NAME =  collection.name
            if (results[idx].length === 0) {
              // Empty Collection
              schemaInfo[idx].columns =  ["JSON_DATA"]
              schemaInfo[idx].jsonDataTypes = ["JSON"];
              schemaInfo[idx].sizeConstraints = [""] 
            }
            else {
              const metadata = results[idx][0].value
              schemaInfo[idx].columns =  metadata.key
              schemaInfo[idx].jsonDataTypes = metadata.types
              schemaInfo[idx].sizeConstraints = metadata.sizes
            }
          },this)
        }
        else {
          schemaInfo.forEach(function(collection,idx) {
          schemaInfo[idx].TABLE_NAME =  collection.name
          schemaInfo[idx].columns =  ["JSON_DATA"];
          schemaInfo[idx].jsonDataTypes =  ["JSON"];
          schemaInfo[idx].sizeConstraints = [""] ;
        },this)
      } 
      return schemaInfo
	} catch(e) {
	  throw new MongodbError(e,stack,operation);
	}
  }

  generateMetadata(collections,server) {    
  
    const metadata = {}
    collections.forEach(function(collection) {
      if (this.parameters.MONGO_STRIP_ID === true) {
        const idx = collection.columns.indexOf('_id');
        if (idx > -1) {
          collection.columns.splice(idx,1);
          collection.jsonDataTypes.splice(idx,1);
          collection.sizeConstraints.splice(idx,1);
        }
      }
      const tableMetadata =  {
        tableName       : collection.TABLE_NAME
       ,columns         : '"' + collection.columns.join('","') + '"'
       ,dataTypes       : collection.jsonDataTypes
       ,sizeConstraints : collection.sizeConstraints
      } 
      metadata[collection.name] = tableMetadata
    },this) 
    return metadata
    
  }

  createParser(tableInfo,objectMode) {
	switch (true) {
	  case ((this.parameters.MONGO_STORAGE_FORMAT === 'DOCUMENT') && (this.parameters.MONGO_EXPORT_FORMAT === 'ARRAY')) :
	    tableInfo.transformation = 'DOCUMENT_TO_ARRAY'
		break;
	  case ((this.parameters.MONGO_STORAGE_FORMAT === 'ARRAY') && (this.parameters.MONGO_EXPORT_FORMAT === 'DOCUMENT')) :
	    tableInfo.transformation = 'ARRAY_TO_DOCUMENT'
		break;
      default:
	    tableInfo.transformation = 'NONE'
    } 
	tableInfo.stripID = this.parameters.MONGO_STRIP_ID ? this.parameters.MONGO_STRIP_ID : false
    return new DBParser(tableInfo,objectMode,this.yadamuLogger);
  }  
  
  async getInputStream(collection,parser) {
     
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};

    let stack
	let operation
	try {
	  operation = `db.collection(${collection.name}.find().stream()`
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(operation)      
      }
   	  let sqlStartTime = performance.now();
	  stack = new Error().stack;
      const mongoStream = await this.db.collection(collection.name).find().stream();
      this.traceTiming(sqlStartTime,performance.now())
      mongoStream.on('data', function(data) {readStream.push(data)})
      mongoStream.on('end',function(result) {readStream.push(null)});
      mongoStream.on('error',function(e) {readStream.emit('error',e)});
      return readStream;      
    } catch (e) {
      throw new MongodbError(e,stack,operation)
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
  
  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)
  }

  async insertMany(collection,array) {
	  
	let stack
	const operation = `db.collection(${collection}).insertMany(${array.length})`;
	try {
      if (this.status.sqlTrace) {
  	    this.status.sqlTrace.write(traceMongo(operation))      
      }
  	  let sqlStartTime = performance.now();
	  stack = new Error().stack;
      const results = await this.db.collection(collection).insertMany(array,{w:1});
      this.traceTiming(sqlStartTime,performance.now())
      return results;
	} catch (e) {
      throw new MongodbError(e,stack,operation);
	}
  }

  async insertOne(collection,doc) {
	let stack
	const operation = `db.collection(${collection}).insertOne()`;
	try {
      if (this.status.sqlTrace) {
  	    this.status.sqlTrace.write(traceMongo(operation))      
      }
  	  let sqlStartTime = performance.now();
	  stack = new Error().stack;
      const results = await this.db.collection(collection).insertOne(doc),{w:1};
      this.traceTiming(sqlStartTime,performance.now())
      return results;
	} catch (e) {
      throw new MongodbError(e,stack,operation);
	}
  }

  async configureSlave(slaveNumber,client) {
	this.slaveNumber = slaveNumber
	this.client = client
  }

  async newSlaveInterface(slaveNumber) {
	const dbi = new MongoDBI(this.yadamu)
	dbi.setParameters(this.parameters);
	// return await super.newSlaveInterface(slaveNumber,dbi,this.pool)
	await dbi.configureSlave(slaveNumber,this.client);
	this.cloneSlaveConfiguration(dbi);
	dbi.useDatabase(this.stats.db);
	return dbi
  }

  tableWriterFactory(tableName) {
    this.skipCount = 0;    
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger)
  }
  
}

module.exports = MongoDBI
