
"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const MongoClient = require('mongodb').MongoClient

const Yadamu = require('../../common/yadamu.js');
const YadamuDBI =  require('../../common/yadamuDBI.js');
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
      self.db.collection(tableName,{strict: true},function(err,collection) {
        if (err != null) {
          reject(err)
        }
        resolve(collection);
      })
    })
  }
                                                                    ;
  async executeDDL(collectionList) {
           
    const results = await Promise.all(collectionList.map(function(collectionName) {
      return this.db.createCollection(collectionName);
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

  getMongoURL() {
    
    return `mongodb://${this.connectionProperties.host}:${this.connectionProperties.port}`;
    
  }
  
  getConnectionProperties() {
  
    return{
      host             : this.parameters.HOSTNAME
     ,database         : this.parameters.DATABASE
     ,port             : this.parameters.PORT
    }
  }
  
  constructor(yadamu) {
    super(yadamu,yadamu.getYadamuDefaults().mongoDB)
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize() {
      
     this.client = new MongoClient(this.getMongoURL());
     await this.client.connect();
     this.db = this.client.db(this.parameters.DATABASE);
     
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
       this.client.close();
     }      
  }

  /*
  **
  ** Begin the current transaction
  **
  */
  
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
    const buildInfo = await this.db.admin().buildInfo()
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
    const schemaInfo = await this.db.listCollections().toArray();  
    if (this.parameters.MONGO_MODE === 'ARRAY') {
      const promises =  schemaInfo.map(function(collection) {     
        return this.db.collection(collection.name).mapReduce(
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
      },this)
      
      
      const results = await Promise.all(promises);
      schemaInfo.forEach(function(collection,idx) {
        schemaInfo[idx].TABLE_NAME =  collection.name
        if (results[idx].length === 0) {
          // Empty Collection
          schemaInfo[idx].columns =  ["JSON_DATA"]
          schemaInfo[idx].dataTypes = ["JSON"];
          schemaInfo[idx].sizeConstraints = [""] 
        }
        else {
          const metadata = results[idx][0].value
          schemaInfo[idx].columns =  metadata.key
          schemaInfo[idx].dataTypes = metadata.types
          schemaInfo[idx].sizeConstraints = metadata.sizes
        }
      },this)
    }
    else {
      schemaInfo.forEach(function(collection,idx) {
        schemaInfo[idx].TABLE_NAME =  collection.name
        schemaInfo[idx].columns =  ["JSON_DATA"];
        schemaInfo[idx].dataTypes =  ["JSON"];
        schemaInfo[idx].sizeConstraints = [""] ;
      },this)
    }     
    return schemaInfo
  }

  generateMetadata(collections,server) {    
  
    const metadata = {}
    collections.forEach(function(collection) {
      if (this.parameters.MONGO_REMOVE_ID === true) {
        const idx = collection.columns.indexOf('_id');
        if (idx > -1) {
          collection.columns.splice(idx,1);
          collection.dataTypes.splice(idx,1);
          collection.sizeConstraints.splice(idx,1);
        }
      }
      const tableMetadata =  {
        tableName       : collection.TABLE_NAME
       ,columns         : '"' + collection.columns.join('","') + '"'
       ,dataTypes       : collection.dataTypes
       ,sizeConstraints : collection.sizeConstraints
      } 
      metadata[collection.name] = tableMetadata
    },this) 
    return metadata
  }

  createParser(query,objectMode) {
    query.outputMode = this.parameters.MONGO_MODE ? this.parameters.MONGO_MODE : 'DOCUMENT'
    query.removeID = this.parameters.MONGO_REMOVE_ID ? this.parameters.MONGO_REMOVE_ID : false
    return new DBParser(query,objectMode,this.yadamuLogger);
  }  
  
  async getInputStream(collection,parser) {
     
    const readStream = new Readable({objectMode: true });
    readStream._read = function() {};
   
    const mongoStream = this.db.collection(collection.name).find().stream();
    mongoStream.on('data', function(data) {readStream.push(data)})
    mongoStream.on('end',function(result) {readStream.push(null)});
    return readStream;      
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
    const results = await this.db.collection(collection).insertMany(array);
    return results;
  }

  async insertOne(collection,doc) {
    const results = await this.db.collection(collection).insertOne(doc);
    return results;
  }

}

module.exports = MongoDBI
