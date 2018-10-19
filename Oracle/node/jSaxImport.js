const { Transform } = require('stream');
const { Writable } = require('stream');
const Readable = require('stream').Readable;
const common = require('./common.js');
const clarinet = require('c:/Development/github/clarinet/clarinet.js');
// const clarinet = require('clarinet');
const fs = require('fs');
const oracledb = require('oracledb');

const LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const LOB_STRING_MAX_LENGTH    = 64 * 1024;
const BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const STRING_MAX_LENGTH        =  4 * 1024;

const DATA_TYPE_STRING_LENGTH = {
  BLOB          : LOB_STRING_MAX_LENGTH
, CLOB          : LOB_STRING_MAX_LENGTH
, NCLOB         : LOB_STRING_MAX_LENGTH
, OBJECT        : LOB_STRING_MAX_LENGTH
, XMLTYPE       : LOB_STRING_MAX_LENGTH
, ANYDATA       : LOB_STRING_MAX_LENGTH
, BFILE         : BFILE_STRING_MAX_LENGTH
, DATE          : 24
, TIMESTAMP     : 30
, INTERVAL      : 16
}  
  


class RowParser extends Transform {
  
  constructor(options) {
    super({objectMode: true });  
    const rowParser = this;
    let   dataPhase = false;
    this.saxJParser = clarinet.createStream();
     
    this.objectStack = [];
    this.keyStack = [];
    this.jDepth = 0;

    this.currentObject;
    this.chunks = [];
    
    this.saxJParser.onvaluechunk = function (v) {
      
      rowParser.chunks.push(v);  
    
    };
       
    this.saxJParser.onvalue = function (v) {
      
      if (rowParser.chunks.length > 0) {
        rowParser.chunks.push(v);
        v = rowParser.chunks.join('');
        // console.log(`onvalue(${rowParser.chunks.length},${v.length})`);
        rowParser.chunks = []
      }
      
      if (typeof v === 'boolean') {
        v = new Boolean(v).toString();
      }
      
      if (Array.isArray(rowParser.currentObject)) {
        rowParser.currentObject.push(v);
      }
      else {
        const currentKey = rowParser.keyStack.pop();
        rowParser.currentObject[currentKey] = v;
      }
    };
       
    this.saxJParser.onopenobject = function (key) {
      rowParser.jDepth++;
      // console.log(`onOpenObject(${rowParser.jDepth},${key}, Keys:${rowParser.keyStack}`);
      rowParser.keyStack.push(key);
      switch (rowParser.jDepth) {
        case 1:
          break;
        case 2:
          if ((rowParser.dataPhase) && (key !== undefined)) {
            rowParser.push({ table : key});
          }
          // Fall through to default
        default:
          rowParser.objectStack.push(rowParser.currentObject);
      }
      rowParser.currentObject = {}
    };
    
    this.saxJParser.onkey = function (key) {
      // console.log(`onKey(${rowParser.jDepth},${key})`);
      switch (rowParser.jDepth){
        case 1:
          if (key === 'data') {
            rowParser.dataPhase = true;
          }
          break;
        case 2:
          if (rowParser.dataPhase) {
            rowParser.push({ table : key});
          }
          break;
        default:
      }
      rowParser.keyStack[rowParser.jDepth-1] = key;
    };

    this.saxJParser.oncloseobject = async function () {
      // console.log(`onCloseObject(${rowParser.jDepth})`);
      
      switch (rowParser.jDepth) {
        case 0:
          break;
        case 1:
          break;
        case 2:        
          const key = rowParser.keyStack.pop();
          switch (key) {
            case 'systemInformation' :
              rowParser.push({ systemInformation : rowParser.currentObject})
              break;
            case 'metadata' :
              rowParser.push({ metadata : rowParser.currentObject});
              break;
           default:
             rowParser.objectStack.length = 0;
             rowParser.keyStack.length = 0;   
          }
          break;
        case 5:
          if (rowParser.dataPhase) {
            rowParser.currentObject = JSON.stringify(rowParser.currentObject);
          }
        default:
          const parentObject = rowParser.objectStack.pop()     
          if (Array.isArray(parentObject)) {   
            // console.log(`oncloseobject(${rowParser.jDepth},Array): push(${JSON.stringify(rowParser.currentObject)})`);
            parentObject.push(rowParser.currentObject);
          }    
          else {
            // console.log(`oncloseobject(Object): KeyStack:${JSON.stringify(rowParser.keyStack)}`);            
            const parentKey = rowParser.keyStack.pop();
            // console.log(`oncloseobject(Object): Adding {${parentKey}:${JSON.stringify(rowParser.currentObject)}`);
            parentObject[parentKey] = rowParser.currentObject;
         }
         rowParser.currentObject = parentObject;
      }   
      rowParser.jDepth--;
    }
   
    this.saxJParser.onopenarray = function () {
      rowParser.jDepth++;
      // console.log(`onOpenArray(${rowParser.jDepth})`);
      rowParser.objectStack.push(rowParser.currentObject);
      rowParser.currentObject = [];
    };

    this.saxJParser.onclosearray = function () {
      // console.log(`onCloseArray(${rowParser.jDepth})`);
      rowParser.keyStack.pop(); 
      switch (rowParser.jDepth) {
        case 0:
          break;
        case 1:
          break;
        case 2:
          break;
        case 3:
          break;
        case 4:
          rowParser.push({ data : rowParser.currentObject});
          break;
        default:
          const parentObject = rowParser.objectStack.pop()     
          if (Array.isArray(parentObject)) {    
            // console.log(`onclosearray(Array): push(${JSON.stringify(rowParser.currentObject)})`);
            parentObject.push(rowParser.currentObject);
          }     
          else {
            // console.log(`onclosearray(Object): KeyStack:${JSON.stringify(rowParser.keyStack)}`);            
            const parentKey = rowParser.keyStack.pop();
            // console.log(`onclosearray(Object): Adding {${parentKey}:${JSON.stringify(rowParser.currentObject)}`);
            parentObject[parentKey] = rowParser.currentObject;
          }
          rowParser.currentObject = parentObject;
      }   
      rowParser.jDepth--;
    };  
  }
   
  _transform(data,enc,callback) {
    this.saxJParser.write(data);
    callback();
  };
}

async function createTables(conn, schema, metadata) {
    
  try {
  
    const insertDetails = {}
    const ddlStatements = [];
    
    const sqlStatement = `begin :sql := JSON_IMPORT.GENERATE_STATEMENTS(:metadata, :schema);\nEND;`;
    const metadataLob = await common.lobFromJSON(conn,{metadata: metadata});  
    const results = await conn.execute(sqlStatement,{sql:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , metadata:metadataLob, schema:schema});
    const sql = JSON.parse(results.outBinds.sql);
    
    const tables = Object.keys(metadata); 
    tables.forEach(function(table,idx) {
                     ddlStatements[idx] = sql[idx].ddl;                   
                     const tableInfo = { 
                             dmlStatement : sql[idx].dml
                            ,binds        : []
                            ,lobColumns   : []
                           }
                     insertDetails[table] = tableInfo;
                     
                     const tableMetadata = metadata[table];   
                     const dataTypeArray = JSON.parse('[' +  sql[idx].targetDataTypes.replace(/(\"\.\")/g, '\\".\\"') + ']')
                     const dataLengthArray = JSON.parse('[' + tableMetadata.dataTypeSizing + ']')
                     /*
                     console.log(tableMetadata.tableName)
                     console.log(dataTypeArray)
                     console.log(dataLengthArray);
                     */
                     tableInfo.binds = dataTypeArray.map(function (dataType,idx) {
                       switch (dataType) {
                         case 'CLOB':
                           // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                           tableInfo.lobColumns.push(idx);
                           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                         case 'NCLOB':
                           // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                           tableInfo.lobColumns.push(idx);
                           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                         case 'BLOB':
                           // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                           tableInfo.lobColumns.push(idx);
                           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                         case 'XMLTYPE':
                           // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                           tableInfo.lobColumns.push(idx);
                           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                         case 'ANYDATA':
                           // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                           tableInfo.lobColumns.push(idx);
                           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                         case 'NUMBER':
                           return { type: oracledb.NUMBER }
                         case 'FLOAT':
                           return { type: oracledb.NUMBER }
                         case 'CHAR':
                           return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx])}
                         case 'NCHAR':
                           return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx]*2)}
                         case 'NVARCHAR2':
                           return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx]*2)}
                         case 'VARCHAR':
                           return { type :oracledb.STRING, maxSize : parseInt(ataLengthArray[idx])}
                         case 'VARCHAR2':
                           return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx])}
                         case 'DATE':
                           return { type :oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType]}
                         case 'RAW':
                           return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx])*2}
                         case 'RAW(1)':
                           return { type :oracledb.STRING, maxSize : 5}
                         case 'BFILE':
                           return { type :oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
                         default:
                           if (dataType.startsWith('NUMBER')) {
                             return { type: oracledb.NUMBER }
                           }
                           if (dataType.startsWith('RAW(')) {
                             return { type: oracledb.STRING, maxSize : parseInt(dataType.match(/\((\d+)\)/)[0].slice(1,-1))*2}
                           }
                           if ((dataType.startsWith('CHAR(')) || (dataType.startsWith('VARCHAR2(')) || (dataType.startsWith('NCHAR(')) || (dataType.startsWith('NVARCHAR2('))){
                             return { type: oracledb.STRING, maxSize : parseInt(dataType.match(/\((\d+)\)/)[0].slice(1,-1))}
                           }
                           if (dataType.startsWith('FLOAT')) {
                             return { type: oracledb.NUMBER }
                           }
                           if (dataType.startsWith('TIMESTAMP')) {
                             return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH['TIMESTAMP']  }
                           }
                           if (dataType.startsWith('INTERVAL')) {
                             return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH['TIMESTAMP']  }
                           }
                           if (dataType.indexOf('.') > -1) {
                             // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH['OBJECT']  }
                             tableInfo.lobColumns.push(idx);
                             return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH['OBJECT'] }
                           }
                           return {type : dataType};
                       }
                     })
    });
    
    for (let i=0; i<ddlStatements.length;i++) {
      try {
        const results = await conn.execute(ddlStatements[i]);   
      } catch (e) {
        console.log(e);
      }
    }
    return insertDetails
  } catch (e) {
    console.log(e);
  }
}

  
function stringToLob(conn,str) {
   return new Promise(async function(resolve,reject) {
                              if (str === null) {
                                 resolve(str)
                              }
                              else {
                                const s = new Readable();
                                s.push(str);
                                s.push(null);
                                s.on('error', function(err) {reject(err);});                               
                                const tempLob = await conn.createLob(oracledb.CLOB);
                                tempLob.on('error',function(err) {reject(err);});
                                tempLob.on('finish', function() {resolve(tempLob)});
                                s.pipe(tempLob)
                              }
   })
}
               
async function convertLobs(conn, data, lobColumns) {
    
  try {
    const lobList = await Promise.all(lobColumns.map(async function (lobIndex) {
                                                 data[lobIndex] = await stringToLob(conn,data[lobIndex]);
                                                 return data[lobIndex]
    }))
    return lobList
  } catch (e) {
    console.log(e);
    throw (e);
  }
}
  
class DbWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,lobCacheSize,logWriter,options) {
    super({objectMode: true });
    
    this.logWriter = logWriter;
    const dbWriter = this;
    
    this.systemInformation;
    this.metadata;

    this.insertCache
    
    this.tableName;
    this.insertStatement;
    this.lobColumns;
    this.binds;
    this.rowCount;
    this.startTime;
    
    this.schema = schema;
    this.conn = conn;

    this.batch = [];
    this.lobCache = [];
    
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.lobCacheSize = lobCacheSize;
  }      
  
  clearLobCache(lobCache) {
    lobCache.forEach(function(lob) {
                       try {
                         if (lob !== null) {
                           lob.close();     
                         }
                         } catch (e) {
                           console.log(`Error Closing Lob: ${e}`);
                         }
                           
    })
    lobCache.length = 0;
  }
 
  switchTable(tableName) {
    this.tableName = tableName
    this.insertStatement =  this.insertCache[tableName].dmlStatement;
    this.binds = this.insertCache[tableName].binds;
    this.lobColumns = this.insertCache [tableName].lobColumns
    this.rowCount = 0;
    this.batch.length = 0;
    this.startTime = new Date().getTime();
    this.clearLobCache(this.lobCache);
  }
  
  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          this.insertCache = await createTables(this.conn, this.schema, this.metadata);
          break;
        case 'table':
          // console.log(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          const elapsedTime = new Date().getTime() - this.startTime;
          if (this.tableName !== undefined) {
            if (this.batch.length > 0) {
              // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
              try {
                const results = await this.conn.executeMany(this.insertStatement,this.batch,{bindDefs : this.binds});
              } catch (e) {
                console.log(`_write(${this.tableName}) : executeMany() failed. ${e}. Retrying using execute() loop.`);
                console.log(this.binds);
                for (let row in this.batch) {
                  let results = await this.conn.execute(this.insertStatement,this.batch[row],{bindDefs : this.binds})
                }
              }
              this.clearLobCache(this.lobCache);
              await this.conn.commit();
            }  
            this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          }
          this.switchTable(obj.table);
          break;
        case 'data': 
          if (this.lobColumns.length > 0) {
            const newLobList = await convertLobs(this.conn, obj.data, this.lobColumns)
            this.lobCache.push(...newLobList);
          }
          this.batch.push(obj.data);
          // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batch.length} rows.`);
          if ((this.batch.length === this.batchSize) || (this.lobCache.length >= this.lobCacheSize)) {
              // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batch.length} rows.`);
              try {
                const results = await this.conn.executeMany(this.insertStatement,this.batch,{bindDefs : this.binds})
              } catch (e) {
                console.log(`${new Date().toISOString()}:_write(${this.tableName},${this.batch.length}) : executeMany() failed. ${e}. Retrying using execute() loop.`);
                console.log(this.binds);
                for (let row in this.batch) {
                  let results = await this.conn.execute(this.insertStatement,this.batch[row],{bindDefs : this.binds});
                }
              }
              this.clearLobCache(this.lobCache);
              this.batch.length = 0;
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             await this.conn.commit();
             const elapsedTime = new Date().getTime() - this.startTime;
             // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}:_write(${this.tableName}): ${e}`)
      this.logWriter.write(this.insertStatement);
      console.log(this.binds);
      console.log(this.batch);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      const elapsedTime = new Date().getTime() - this.startTime;
      if (this.batch.length > 0) {
        // console.log(`${new Date().toISOString()}:_final() Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
        try {
          const results = await this.conn.executeMany(this.insertStatement,this.batch,{bindDefs : this.binds});
        } catch (e) {
          if (e.errorNum & (e.errorNum === 1461)) {
            console.log(`${new Date().toISOString()}:_final(${this.tableName}) : executeMany() failed. ${e}. Skipping table.`);
            callback()
            return;
          }
          this.logWriter.write(`${new Date().toISOString()}:_final(${this.tableName}) : executeMany() failed. ${e}. Retrying using execute() loop.`);
          for (let row in this.batch) {
            let results = await this.conn.execute(this.insertStatement,this.batch[row],{bindDefs : this.binds});
          }
        }
        this.clearLobCache(this.lobCache);
        await this.conn.commit();
      }   
      if (this.tableName) {
        console.log(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
      }
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }  
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}:_final(${this.tableName}): ${e}`)
      this.logWriter.write(this.insertStatement);
      this.logWriter.write(this.binds);
      this.logWriter.write(this.batch);
      callback(e);
    } 
  } 
}

function processFile(conn, schema, dumpFilePath,batchSize,commitSize,lobCacheSize,logWriter) {
  
  return new Promise(function (resolve,reject) {
    const dbWriter = new DbWriter(conn,schema,batchSize,commitSize,lobCacheSize,logWriter);
    const rowGenerator = new RowParser();
    const readStream = fs.createReadStream(dumpFilePath);    
    dbWriter.on('finish', function() { resolve()});
    readStream.pipe(rowGenerator).pipe(dbWriter);
  })
}
    
async function main() {

  let pool;	
  let conn;
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;
    
  let errorRaised = false;
  let warningRaised = false;
  let statusMsg = 'successfully';
  let results;
  
  try {

    parameters = common.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

	if (parameters.SQLTRACE) {
	  sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }
	
    
    conn = await common.doConnect(parameters.USERID);
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
	
    const logDML         = (parameters.LOGLEVEL && (parameters.loglevel > 0));
    const logDDL         = (parameters.LOGLEVEL && (parameters.loglevel > 1));
    const logDDLIssues   = (parameters.LOGLEVEL && (parameters.loglevel > 2));
    const logTrace       = (parameters.LOGLEVEL && (parameters.loglevel > 3));
    	
    await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.LOBCACHESIZE, logWriter);
    
    common.doRelease(conn);						   

    statusMsg = warningRaised ? 'with warnings' : statusMsg;
    statusMsg = errorRaised ? 'with errors'  : statusMsg;
     
    logWriter.write(`Import operation completed ${statusMsg}.`);
    if (logWriter !== process.stdout) {
      console.log(`Import operation completed ${statusMsg}: See "${parameters.LOGFILE}" for details.`);
    }
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Import operation failed.\n');
      logWriter.write(e.stack);
    }
    else {
        console.log('Import operation Failed.');
        console.log(e);
    }
    if (conn !== undefined) {
      common.doRelease(conn);
    }
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

  if (parameters.SQLTRACE) {
    sqlTrace.close();
  }
}
    
main()


 