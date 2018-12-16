"use strict";
const fs = require('fs');
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;
const Readable = require('stream').Readable;
const Writable = require('stream').Writable
const path = require('path');

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamuCore.js');
const OracleCore = require('./oracleCore.js');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'Oracle';

const sqlGetSystemInformation = 
`select JSON_EXPORT.JSON_FEATURES() JSON_FEATURES, 
        JSON_EXPORT.DATABASE_RELEASE() DATABASE_RELEASE, 
        SYS_CONTEXT('USERENV','SESSION_USER') SESSION_USER, 
        SYS_CONTEXT('USERENV','DB_NAME') DATABASE_NAME, 
        SYS_CONTEXT('USERENV','SERVER_HOST') SERVER_HOST,
        SESSIONTIMEZONE SESSION_TIME_ZONE,
        JSON_OBJECTAGG(parameter, value) NLS_PARAMETERS
        from NLS_DATABASE_PARAMETERS`;

const sqlFetchDDL = 
`select COLUMN_VALUE JSON 
   from TABLE(JSON_EXPORT_DDL.FETCH_DDL_STATEMENTS(:schema))`;;

const sqlGenerateQueries = 
`select * 
   from table(JSON_EXPORT.GET_DML_STATEMENTS(:schema))`;


class StringWriter extends Writable {
   
  constructor(options) {
    super(options)
    this.chunks = []
  }

  _write(chunk, encoding, done) {
     this.chunks.push(chunk);
     done();
  }
  
  toString() {
    return this.chunks.join('');
  }
  
}

class BufferWriter extends Writable {
   
  constructor(options) {
    super(options)
    this.chunks = []
  }

  _write(chunk, encoding, done) {
     this.chunks.push(chunk);
     done();
  }
  
  toHexBinary() {
    return Buffer.concat(this.chunks).toString('hex');
  }
  
}

async function getSystemInformation(conn,status) {     
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }

    const results = await conn.execute(sqlGetSystemInformation,[],{outFormat: oracledb.OBJECT ,});
    return results.rows[0];
}

async function generateQueries(conn,status,schema) {       
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }

    const results = await conn.execute(sqlGenerateQueries,{schema: schema},{outFormat: oracledb.OBJECT , fetchInfo:{
                                                                                                           COLUMN_LIST:          {type: oracledb.STRING}
                                                                                                          ,DATA_TYPE_LIST:       {type: oracledb.STRING}
                                                                                                          ,SIZE_CONSTRAINTS:     {type: oracledb.STRING}
                                                                                                          ,EXPORT_SELECT_LIST:   {type: oracledb.STRING}
                                                                                                          ,NODE_SELECT_LIST:     {type: oracledb.STRING}
                                                                                                          ,WITH_CLAUSE:          {type: oracledb.STRING}
                                                                                                          ,SQL_STATEMENT:        {type: oracledb.STRING}
                                                                                                         }
    });
    return results.rows;
}

function fetchDDL(conn,status,schema,outStream) {
   

  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
    this.push(data.JSON);
    done();
  }
  
  return new Promise(async function(resolve,reject) {  
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlFetchDDL}\n\/\n`)
    }
    const stream = await conn.queryStream(sqlFetchDDL,{schema: schema},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
    stream.on('end',function() {resolve()})
    stream.on('error',function(err){reject(err)});
    stream.pipe(parser).pipe(JSONStream.stringify('[',',',']')).pipe(outStream,{end: false })
  })
}



async function jsonFetchData(conn,status,sqlStatement,tableName,outStream,logWriter) {

  let counter = 0;
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
    counter++;
    let parsingCompleted = false;
    while(!parsingCompleted) {
      try {
        this.push(JSON.parse(data.JSON));
        parsingCompleted = true;
        done();
      } catch(e) {
        const tokens = e.message.split(' ');
        if ((tokens[0] === 'Unexpected') && (tokens[1] === 'token')) {
           const badToken = tokens[2]
           const offset = tokens.pop();
           if ((badToken === '.') && (data.JSON[offset-1] === ',')) {
             // Oracle 12c may render non-integer values < 1 without a leading zero which is invalid JSON...
             data.JSON = data.JSON.slice(0,offset) + '0' + data.JSON.slice(offset)
           }
           else {
             logWriter.write(`${new Date().toISOString()}["${tableName}"][${counter}]: ${e}\n`);
             logWriter.write(`${data.JSON}\n`);
             parsingCompleted = true;
             done();
           }
        }
        else {
          logWriter.write(`${new Date().toISOString()}["${tableName}"][${counter}]: ${e}\n`);
          logWriter.write(`${data.JSON}\n`);
          parsingCompleted = true;
          done();
        }
      }
    }
  }
    
  if (status.sqlTrace) {
    status.sqlTrace.write(`${sqlStatement}\n\/\n`)
  }

  const stream = await conn.queryStream(sqlStatement,[],{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(function(resolve,reject) {  
    jsonStream.on('end',function() {resolve(counter)})
    stream.on('error',function(err){reject(err)});
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
  })
}

async function wideFetchData(conn,status,query,tableName,columnList,outStream,logWriter) {

  let counter = 0;
  const columns = JSON.parse('[' + columnList + ']');
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
    counter++;
    const rowArray = []
    // ### Need to fix  12c invalid JSON issue.
    columns.forEach(function(column) {
      rowArray.push(JSON.parse(data[column])[0]);
    })
    this.push(rowArray);
    done();
  }

  if (status.sqlTrace) {
    status.sqlTrace.write(`${query.sql}\n\/\n`)
  }

  const stream = await conn.queryStream(query.sql,[],{outFormat: oracledb.OBJECT,fetchInfo:query.fetchInfo})
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(function(resolve,reject) {  
    jsonStream.on('end',function() {resolve(counter)})
    stream.on('error',function(err){reject(err)});
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
  })
    
}

function blob2HexBinary(blob) {
 
  return new Promise(async function(resolve,reject) {
    try {
      const bufferWriter = new  BufferWriter();
      
      blob.on('error',
      async function(err) {
         await blob.close();
         reject(err);
      });
      
      bufferWriter.on('finish', 
      async function() {
        await blob.close(); 
        resolve(bufferWriter.toHexBinary());
      });
     
      blob.pipe(bufferWriter);
    } catch (err) {
      reject(err);
    }
  });
};

function clob2String(clob) {
 
  return new Promise(async function(resolve,reject) {
    try {
      const stringWriter = new  StringWriter();
      clob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
      
      clob.on('error',
      async function(err) {
         await clob.close();
         reject(err);
      });
      
      stringWriter.on('finish', 
      async function() {
        await clob.close(); 
        resolve(stringWriter.toString());
      });
     
      clob.pipe(stringWriter);
    } catch (err) {
      reject(err);
    }
  });
};

async function processClientData(conn,status,query,tableName,columnList,outStream,logWriter) {

  let columnMetadata;
  let includesLobs = false;
  let includesJSON = false;
  
  let counter = 0;
  const parser = new Transform({objectMode:true});
  parser._transform = async function(data,encodoing,done) {
    counter++;
    if (includesLobs) {
      data = await Promise.all(data.map(function (item,idx) {
               if ((item !== null) && (columnMetadata[idx].fetchType === oracledb.CLOB)) {
                 return clob2String(item)
               }
               if ((item !== null) && (columnMetadata[idx].fetchType === oracledb.BLOB)) {
                 return blob2HexBinary(item)
               }  
               return item
      }))
    }  
    // Convert the JSON columns into JSON objects
    query.jsonColumns.forEach(function(idx) {
       if (data[idx] !== null) {
         try {
           data[idx] = JSON.parse(data[idx]) 
         } catch (e) {
           logWriter.write(`${counter}:${e}\n`);
           logWriter.write(`${data[idx]}\n`);
         }
       }
    })
    query.rawColumns.forEach(function(idx) {
       if (data[idx] !== null) {
         if(Buffer.isBuffer(data[idx])) {
           data[idx] = data[idx].toString('hex');
         }
       }
    })
    this.push(data);
    done();
  }

  if (status.sqlTrace) {
    status.sqlTrace.write(`${query.sql}\n\/\n`)
  }

  const stream = await conn.queryStream(query.sql,[],{extendedMetaData: true})
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(function(resolve,reject) {  
    jsonStream.on('end',function() {resolve(counter)})
    stream.on('error',function(err){reject(err)});
    stream.on('metadata',
    function(metadata) {
       columnMetadata = metadata;
       columnMetadata.forEach(function (column) {
         if ((column.fetchType === oracledb.CLOB) || (column.fetchType === oracledb.BLOB)) {
            includesLobs = true;
         }
       }) 
    })
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
  })
    
}

function writeTableName(i,tableName,outStream) {

  const s = new Readable();
  if (i > 0) {
    s.push(',');
  }
  s.push(tableName);
  s.push(null);
    
  return new Promise(function(resolve,reject) {  
    s.on('end',function() {resolve()})
    s.pipe(outStream,{end: false })
  })
}

function resetStream(fileWriteStream,offset) {
    const path = fileWriteStream.path;
    fileWriteStream.close();
    return fs.createWriteStream(path,{start:offset,flags:"r+"});
}
    
function decomposeSelectList(exportSelectList) {

  let start = 0;
  let level = 0;
  const selectListMembers = [];

  // A Commma may occur inside function control in which case it's not a column seperator.

  for (let i=0; i < exportSelectList.length; i++) {
    if (exportSelectList[i] === '(') {
      level++;
      continue;
    }
    if (exportSelectList[i] === ')') {
      level--;
      continue;
    }
    if ((level === 0) && (exportSelectList[i] === ',')) {
      selectListMembers.push(exportSelectList.substring(start,i));
      start = i+1;
    }
  }  
  selectListMembers.push(exportSelectList.substring(start));

  return selectListMembers

}
 
function wideTableWorkaround(tableInfo,maxVarcharSize) {
   
  // rewrite from JSON_ARRAY(A,B,FOO(C)) into JSON_ARRAY(A), JSON_ARRAY(B), JSON_ARRAY(FOO(C))
   
  let selectList = '';
  const columnList = JSON.parse('[' + tableInfo.COLUMN_LIST + ']');
  const selectListMembers = decomposeSelectList(tableInfo.EXPORT_SELECT_LIST)

  const fetchInfo = {}
  selectList = columnList.map(function(column,index){
                                fetchInfo[column] = { type : oracledb.STRING }
                                return `JSON_ARRAY(${selectListMembers[index]} NULL on NULL RETURNING VARCHAR2(${maxVarcharSize})) "${column}"`;
  }).join(',');
   
  let sqlStatement = `select ${selectList} from "${tableInfo.OWNER}"."${tableInfo.TABLE_NAME}" t`;
  
  if (tableInfo.SQL_STATEMENT.indexOf('WITH') === 0) {
     const endOfWithClause = tableInfo.SQL_STATEMENT.indexOf('select JSON_ARRAY(');
     sqlStatement = tableInfo.SQL_STATEMENT.substring(0,endOfWithClause) + sqlStatement;
  }
  
  return {sql: sqlStatement, fetchInfo : fetchInfo}

}

function generateClientQuery(tableInfo) {
   
  // Perform a traditional relational select..
  
  const query = {
    fetchInfo   : {}
   ,jsonColumns : []
   ,rawColumns  : []
  }   
  
  let selectList = '';
  const columnList = JSON.parse('[' + tableInfo.COLUMN_LIST + ']');
  
  const dataTypeList = JSON.parse(tableInfo.DATA_TYPE_LIST);
  dataTypeList.forEach(function(dataType,idx) {
    switch (dataType) {
      case 'JSON':
        query.jsonColumns.push(idx);
        break
      case 'RAW': 
        query.rawColumns.push(idx);
        break;
      default:
    }
  })
  
  query.sql = `select ${tableInfo.NODE_SELECT_LIST} from "${tableInfo.OWNER}"."${tableInfo.TABLE_NAME}" t`; 
  
  if (tableInfo.WITH_CLAUSE !== null) {
     query.sql = `with\n${tableInfo.WITH_CLAUSE}\n${query.sql}`;
  }
  
  return query

}

async function main(){

  let conn;
  let parameters;
  let logWriter = process.stdout;
  let status;
  
  try {
    parameters = OracleCore.processArguments(process.argv);
    status = Yadamu.getStatus(parameters,'Export');
    
    if (parameters.LOGFILE) {
      logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }
    
    conn = await OracleCore.doConnect(parameters.USERID,status);
    
    const exportFilePath = path.resolve(parameters.FILE);
    let exportFile = fs.createWriteStream(exportFilePath);
    // exportFile.on('error',function(err) {console.log(err)})
    logWriter.write(`${new Date().toISOString()}[Export]: Generating file "${exportFilePath}".\n`)
    
    const sysInfo = await getSystemInformation(conn,status);
    exportFile.write('{"systemInformation":');
    exportFile.write(JSON.stringify({
                         "date"               : new Date().toISOString()
                        ,"timeZoneOffset"     : new Date().getTimezoneOffset()
                        ,"sessionTimeZone"    : sysInfo.SESSION_TIME_ZONE
                        ,"vendor"             : DATABASE_VENDOR
                        ,"spatialFormat"      : "WKT"
                        ,"schema"             : parameters.OWNER
                        ,"exportVersion"      : EXPORT_VERSION
                        ,"sessionUser"        : sysInfo.SESSION_USER
                        ,"dbName"             : sysInfo.DATABASE_NAME
                        ,"serverHostName"     : sysInfo.SERVER_HOST
                        ,"databaseVersion"    : sysInfo.DATABASE_RELEASE
                        ,"jsonFeatures"       : JSON.parse(sysInfo.JSON_FEATURES)
                        ,"nlsParameters"      : JSON.parse(sysInfo.NLS_PARAMETERS)
    }));

    const maxVarcharSize = JSON.parse(sysInfo.JSON_FEATURES).extendedString ? 32767 : 4000;
    const serverGeneration = (JSON.parse(sysInfo.JSON_FEATURES).clobSupported === true)
    // const serverGeneration = false;

    if (parameters.MODE !== 'DATA_ONLY') {
      exportFile.write(',"ddl":');
      await fetchDDL(conn,status,parameters.OWNER,exportFile);
    }
    
    if (parameters.MODE !== 'DDL_ONLY') {
      exportFile.write(',"metadata":{');
      const sqlQueries = await generateQueries(conn,status,parameters.OWNER);
      for (let i=0; i < sqlQueries.length; i++) {
        if (i > 0) {
          exportFile.write(',');
        }
        exportFile.write(`"${sqlQueries[i].TABLE_NAME}" : ${JSON.stringify({
                                                           "owner"                    : sqlQueries[i].OWNER
                                                          ,"tableName"                : sqlQueries[i].TABLE_NAME
                                                          ,"columns"                  : sqlQueries[i].COLUMN_LIST
                                                          ,"dataTypes"                : JSON.parse(sqlQueries[i].DATA_TYPE_LIST)
                                                          ,"sizeConstraints"          : JSON.parse(sqlQueries[i].SIZE_CONSTRAINTS)
                                                          ,"exportSelectList"         : (serverGeneration) ? sqlQueries[i].EXPORT_SELECT_LIST : sqlQueries[i].NODE_SELECT_LIST 
                      })}`)                
      }
    
      exportFile.write('},"data":{');
       for (let i=0; i < sqlQueries.length; i++) {
        await writeTableName(i,`"${sqlQueries[i].TABLE_NAME}" :`,exportFile);
        let rows;
        let startTime;
        if (serverGeneration ) {
          let dataOffset = exportFile.bytesWritten + exportFile.writableLength;
          try {
            startTime = new Date().getTime()
            rows = await jsonFetchData(conn,status,sqlQueries[i].SQL_STATEMENT,sqlQueries[i].TABLE_NAME,exportFile,logWriter) 
          } catch(e) {
            if ((e.message) && (e.message.indexOf('ORA-40478') == 0)) {
              if (exportFile.bytesWritten > dataOffset) {
                exportFile = resetStream(exportFile,dataOffset);
              }
              const query = wideTableWorkaround(sqlQueries[i],maxVarcharSize);
              startTime = new Date().getTime()
              rows = await wideFetchData(conn,status,query,sqlQueries[i].TABLE_NAME,sqlQueries[i].COLUMN_LIST,exportFile,logWriter) 
            }
            else {
              throw e;
            }
          }
        }
        else {
          const query = generateClientQuery(sqlQueries[i]);
          startTime = new Date().getTime()
          rows = await processClientData(conn,status,query,sqlQueries[i].TABLE_NAME,sqlQueries[i].COLUMN_LIST,exportFile,logWriter) 
        }   
        const elapsedTime = new Date().getTime() - startTime
        logWriter.write(`${new Date().toISOString()}["${sqlQueries[i].TABLE_NAME}"]: Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
      }
      exportFile.write('}');
    }
    
    exportFile.write('}');
    exportFile.close();
    
    OracleCore.doRelease(conn);
    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Export operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Export operation failed.\n');
      logWriter.write(`${e.stack}\n`);
    }
    else {
        console.log('Export operation Failed.');
        console.log(e);
    }
    if (conn !== undefined) {
      OracleCore.doRelease(conn);
    }
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }    


  
  
}

main();
