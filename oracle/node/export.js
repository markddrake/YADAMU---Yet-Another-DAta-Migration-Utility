"use strict";
const fs = require('fs');
const oracledb = require('oracledb');
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;
const Readable = require('stream').Readable;
const path = require('path');

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

    const results = await conn.execute(sqlGenerateQueries,{schema: schema},{outFormat: oracledb.OBJECT , fetchInfo: {
                                                                                                           COLUMN_LIST:{type: oracledb.STRING}
                                                                                                          ,DATA_TYPE_LIST:{type: oracledb.STRING}
                                                                                                          ,SIZE_CONSTRAINTS:{type: oracledb.STRING}
                                                                                                          ,EXPORT_SELECT_LIST:{type: oracledb.STRING}
                                                                                                          ,IMPORT_SELECT_LIST:{type: oracledb.STRING}
                                                                                                          ,COLUMN_PATTERN_LIST:{type: oracledb.STRING}
                                                                                                          ,DESERIALIZATION_INFO:{type: oracledb.STRING}
                                                                                                          ,SQL_STATEMENT:{type: oracledb.STRING}
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

async function fetchData(conn,status,sqlQuery,outStream) {

  let counter = 0;
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
    counter++;
    this.push(JSON.parse(data.JSON));
    done();
  }

  if (status.sqlTrace) {
    status.sqlTrace.write(`${sqlQuery}\n\/\n`)
  }

  const stream = await conn.queryStream(sqlQuery,[],{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(function(resolve,reject) {  
    jsonStream.on('end',function() {resolve(counter)})
    stream.on('error',function(err){reject(err)});
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
    
async function wideTableWorkaround(tableInfo,varcharSize,outStream) {
    
  let selectList = '';
  const columnList = JSON.parse('[' + tableInfo.COLUMN_LIST + ']');
  const selectListMembers = [];
  let start = 0;
  let level = 0;
  for (let i=0; i < tableInfo.EXPORT_SELECT_LIST.length; i++) {
    if (tableInfo.EXPORT_SELECT_LIST[i] === '(') {
      level++;
      continue;
    }
    if (tableInfo.EXPORT_SELECT_LIST[i] === ')') {
      level--;
      continue;
    }
    if ((level === 0) && (tableInfo.EXPORT_SELECT_LIST[i] === ',')) {
      selectListMembers.push(tableInfo.EXPORT_SELECT_LIST.substring(start,i));
      start = i+1;
    }
  }
  selectListMembers.push(tableInfo.EXPORT_SELECT_LIST.substring(start));
  columnList.forEach(function(column,index){
                       let selectListEntry = `JSON_ARRAY(${selectListMembers[index]} NULL on NULL RETURNING VARCHAR2(${varcharSize})) "${column}"`;
                       if (index > 0) {
                         selectListEntry = ',' + selectListEntry;
                       }
                       selectList = selectList + selectListEntry;
  })
   
  let sqlStatement = `select ${selectList} from "${tableInfo.OWNER}"."${tableInfo.TABLE_NAME}"`;
  
  if (tableInfo.SQL_STATEMENT.indexOf('WITH') === 0) {
     const endOfWithClause = tableInfo.SQL_STATEMENT.indexOf('select JSON_ARRAY(');
     sqlStatement = tableInfo.SQL_STATEMENT.substring(0,endOfWithClause) + sqlStatement;
  }
  
  return sqlStatement;

  let counter = 0;
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
    counter++;
    const rowArray = []
    data.forEach(function(json) {
      rowArray.push(JSON.parse(json)[0]);
    })
    this.push(rowArray);
    done();
  }

  const stream = await conn.queryStream(sqlStatement)
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(function(resolve,reject) {  
    jsonStream.on('end',function() {resolve(counter)})
    stream.on('error',function(err){reject(err)});
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
  })
    
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
                        ,"schema"             : parameters.OWNER
                        ,"exportVersion"      : EXPORT_VERSION
                        ,"sessionUser"        : sysInfo.SESSION_USER
                        ,"dbName"             : sysInfo.DATABASE_NAME
                        ,"serverHostName"     : sysInfo.SERVER_HOST
                        ,"databaseVersion"    : sysInfo.DATABASE_RELEASE
                        ,"jsonFeatures"       : JSON.parse(sysInfo.JSON_FEATURES)
                        ,"nlsParameters"      : JSON.parse(sysInfo.NLS_PARAMETERS)
    }));

    const varcharSize = JSON.parse(sysInfo.JSON_FEATURES).extendedString ? 32767 : 4000;
    
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
                                                          ,"sizeConstraints"           : JSON.parse(sqlQueries[i].SIZE_CONSTRAINTS)
                                                          ,"exportSelectList"         : sqlQueries[i].EXPORT_SELECT_LIST
                                                          ,"insertSelectList"         : sqlQueries[i].IMPORT_SELECT_LIST
                                                          ,"deserializationFunctions" : sqlQueries[i].DESERIALIZATION_INFO
                                                          ,"columnPatterns"           : sqlQueries[i].COLUMN_PATTERN_LIST
                      })}`)                
      }
    
      exportFile.write('},"data":{');
       for (let i=0; i < sqlQueries.length; i++) {
        await writeTableName(i,`"${sqlQueries[i].TABLE_NAME}" :`,exportFile);
        let dataOffset = exportFile.bytesWritten + exportFile.writableLength;
        let rows;
        let startTime;
        try {
          startTime = new Date().getTime()
          rows = await fetchData(conn,status,sqlQueries[i].SQL_STATEMENT,exportFile) 
        } catch(e) {
          if ((e.message) && (e.message.indexOf('ORA-40478') == 0)) {
            if (exportFile.bytesWritten > dataOffset) {
              console.log(exportFile.bytesWritten);
              console.log(dataOffset);
              exportFile = resetStream(exportFile,dataOffset);
            }
            const sqlWideTable = wideTableWorkaround(sqlQueries[i],varcharSize,exportFile);
            startTime = new Date().getTime()
            rows = await fetchData(conn,status,sqlWideTable.SQL_STATEMENT,exportFile) 
          }
          else {
            throw e;
          }
        }
        const elapsedTime = new Date().getTime() - startTime
        logWriter.write(`${new Date().toISOString()} - Table: "${sqlQueries[i].TABLE_NAME}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
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
      logWriter.write(e.stack);
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

  if (status.sqlTrace) {
    sqlTrace.close();
  }
  
}

main();
