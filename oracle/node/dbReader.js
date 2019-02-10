"use strict";
const Readable = require('stream').Readable;
const Transform = require('stream').Transform;

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const StringWriter = require('./StringWriter');
const BufferWriter = require('./BufferWriter');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'Oracle';
const SOFTWARE_VENDOR = 'Oracle Corporation';
const SPATIAL_FORMAT = "WKT";

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


class DBReader extends Readable {  

  constructor(conn,schema,outputStream,mode,status,logWriter,options) {

    super({objectMode: true });  
    const self = this;
  
    this.conn = conn;
    this.schema = schema;
    this.outputStream = outputStream;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBReader ready. Mode: ${this.mode}.\n`)
        
    this.tableInfo = [];
    
    this.nextPhase = 'systemInformation'
    this.serverGeneration = undefined;
    this.maxVarcharSize = undefined;
  
  }
  
  async getSystemInformation() {     

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }

    const results = await this.conn.execute(sqlGetSystemInformation,[],{outFormat: oracledb.OBJECT ,})
    const sysInfo = results.rows[0];
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : DATABASE_VENDOR
     ,spatialFormat      : SPATIAL_FORMAT 
     ,schema             : this.schema
     ,exportVersion      : EXPORT_VERSION
     ,sessionUser        : sysInfo.SESSION_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_RELEASE
     ,softwareVendor     : SOFTWARE_VENDOR
     ,hostname           : sysInfo.SERVER_HOST
     ,jsonFeatures       : JSON.parse(sysInfo.JSON_FEATURES)
     ,nlsParameters      : JSON.parse(sysInfo.NLS_PARAMETERS)
    }
    
  }

  async getDDLOperations() {
    
    const results = await this.conn.execute(sqlFetchDDL,{schema: this.schema},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
    const ddl = results.rows.map(function(row) {
      return row.JSON;
    })
    return ddl;    

  }
  
  async getMetadata() {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }

    const results = await this.conn.execute(sqlGenerateQueries,{schema: this.schema},{outFormat: oracledb.OBJECT , fetchInfo:{
                                                                                                                     COLUMN_LIST:          {type: oracledb.STRING}
                                                                                                                    ,DATA_TYPE_LIST:       {type: oracledb.STRING}
                                                                                                                    ,SIZE_CONSTRAINTS:     {type: oracledb.STRING}
                                                                                                                    ,EXPORT_SELECT_LIST:   {type: oracledb.STRING}
                                                                                                                    ,NODE_SELECT_LIST:     {type: oracledb.STRING}
                                                                                                                    ,WITH_CLAUSE:          {type: oracledb.STRING}
                                                                                                                    ,SQL_STATEMENT:        {type: oracledb.STRING}
                                                                                                                   }
    });
      
    this.tableInfo = results.rows;
    const metadata = {}
	for (let table of this.tableInfo) {
      metadata[table.TABLE_NAME] = {
        owner                    : table.OWNER
       ,tableName                : table.TABLE_NAME
       ,columns                  : table.COLUMN_LIST
       ,dataTypes                : JSON.parse(table.DATA_TYPE_LIST)
       ,sizeConstraints          : JSON.parse(table.SIZE_CONSTRAINTS)
       ,exportSelectList         : (this.serverGeneration) ? table.EXPORT_SELECT_LIST : table.NODE_SELECT_LIST 
      }
    }
    return metadata;    
  }
  

  
  /*
  
  async jsonFetchData(conn,status,sqlStatement,tableName,outputStream,logWriter) {

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
      stream.pipe(parser).pipe(jsonStream).pipe(outputStream,{end: false })
    })
  }

  async wideFetchData(conn,status,query,tableName,columnList,outputStream,logWriter) {
  
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
      stream.pipe(parser).pipe(jsonStream).pipe(outputStream,{end: false })
    })
      
  }
  
  
  resetStream(fileWriteStream,offset) {
      const path = fileWriteStream.path;
      fileWriteStream.close();
      return fs.createWriteStream(path,{start:offset,flags:"r+"});
  }
      
  decomposeSelectList(exportSelectList) {
  
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
   
  wideTableWorkaround(tableInfo,maxVarcharSize) {
     
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
  */
  
  generateClientQuery(tableInfo) {
     
    // Perform a traditional relational select..
    
    const queryInfo = {
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
          queryInfo.jsonColumns.push(idx);
          break
        case 'RAW': 
          queryInfo.rawColumns.push(idx);
          break;
        default:
      }
    })
    
    queryInfo.sqlStatement = `select ${tableInfo.NODE_SELECT_LIST} from "${tableInfo.OWNER}"."${tableInfo.TABLE_NAME}" t`; 
    
    if (tableInfo.WITH_CLAUSE !== null) {
       queryInfo.sqlStatement = `with\n${tableInfo.WITH_CLAUSE}\n${queryInfo.sqlStatement}`;
    }
    
    return queryInfo
  
  }
  
  async pipeTableData(queryInfo,outputStream) {

    function waitUntilEmpty(outputStream,resolve) {
        
      const recordsRemaining = outputStream.writableLength;
      if (recordsRemaining === 0) {
        resolve(counter);
      } 
      else  {
        // console.log(`${new Date().toISOString()}[${DATABASE_VENDOR}]: DBReader Records Reamaining ${recordsRemaining}.`);
        setTimeout(waitUntilEmpty, 10,outputStream,resolve);
      }   
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
      queryInfo.jsonColumns.forEach(function(idx) {
         if (data[idx] !== null) {
           try {
             data[idx] = JSON.parse(data[idx]) 
           } catch (e) {
             logWriter.write(`${counter}:${e}\n`);
             logWriter.write(`${data[idx]}\n`);
           }
         }
      })
      queryInfo.rawColumns.forEach(function(idx) {
         if (data[idx] !== null) {
           if(Buffer.isBuffer(data[idx])) {
             data[idx] = data[idx].toString('hex');
           }
         }
      })
      if (!outputStream.objectMode()) {
        data = JSON.stringify(data);
      }
      const res = this.push({data:data})
      // console.log(counter,':',res);
      done();
    }
  
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${queryInfo.sqlStatement}\n\/\n`)
    }
  
    const stream = await this.conn.queryStream(queryInfo.sqlStatement,[],{extendedMetaData: true})
    
    return new Promise(function(resolve,reject) {  
      const outputStreamError = function(err){reject(err)}       
      outputStream.on('error',outputStreamError);
      parser.on('finish',function() {outputStream.removeListener('error',outputStreamError);waitUntilEmpty(outputStream,resolve)})
      parser.on('error',function(err){reject(err)});
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
      stream.pipe(parser).pipe(outputStream,{end: false })
    })
      
  }
  
  async getTableData(tableInfo) {
  /*
        if (this.serverGeneration ) {
          let dataOffset = exportFile.bytesWritten + exportFile.writableLength;
          try {
            startTime = new Date().getTime()
            rows = await jsonFetchData(conn,status,tableInfo[i].SQL_STATEMENT,tableInfo[i].TABLE_NAME,exportFile,logWriter) 
          } catch(e) {
            if ((e.message) && (e.message.indexOf('ORA-40478') == 0)) {
              if (exportFile.bytesWritten > dataOffset) {
                exportFile = resetStream(exportFile,dataOffset);
              }
              const query = wideTableWorkaround(tableInfo[i],maxVarcharSize);
              startTime = new Date().getTime()
              rows = await wideFetchData(conn,status,query,tableInfo[i].TABLE_NAME,tableInfo[i].COLUMN_LIST,exportFile,logWriter) 
            }
            else {
              throw e;
            }
          }
        }
        else {
          const query = generateClientQuery(sqlStatement);
          startTime = new Date().getTime()
          rows = await processClientData(conn,status,query,tableInfo[i].TABLE_NAME,tableInfo[i].COLUMN_LIST,exportFile,logWriter) 
        }   
        const elapsedTime = new Date().getTime() - startTime
        logWriter.write(`${new Date().toISOString()}["${tableInfo[i].TABLE_NAME}"]: Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
      }
  */
    const queryInfo = this.generateClientQuery(tableInfo);
    const startTime = new Date().getTime()
    const rows = await this.pipeTableData(queryInfo,this.outputStream) 
    const elapsedTime = new Date().getTime() - startTime
    this.logWriter.write(`${new Date().toISOString()}[DBReader "${tableInfo.TABLE_NAME}"]: Rows read: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
    return rows;
  }
  
  async _read() {

    try {
      switch (this.nextPhase) {
         case 'systemInformation' :
           const sysInfo = await this.getSystemInformation();
           this.push({systemInformation : sysInfo});
           if (this.mode === 'DATA_ONLY') {
             this.nextPhase = 'metadata';
           }
           else { 
             this.nextPhase = 'ddl';
           }
           break;
         case 'ddl' :
           const ddl = await this.getDDLOperations();
           this.push({ddl: ddl});
           if (this.mode === 'DDL_ONLY') {
             this.push(null);
           }
           else {
             this.nextPhase = 'metadata';
           }
           break;
         case 'metadata' :
           const metadata = await this.getMetadata();
           this.push({metadata: metadata});
           this.nextPhase = 'table';
           break;
         case 'table' :
           if (this.mode !== 'DDL_ONLY') {
             if (this.tableInfo.length > 0) {
               this.push({table : this.tableInfo[0].TABLE_NAME})
               this.nextPhase = 'data'
               break;
             }
           }
           this.push(null);
           break;
         case 'data' :
           const rows = await this.getTableData(this.tableInfo[0])
           this.push({rowCount:rows});
           this.tableInfo.splice(0,1)
           this.nextPhase = 'table';
           break;
         default:
      }
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }
  }
}

module.exports = DBReader;

