"use strict"
const { Transform } = require('stream');
const { Writable } = require('stream');
const mysql = require('mysql')
const common = require('./common.js');
// const clarinet = require('clarinet');
const clarinet = require('../../clarinet/clarinet.js');
const fs = require('fs');


const unboundedTypes = ['date','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum'];
const spatialTypes   = ['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'];
const nationalTypes  = ['nchar','nvarchar'];
const integerTypes   = ['tinyint','mediumint','smallint','int','bigint']
   
// Will be set to generateStatementCacheLocal if JSON_TABLE is not supported or generateStatementCacheRemote if JSON_TABLE is supported
// JSON_TABLE is supported starting with MYSQL 8.0

let generateStatementCache = undefined;

function connect(conn) {
    
  return new Promise(function(resolve,reject) {
                       conn.connect(function(err) {
                                      if (err) {
                                        reject(err);
                                      }
                                      resolve();
                                    })
                    })
}   
      
function query(conn,sqlQuery,args) {
    
  return new Promise(function(resolve,reject) {
                       conn.query(sqlQuery,args,function(err,rows,fields) {
                                             if (err) {
                                               reject(err);
                                             }
                                             resolve(rows);
                                           })
                     })
}  

class RowParser extends Transform {
  
  constructor(logWriter, options) {

    super({objectMode: true });  
  
    const rowParser = this;
    
    this.logWriter = logWriter;

    this.saxJParser = clarinet.createStream();
    this.saxJParser.on('error',function(err) {this.logWriter.write(`$(err}\n`);})
    
    this.objectStack = [];
    this.dataPhase = false;     
    
    this.currentObject = undefined;
    this.chunks = [];

    this.jDepth = 0;
       
    this.saxJParser.onkey = function (key) {
      // rowParser.logWriter.write(`onKey(${rowParser.jDepth},${key})\n`);
      
      switch (rowParser.jDepth){
        case 1:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          rowParser.push(rowParser.currentObject);
          if (Array.isArray(rowParser.currentObject)) {
             rowParser.currentObject = [];
          }
          else {
             rowParser.currentObject = {};
          }
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
      // Push the current object onto the stack and the current object to the key
      rowParser.objectStack.push(rowParser.currentObject);
      rowParser.currentObject = key;
    };

    this.saxJParser.onopenobject = function (key) {
      // rowParser.logWriter.write(`onOpenObject(${rowParser.jDepth}:, Key:"${key}". ObjectStack:${rowParser.objectStack}\n`);      
      
      if (rowParser.jDepth > 0) {
        rowParser.objectStack.push(rowParser.currentObject);
      }
         
      switch (rowParser.jDepth) {
        case 0:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          if (rowParser.currentObject !== undefined) {
            rowParser.push(rowParser.currentObject);
          }  
          if (key === 'data') {
            rowParser.dataPhase = true;
          }
          break;
        case 1:
          if ((rowParser.dataPhase) && (key != undefined)) {
            rowParser.push({ table : key});
          }
          break;
        default:
      }
      // If the object has a key put the object on the stack and set the current object to the key. 
      rowParser.currentObject = {}
      rowParser.jDepth++;
      if (key !== undefined) {
        rowParser.objectStack.push(rowParser.currentObject);
        rowParser.currentObject = key;
      }
    };

    this.saxJParser.onopenarray = function () {
      // rowParser.logWriter.write(`onOpenArray(${rowParser.jDepth}): ObjectStack:${rowParser.objectStack}\n`);
      if (rowParser.jDepth > 0) {
        rowParser.objectStack.push(rowParser.currentObject);
      }
      rowParser.currentObject = [];
      rowParser.jDepth++;
    };


    this.saxJParser.onvaluechunk = function (v) {
      rowParser.chunks.push(v);  
    };
       
    this.saxJParser.onvalue = function (v) {
      // rowParser.logWriter.write(`onvalue(${rowParser.jDepth}: ObjectStack:${rowParser.objectStack}\n`);        
      if (rowParser.chunks.length > 0) {
        rowParser.chunks.push(v);
        v = rowParser.chunks.join('');
        rowParser.chunks = []
      }
      
      if (typeof v === 'boolean') {
        v = new Boolean(v).toString();
      }
      
      if (Array.isArray(rowParser.currentObject)) {
          // currentObject is an ARRAY. We got a value so add it to the Array
          rowParser.currentObject.push(v);
      }
      else {
          // currentObject is an Key. We got a value so fetch the parent object and add the KEY:VALUE pair to it. Parent Object becomes the Current Object.
          const parentObject = rowParser.objectStack.pop();
          parentObject[rowParser.currentObject] = v;
          rowParser.currentObject = parentObject;
      }
    }
      
    this.saxJParser.oncloseobject = async function () {
      // rowParser.logWriter.write(`onCloseObject(${rowParser.jDepth}):\nObjectStack:${rowParser.objectStack})\nCurrentObject:${rowParser.currentObject}\n`);           
      rowParser.jDepth--;

      // An object can belong to an Array or a Key
      if (rowParser.objectStack.length > 0) {
        let owner = rowParser.objectStack.pop()
        let parentObject = undefined;
        if (Array.isArray(owner)) {   
          parentObject = owner;
          parentObject.push(rowParser.currentObject);
        }    
        else {
          parentObject = rowParser.objectStack.pop()
          if (!this.emptyObject) {
            parentObject[owner] = rowParser.currentObject;
          }
        }   
        rowParser.currentObject = parentObject;
      }
    }
   
    this.saxJParser.onclosearray = function () {
      // rowParser.logWriter.write(`onclosearray(${rowParser.jDepth}: ObjectStack:${rowParser.objectStack}. CurrentObject:${rowParser.currentObject}\n`);          
      let skipObject = false;
      
      if ((rowParser.dataPhase) && (rowParser.jDepth === 4)) {
        rowParser.push({ data : rowParser.currentObject});
        skipObject = true;
      }

      rowParser.jDepth--;

      // An Array can belong to an Array or a Key
      if (rowParser.objectStack.length > 0) {
        let owner = rowParser.objectStack.pop()
        let parentObject = undefined;
        if (Array.isArray(owner)) {   
          parentObject = owner;
          if (!skipObject) {
            parentObject.push(rowParser.currentObject);
          }
        }    
        else {
          parentObject = rowParser.objectStack.pop()
          if (!skipObject) {
            parentObject[owner] = rowParser.currentObject;
          }
        }
        rowParser.currentObject = parentObject;
      }   
    }

   }  
   
  _transform(data,enc,callback) {
    this.saxJParser.write(data);
    callback();
  };
}

function decomposeDataType(targetDataType) {
    
  const results = {};
    
  let components = targetDataType.split('(');
  results.type = components[0];
  if (components.length > 1 ) {
    components = components[1].split(')');
    components = components[0].split(',');
    if (components.length > 1 ) {
      results.length = parseInt(components[0]);
      results.scale = parseInt(components[1]);
    }
    else {
      if (components[0] === 'max') {
        results.length = sql.MAX;
      }
      else {
        results.length = parseInt(components[0])
      }
    }
  }           
   
  return results;      
    
}      

function mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeSize) {
  switch (vendor) {
     case 'Oracle':
       switch (dataType) {
         case 'VARCHAR2':        return 'varchar';
         case 'NVARCHAR2':       return 'varchar';
         case 'NUMBER':          return 'decimal';
         case 'CLOB':            return 'longtext';
         case 'BLOB':            return 'longblob';
         case 'NCLOB':           return 'longtext';
         case 'XMLTYPE':         return 'longtext';
         case 'BFILE':           return 'varchar(2048)';
         case 'ROWID':           return 'varchar(32)';
         case 'RAW':             return 'binary';
         case 'ROWID':           return 'varchar(32)';
         case 'ANYDATA':         return 'longtext';
         default :
           if (dataType.indexOf('TIME ZONE') > -1) {
             return 'timestamp'; 
           }
           if (dataType.indexOf('INTERVAL') === 0) {
             return 'timestamp'; 
           }
           if (dataType.indexOf('XMLTYPE') > -1) { 
             return 'varchar(16)';
           }
           if (dataType.indexOf('.') > -1) { 
             return 'longtext';
           }
           return dataType.toLowerCase();
       }
       break;
     case 'MSSQLSERVER':
       switch (dataType) {
         case 'binary':
           switch (true) {
             case (dataTypeLength > 16777215):   return 'longblob';
             case (dataTypeLength > 65535):      return 'mediumblob';
             default:                            return 'binary';
           }
         case 'bit':                             return 'tinyint(1)';
         case 'char':
           switch (true) {
             case (dataTypeLength === -1):       return 'longtext';
             case (dataTypeLength > 16777215):   return 'longtext';
             case (dataTypeLength > 65535):      return 'mediumtext';
              case (dataTypeLength > 255):       return 'text';
              default:                           return 'char';
           }
         case 'datetime':                        return 'datetime(3)';
         case 'datetime2':                       return 'datetime';
         case 'datetimeoffset':                  return 'datetime';
         case 'geography':                       return 'json';
         case 'geogmetry':                       return 'json';
         case 'hierarchyid':                     return 'varbinary(446)';
         case 'image':                           return 'longblob';
         case 'mediumint':                       return 'int';
         case 'money':                           return 'decimal(19,4)';
         case 'nchar':
           switch (true) {
              case (dataTypeLength === -1):      return 'longtext';
              case (dataTypeLength > 16777215):  return 'longtext';
              case (dataTypeLength > 65535):     return 'mediumtext';
              case (dataTypeLength > 255):       return 'text';
              default:                           return 'char';
           }
         case 'ntext':                           return 'longtext';
         case 'nvarchar':
           switch (true) {
             case (dataTypeLength === -1):       return 'longtext';
             case (dataTypeLength > 16777215):   return 'longtext';
             case (dataTypeLength > 65535):      return 'mediumtext';
             default:                            return 'varchar';
           }             
         case 'real':                            return 'float';
         case 'rowversion':                      return 'binary(8)';
         case 'smalldate':                       return 'datetime';
         case 'smallmoney':                      return 'decimal(10,4)';
         case 'text':                            return 'longtext';
         case 'tinyint':                         return 'smallint';
         case 'uniqueidentifier':                return 'varchar(64)';
         case 'varbinary':
           switch (true) {
             case (dataTypeLength === -1):       return 'longblob';
             case (dataTypeLength > 16777215):   return 'longblob';
             case (dataTypeLength > 65535):      return 'mediumblob';
             default:                            return 'varbinary';
           }
         case 'varchar':
           switch (true) {
             case (dataTypeLength === -1):       return 'longtext';
             case (dataTypeLength > 16777215):   return 'longtext';
             case (dataTypeLength > 65535):      return 'mediumtext';
             default:                            return 'varchar';
           }
         case 'xml':                             return 'longtext';
         default:                                return dataType.toLowerCase();
       }
       break;
     case 'Postgres':                            return dataType.toLowerCase();
       break
     case 'MySQL':
       switch (dataType) {
         case 'set':                             return 'text';
         case 'enum':                            return 'text';
         default:                                return dataType.toLowerCase();
       }
       break;
     case 'MariaDB':
       switch (dataType) {
         case 'set':                             return 'text';
         case 'enum':                            return 'text';
         default:                                return dataType.toLowerCase();
       }
       break;
     default:                                    return dataType.toLowerCase();
  }  
}
    
function generateStatements(vendor, schema, metadata) {
    
   let useSetClause = false;
   
   const columnNames = metadata.columns.split(',');
   const dataTypes = metadata.dataTypes.split(',');
   const sizeConstraints = JSON.parse('[' + metadata.dataTypeSizing.replace(/\"\.\"/g, '\",\"') + ']');
   const targetDataTypes = [];
   const setOperators = []

   const columnClauses = columnNames.map(function(columnName,idx) {    
                                           const dataType = dataTypes[idx].replace(/\"/g, "");
                                           const sizeConstraint = sizeConstraints[idx].replace(/\"/g, "");
                                           let dataLength = null;
                                           let dataScale = null;
                                           let qualifier = ''
                                           if (sizeConstraint.length > 0) {
                                             dataLength = sizeConstraint;
                                             const scaleOffset = dataLength.indexOf(',');
                                             if (scaleOffset > -1) {
                                               dataScale = parseInt(dataLength.substring(scaleOffset+1))
                                               dataLength = parseInt(dataLength.substring(0,scaleOffset))
                                             }
                                             else {
                                               dataLength = parseInt(dataLength);
                                             }
                                           }

                                           let targetDataType = mapForeignDataType(vendor,dataType,dataLength,dataScale);
                                           targetDataTypes.push(targetDataType);
                                           
                                           switch (targetDataType) {
                                             case 'geometry':
                                                useSetClause = true;
                                                setOperators.push(' "' + columnName + '" = ST_GEOMFROMGEOJSON(?)');
                                                break;
                                                
                                             default:
                                               setOperators.push(' "' + columnName + '" = ?')
                                           }
                                           
                                           switch (true) {
                                              case (RegExp(/\(.*\)/).test(targetDataType)):
                                                break;
                                              case (targetDataType.endsWith(" unsigned")):
                                                break;
                                              case unboundedTypes.includes(targetDataType):
                                                break;
                                              case spatialTypes.includes(targetDataType):
                                                break;
                                              case nationalTypes.includes(targetDataType):
                                                targetDataType = targetDataType + '(' + dataLength + ')';
                                                break;
                                              case (dataScale != null):
                                                switch (true) {
                                                   case integerTypes.includes(targetDataType):
                                                     targetDataType = targetDataType + '(' + dataLength + ')';
                                                     break;
                                                   default:
                                                     targetDataType = targetDataType + '(' + dataLength + ',' + dataScale + ')';
                                                }
                                                break;
                                              case (dataLength != null):
                                                switch (true) {
                                                  case (targetDataType === 'double'):
                                                    // Do not add length restriction when scale is not specified
                                                    break;
                                                  default:
                                                    targetDataType = targetDataType + '(' + dataLength + ')';
                                                    break;
                                                }
                                                break;
                                              default:
                                           }
                                           return `${columnName} ${targetDataType} ${qualifier}\n `;
                                        })
                                      
    const createStatement = `create table if not exists "${schema}"."${metadata.tableName}"(\n  ${columnClauses.join(',')})`;
    let insertStatement = `insert into "${schema}"."${metadata.tableName}"`;
    if (useSetClause) {
      insertStatement += ` set` + setOperators.join(',');
    }
    else {
      insertStatement += `(${metadata.columns}) values ?`;
    }
    return { ddl : createStatement, dml : insertStatement, targetDataTypes : targetDataTypes, useSetClause : useSetClause}
}

async function generateStatementCacheLocal(conn, schema, systemInformation, metadata, status, logWriter) {

  const statementCache = {}
  const tables = Object.keys(metadata); 
  await Promise.all(tables.map(async function(table,idx) {
                                       const tableMetadata = metadata[table];
                                       const sql = generateStatements(systemInformation.vendor, schema,tableMetadata);
                                       statementCache[table] = sql;
                                       try {
                                         if (status.sqlTrace) {
                                           status.sqlTrace.write(`${statementCache[table].ddl};\n--\n`);
                                         }
                                         const results = await conn.query(statementCache[table].ddl);   
                                       } catch (e) {
                                         logWriter.write(`${e}\n${statementCache[table].ddl}\n`)
                                       }
  }))
  return statementCache;
}

async function generateStatementCacheRemote(conn, schema, systemInformation, metadata, status,logWriter) {
    
 
  const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS";`;                       
 
  let results = await query(conn,sqlStatement,[JSON.stringify({systemInformation: systemInformation, metadata : metadata}),schema]);
  results = results.pop();
  const statementCache = JSON.parse(results[0].SQL_STATEMENTS)
  const tables = Object.keys(metadata); 
  await Promise.all(tables.map(async function(table,idx) {
                                       const tableInfo = statementCache[table];
                                       const columnNames = JSON.parse('[' + metadata[table].columns + ']');
                                       tableInfo.targetDataTypes = JSON.parse('[' + tableInfo.targetDataTypes + ']');

                                       tableInfo.useSetClause = false;
                                       const setOperators = tableInfo.targetDataTypes.map(function(targetDataType,idx) {
                                                                                            switch (targetDataType) {
                                                                                              case 'geometry':
                                                                                                 tableInfo.useSetClause = true;
                                                                                                return ' "' + columnNames[idx] + '" = ST_GEOMFROMGEOJSON(?)';
                                                                                              default:
                                                                                                return ' "' + columnNames[idx] + '" = ?'
                                                                                            }
                                       })
                                       
                                       if (tableInfo.useSetClause) {
                                         tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('(')) + ` set ` + setOperators.join(',');
                                       }
                                       else {
                                         tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values ?`;
                                         }
                                        
                                       if (status.sqlTrace) {
                                         status.sqlTrace.write(`${tableInfo.ddl};\n--\n`);
                                       }
                                       try {
                                         const results = await query(conn,tableInfo.ddl);   
                                       } catch (e) {
                                         logWriter.write(`${e}\n${tableInfo.ddl}\n`)
                                       }  
  }));
  
  // console.log(metadata)
  // console.log(statementCache)
  
  return statementCache;
}

class DbWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,mode,status,logWriter,options) {
    super({objectMode: true });
    const dbWriter = this;
    
    this.conn = conn;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;

    this.systemInformation = undefined;
    this.metadata = undefined;
    this.statementCache = undefined;
    
    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;
    
    this.batch = [];
  }      
  
  async setTable(tableName) {
       
    this.tableName = tableName
    this.tableInfo =  this.statementCache[tableName]
    this.rowCount = 0;
    this.batch.length = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
   async writeBatch(status) {
    
     try {
      if (this.tableInfo.useSetClause) {
        for (const i in this.batch) {
          try {
            const results = await query(this.conn,this.tableInfo.dml,this.batch[i]);
          } catch(e) {
            if (e.errno && ((e.errno === 3616) || (e.errno === 3617))) {
              this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping Row Reason: ${e.message}\n`)
              this.rowCount--;
            }
            else {
              throw e;
            }
          }    
        }
      }
      else {  
        const results = await query(this.conn,this.tableInfo.dml,[this.batch]);
      }
      const endTime = new Date().getTime();
      this.batch.length = 0;
      return endTime
    } catch (e) {
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Reason: ${e.message}\n`)
      this.logWriter.write(`${this.tableInfo.dml}[${this.batch.length}]...\n`);
      this.batch.length = 0;
      this.skipTable = true;
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.dml}\n`);
        this.logWriter.write(`${this.batch}\n`);
      }      
    }
  }

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          this.statementCache = await generateStatementCache(this.conn, this.schema, this.systemInformation, this.metadata, this.status, this.logWriter)
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName !== undefined) {
            if (this.batch.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
              await this.conn.commit();
            }  
            if (!this.skipTable) {
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTable(obj.table);
          await this.conn.beginTransaction();
          if (this.status.sqlTrace) {
             this.status.sqlTrace.write(`${this.tableInfo.dml};\n--\n`);
          }
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
                                                 const dataType = decomposeDataType(targetDataType);
                                                   if (obj.data[idx] !== null) {
                                                     switch (dataType.type) {
                                                       case "tinyblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "blob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "mediumblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "longblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "varbinary" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "binary" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "geometry":
                                                         obj.data[idx] = JSON.stringify(obj.data[idx]);
                                                         break;
                                                       case "json" :
                                                         obj.data[idx] = JSON.stringify(obj.data[idx]);
                                                         break;
                                                       case "timezone" :
                                                         obj.data[idx] = new Date(Date.parse(obj.data[idx]));
                                                         break;
                                                       default :
                                                     }
                                                   }
                                                 },this)

          this.batch.push(obj.data);
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batch.length} rows.`);
          if (this.batch.length  === this.batchSize) {
              //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             await this.conn.commit();
             const elapsedTime = this.endTime - this.startTime;
             // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
             await this.conn.beginTransaction();
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.tableName) {        
        if (!this.skipTable) {
          if (this.batch.length > 0) {
            // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
           this.endTime = await this.writeBatch();
          }  
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          await this.conn.commit();
        }
      }          
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n`);
      callback(e);
    } 
  } 
}

function processFile(conn, schema, dumpFilePath, batchSize, commitSize, mode, status, logWriter) {
  
  return new Promise(function (resolve,reject) {
    const dbWriter = new DbWriter(conn,schema,batchSize,commitSize,mode,status,logWriter);
    const rowGenerator = new RowParser(logWriter);
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
  
  const status = {
    errorRaised   : false
   ,warningRaised : false
   ,statusMsg     : 'successfully'
  }
  
  let results;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`Unhandled Rejection:\Error:`);
      logWriter.write(`${err}\n${err.stack}\n`);
    })
    
    parameters = common.processArguments(process.argv,'export');

    if (parameters.LOGFILE) {
      logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

    if (parameters.SQLTRACE) {
      status.sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }
    
    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
           ,database  : parameters.DATABASE
           ,multipleStatements: true
    }

    conn = mysql.createConnection(connectionDetails);
    await connect(conn);

    const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
    results = await query(conn,`SELECT @@max_allowed_packet`);
    
    if (parseInt(results[0]['@@max_allowed_packet']) <  maxAllowedPacketSize) {
        logWriter.write(`${new Date().toISOString()}: Increasing MAX_ALLOWED_PACKET to 1G.\n`);
        results = await query(conn,`SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`);
        await conn.end();
        conn = mysql.createConnection(connectionDetails);
        await connect(conn);
    }    

    results = await query(conn,`SELECT @@version`);
    if (results[0]['@@version'] > '8.0') {
       generateStatementCache = generateStatementCacheRemote
    }
    else {
      generateStatementCache = generateStatementCacheLocal
    }
  
    // Force 5.7 Code Path
    // generateStatementCache = generateStatementCacheLocal
 
    results = await query(conn,`SET SESSION SQL_MODE=ANSI_QUOTES`);
    results = await query(conn,`CREATE DATABASE IF NOT EXISTS "${parameters.TOUSER}"`); 
    
    const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    
    if (parameters.LOGLEVEL) {
       status.loglevel = parameters.LOGLEVEL;
    }
        
    await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    
    await conn.end();

    status.statusMsg = status.warningRaised ? 'with warnings' : status.statusMsg;
    status.statusMsg = status.errorRaised ? 'with errors'  : status.statusMsg;
     
    logWriter.write(`Import operation completed ${status.statusMsg}.`);
    if (logWriter !== process.stdout) {
       console.log(`Import operation completed ${status.statusMsg}: See "${parameters.LOGFILE}" for details.`);
    }
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Import operation failed.\n');
      logWriter.write(`${e}\n`);
    }
    else {
      console.log(`Import operation Failed:`);
      console.log(e);
    }
    if (conn !== undefined) {
      await conn.end();
    }
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

  if (parameters.SQLTRACE) {
    status.sqlTrace.close();
  }
}
    
main()


 