const { Transform } = require('stream');
const { Writable } = require('stream');
const mariadb = require('mariadb');
const common = require('./common.js');
const clarinet = require('clarinet');
const fs = require('fs');

const unboundedTypes = ['tinyint','smallint','mediumint','int','set','enum','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json'];
const spatialTypes = ['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'];
const nationalTypes = ['nchar','nvarchar'];
   
function mapForeignDataType(dataType, dataTypeLength, dataTypeSize) {
  switch (dataType) {
    // TODO : Enable Roundtrip for ENUM and SET
	case 'set':
      return 'text';
	case 'enum':
     return 'text';
    // Oracle Mappings
	case 'VARCHAR2':
     return 'varchar';
	case 'NUMBER':
      return 'decimal';
	case 'CLOB':
      return 'text';
	case 'NCLOB':
      return 'text';
	case 'BFILE':
      return 'varchar(2048)';
	case 'ROWID':
      return 'varchar(32)';
   // SQLServer Mapppings
	case 'nchar':
      return 'char';
	case 'tinyint':
      return 'tinyint unsigned';
	case 'bit':
      return 'tinyint(1)';
	case 'real':
      return 'float';
	case 'numeric':
      return 'decimal';
	case 'money':
      return 'decimal';
	case 'smallmoney':
      return 'decimal';
	case 'char':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
         case (dataTypeLength > 255):
           return 'text';
         default:
           return 'char';
      }
	case 'nchar':
      switch (true) {
         case (dataTypeLength === -1):
           return 'longtext';
         case (dataTypeLength > 16777215):
           return 'longtext';
         case (dataTypeLength > 65535):
           return 'mediumtext';
         case (dataTypeLength > 255):
           return 'text';
         default:
          return 'char';
      }
	case 'nvarchar':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        default:
          return 'varchar';
      }
	case 'varchar':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        default:
           return 'varchar';
      }
	case 'datetime2':
      return 'datatime';
	case 'smalldate':
      return 'datatime';
	case 'datetimeoffset':
      return 'datatime';
	case 'rowversion':
      return 'datatime';
	case 'binary':
      switch (true) {
        case (dataTypeLength > 16777215):
          return 'longblob';
        case (dataTypeLength > 65535):
          return 'mediumblob';
        case (dataTypeLength > 255):
          return 'blob';
        default:
          return 'tinyblob';
      }
	case 'varbinary':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longblob';
        case (dataTypeLength > 16777215):
          return 'longblob';
        case (dataTypeLength > 65535):
          return 'mediumblob';
        default:
          return 'varbinary';
      }
	case 'text':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        case (dataTypeLength > 255):
          return 'text';
        default:
          return 'char';
      }
	case 'ntext':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        case (dataTypeLength > 255):
          return 'text';
        default:
          return 'char';
      }
	case 'image':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longblob';
        case (dataTypeLength > 16777215):
          return 'longblob';
        case (dataTypeLength > 65535):
          return 'mediumblob';
        case (dataTypeLength > 255):
          return 'blob';
        default:
          return 'tinyblob';
      }
	case 'uniqueidentifier':
      return 'varchar(64)';
	case 'hierarchyid':
      return 'varbinary(446)';
	case 'xml':
      return 'longtext';
	default:
   	  if (dataType.indexOf('TIME ZONE') > -1) {
	    return 'timestamp';	
      }
	  if (dataType.indexOf('"."') > -1) { 
	    return 'text';
      }
      
   	  if ((dataType.indexOf('INTERVAL') === 0)) {
	    return 'varchar(16)';
      }
	  return dataType.toLowerCase();
  }
}
    
function generateStatements(schema, metadata) {
    
   const columnNames = metadata.columns.split(',');
   const dataTypes = metadata.dataTypes.split(',');
   const sizeConstraints = JSON.parse('[' + metadata.dataTypeSizing.replace(/\"\.\"/g, '\",\"') + ']');
   const argsList = [];
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
                                               dataScale = dataLength.substring(scaleOffset+1)
                                               dataLength = dataLength.substring(0,scaleOffset)
                                             }
                                           }
                                           let targetDataType = mapForeignDataType(dataType,dataLength,dataScale);
   
                                           switch (true) {
                                              case (targetDataType.indexOf('(') > -1):
                                                break;
                                              case (targetDataType.endsWith(" unsigned")):
                                                break;
                                              case unboundedTypes.includes(targetDataType):
                                                break;
                                              case spatialTypes.includes(targetDataType):
                                                break;
                                              case nationalTypes.includes(targetDataType):
                                                targetDataType = targetDataType + '(' + dataLength + ') CHARACTER SET UTF8MB4';
                                                break;
                                              case (dataScale != null):
                                                targetDataType = targetDataType + '(' + dataLength + ',' + dataScale + ')';
                                                break;
                                              case (dataLength != null):
                                                switch (true) {
                                                  case (targetDataType === 'double'):
                                                    targetDataType = targetDataType + '(' + dataLength + ',0)';
                                                    break;
                                                  default:
                                                    targetDataType = targetDataType + '(' + dataLength + ')';
                                                    break;
                                                }
                                                break;
                                              default:
                                           }
                                           
                                           switch (true) {
                                             default:
                                               argsList[idx] = '?';
                                           }
                                            
                                           return `${columnName} ${targetDataType} ${qualifier}\n `;
                                        })
                                       
    
    const createStatement = `create table if not exists "${schema}"."${metadata.tableName}"(\n  ${columnClauses.join(',')})`;
    const insertStatement = `insert into "${schema}"."${metadata.tableName}"(${metadata.columns}) values`

    // The extra comma on the end of the args list allows it to be easily replicated when performing batch inserts
    
    return { ddl : createStatement, dml : { sql : insertStatement, args : '(' + argsList.join(',') + '),'}}
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
    
    this.saxJParser.onvalue = function (v) {
      
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
      rowParser.keyStack.push(key);
      switch (rowParser.jDepth) {
        case 1:
          break;
        case 2:
          if (rowParser.dataPhase) {
            rowParser.push({ "table" : key});
          }
          // Fall through to default
        default:
          rowParser.objectStack.push(rowParser.currentObject);
      }
      rowParser.currentObject = {}
    };
    
    this.saxJParser.onkey = function (key) {
      switch (rowParser.jDepth){
        case 1:
          if (key === 'data') {
            rowParser.dataPhase = true;
          }
          break;
        case 2:
          if (rowParser.dataPhase) {
            rowParser.push({ "table" : key});
          }
          break;
        default:
      }
      rowParser.keyStack.push(key);
    };

    this.saxJParser.oncloseobject = async function () {
      rowParser.jDepth--;
      switch (rowParser.jDepth) {
        case 0:
          break;
        case 1:        
          const key = rowParser.keyStack.pop();
          switch (key) {
            case 'systemInformation' :
              rowParser.push({ "systemInformation" : rowParser.currentObject})
              break;
            case 'metadata' :
              rowParser.push({ "metadata" : rowParser.currentObject});
              break;
           default:
             rowParser.objectStack.length = 0;
             rowParser.keyStack.length = 0;   
          }
          break;
        default:
          const parentObject = rowParser.objectStack.pop()     
          if (Array.isArray(parentObject)) {   
            parentObject.push(rowParser.currentObject);
          }    
          else {
            const parentKey = rowParser.keyStack.pop();
            parentObject[parentKey] = rowParser.currentObject;
         }
         rowParser.currentObject = parentObject;
      }   
    }
   
    this.saxJParser.onopenarray = function () {
      rowParser.jDepth++;
      rowParser.objectStack.push(rowParser.currentObject);
      rowParser.currentObject = [];
    };

    this.saxJParser.onclosearray = function () {
      rowParser.jDepth--;
      switch (rowParser.jDepth) {
        case 0:
          break;
         case 1:
          break;
         case 2:
          break;
        case 3:
          rowParser.push({ "data" : rowParser.currentObject});
          break;
        default:
          const parentObject = rowParser.objectStack.pop()     
          if (Array.isArray(parentObject)) {    
            parentObject.push(rowParser.currentObject);
          }     
          else {
            const parentKey = rowParser.keyStack.pop();
            parentObject[parentKey] = rowParser.currentObject;
          }
          rowParser.currentObject = parentObject;
      }   
    };  
  }
   
  _transform(data,enc,callback) {
    this.saxJParser.write(data);
    callback();
  };
}

async function createTables(conn, schema, metadata) {
    
  const ddlStatements = []
  const dmlStatements = {};
  const tables = Object.keys(metadata); 
  tables.forEach(function(table,idx) {
                   const tableMetadata = metadata[table];
                   const sql = generateStatements(schema,tableMetadata);
                   ddlStatements[idx] = sql.ddl;
                   dmlStatements[table] = sql.dml;
  });
                    
  for (let i=0; i<ddlStatements.length;i++) {
    try {
      results = await conn.query(ddlStatements[i]);   
    } catch (e) {
      console.log(e);
    }
  }
  return dmlStatements;
}

class DbWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,options) {
    super({objectMode: true });
    const dbWriter = this;
    
    this.systemInformation;
    this.metadata;
    this.statementCache;
    
    this.tableName;
    this.insertStatement;
    this.args
    this.rowCount;
    this.startTime;
    
    this.schema = schema;
    this.conn = conn;
    
    this.batchCount = 0;
    this.batch = [];
    
    this.batchSize = batchSize;
    this.commitSize = commitSize;
  }      

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          this.statementCache = await createTables(this.conn, this.schema, this.metadata);
          break;
        case 'table':
          // console.log(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          const elapsedTime = new Date().getTime() - this.startTime;
          if (this.tableName !== undefined) {
            if (this.batchCount > 0) {
              // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batchCount} rows.`);
              // Slice removes the unwanted last comma from the replicated args list.
              const args = this.args.repeat(this.batchCount).slice(0,-1); 
              const results = await this.conn.query(this.insertStatement+args,this.batch);
              await this.conn.commit();
            }  
            console.log(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          }
          this.tableName = obj.table;
          this.insertStatement = this.statementCache[this.tableName].sql
          this.args = this.statementCache[this.tableName].args
          this.rowCount = 0;
          this.batchCount = 0;
          this.batch.length = 0;
          await this.conn.beginTransaction();
          this.startTime = new Date().getTime();
          break;
        case 'data': 
          this.batch.push(...obj.data);
          this.batchCount++;
          // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batchCount} rows.`);
          if (this.batchCount  === this.batchSize) {
              // Slice removes the unwanted last comma from the replicated args list.
              const args = this.args.repeat(this.batchCount).slice(0,-1); 
              // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batchCount} rows.`);
              const results = await this.conn.query(this.insertStatement+args,this.batch);
              this.batchCount = 0;
              this.batch.length = 0;
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             await this.conn.commit();
             const elapsedTime = new Date().getTime() - this.startTime;
             // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
             await this.conn.beginTransaction();
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      console.log(e);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      const elapsedTime = new Date().getTime() - this.startTime;
      if (this.batchCount > 0) {
        // console.log(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batchCount} rows.`);
        // Slice removes the unwanted last comma from the replicated args list.
        const args = this.args.repeat(this.batchCount).slice(0,-1); 
        const results = await this.conn.query(this.insertStatement+args,this.batch);
        await this.conn.commit();
      }   
      console.log(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
      callback();
    } catch (e) {
      console.log(e);
      callback(e);
    } 
  } 
}

function processFile(conn, schema, dumpFilePath,batchSize,commitSize) {
  
  return new Promise(function (resolve,reject) {
    const dbWriter = new DbWriter(conn,schema,batchSize,commitSize);
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
	
    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
           ,port      : parameters.PORT ? parameters.PORT : 3306
           ,database  : parameters.DATABASE
           ,multipleStatements: true
    }
    
    pool = mariadb.createPool(connectionDetails);
    conn = await pool.getConnection();
    results = await conn.query(`SET SESSION SQL_MODE=ANSI_QUOTES`);
    
    const dumpFilePath = parameters.FILE;	
	const stats = fs.statSync(dumpFilePath)
    const fileSizeInBytes = stats.size
	
    const logDML         = (parameters.LOGLEVEL && (parameters.loglevel > 0));
    const logDDL         = (parameters.LOGLEVEL && (parameters.loglevel > 1));
    const logDDLIssues   = (parameters.LOGLEVEL && (parameters.loglevel > 2));
    const logTrace       = (parameters.LOGLEVEL && (parameters.loglevel > 3));
    	
    const schema = parameters.TOUSER;
    const commitSize = parameters.COMMITSIZE;
    const batchSize = parameters.BATCHSIZE;

    results = await conn.query(`CREATE DATABASE IF NOT EXISTS "${schema}"`);	

    await processFile(conn, schema, dumpFilePath, batchSize, commitSize);
    
    await conn.end();
    console.log("Closing Pool");
    await pool.end();

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
	  await conn.end();
	}
    if (pool !== undefined) {
	  await pool.end();
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


 