"use strict"
const { Transform } = require('stream');
const { Writable } = require('stream');
const sql = require('mssql');
const common = require('./common.js');
const clarinet = require('c:/Development/github/clarinet/clarinet.js');
// const clarinet = require('clarinet');
const fs = require('fs');

class RowParser extends Transform {
  
  constructor(logWriter, options) {

    super({objectMode: true });  
  
    const rowParser = this;
    
    this.logWriter = logWriter;

    this.saxJParser = clarinet.createStream();
    this.saxJParser.on('error',function(err) {this.logWriter.write(`$(err}\n`);})
    
    this.objectStack = [];
    this.emptyObject = true;
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
      this.emptyObject = (key === undefined);
      
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
          if (rowParser.dataPhase) {
            rowParser.push({ table : key});
          }
          break;
        default:
      }
      rowParser.objectStack.push({});
      if (key !== undefined) {
        rowParser.currentObject = key;
        rowParser.jDepth++;
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
        // rowParser.logWriter.write(`onvalue(${rowParser.chunks.length},${v.length})\n`);
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
      // rowParser.logWriter.write(`onvalue(${rowParser.jDepth}: ObjectStack:${rowParser.objectStack}. CurrentObject:${rowParser.currentObject}\n`);        
    }
      
    this.saxJParser.oncloseobject = async function () {
      // rowParser.logWriter.write(`onCloseObject(${rowParser.jDepth}):\nObjectStack:${rowParser.objectStack})\nCurrentObject:${rowParser.currentObject}\n`);      
      
      if ((rowParser.dataPhase) && (rowParser.jDepth === 5)) {
        // Serialize any embedded objects found inside the array that represents a row of data.
        rowParser.currentObject = JSON.stringify(rowParser.currentObject);
      }
      
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
        // Serialize any embedded objects found inside the array that represents a row of data.
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

function bulkSupported(targetDataTypes) {
    
   let supported = true;
   targetDataTypes.forEach(function (targetDataType,idx) {
     const dataType = decomposeDataType(targetDataType);
     switch (dataType.type) {
       /*
       case 'smallint':
         supported = false;
         break;
      */
      case 'geography':
         // TypeError: parameter.type.validate is not a function
         supported = false;
         break;
      case 'geometry':
         // TypeError: parameter.type.validate is not a function
        supported = false;
         break;
      case 'xml':
         // Unsupported Data Type for Bulk Load
         supported = false;
         break;
       case 'image':
         supported = false;
         break;
     }
   })
   return supported;
   
}
  
function createBulkOperation(database,schema,tableName, columnList, targetDataTypes) {

  const table = new sql.Table(database + '.' + schema + '.' + tableName);
  table.create = false
  
  const columns = JSON.parse('[' +  columnList + ']')

  targetDataTypes.forEach(function (targetDataType,idx) {
    const dataType = decomposeDataType(targetDataType);
    switch (dataType.type) {
      case 'bit':
        table.columns.add(columns[idx],sql.Bit, {nullable: true});
        break;
      case 'bigint':
        table.columns.add(columns[idx],sql.BigInt, {nullable: true});
        break;
      case 'float':
        table.columns.add(columns[idx],sql.Float, {nullable: true});
        break;
      case 'int':
        table.columns.add(columns[idx],sql.Int, {nullable: true});
        break;
      case 'money':
        table.columns.add(columns[idx],sql.Money, {nullable: true});
        break
      case 'decimal':
        // sql.Decimal ([precision], [scale])
        table.columns.add(columns[idx],sql.Decimal(dataType.length,dataType.scale), {nullable: true});
        break;
      case 'smallint':
        table.columns.add(columns[idx],sql.SmallInt, {nullable: true});
        break;
      case 'smallmoney':
        table.columns.add(columns[idx],sql.SmallMoney, {nullable: true});
        break;
      case 'real':
        table.columns.add(columns[idx],sql.Real, {nullable: true}, {nullable: true});
        break;
      case 'numeric':
        // sql.Numeric ([precision], [scale])
        table.columns.add(columns[idx],sql.Numeric(dataType.length,dataType.scale), {nullable: true});
        break;
      case 'tinyint':
        table.columns.add(columns[idx],sql.TinyInt, {nullable: true});
        break;
      case 'char':
        table.columns.add(columns[idx],sql.Char(dataType.length), {nullable: true});
        break;
      case 'nchar':
        table.columns.add(columns[idx],sql.NChar(dataType.length), {nullable: true});
        break;
      case 'text':
        table.columns.add(columns[idx],sql.Text, {nullable: true});
        break;
      case 'ntext':
        table.columns.add(columns[idx],sql.NText, {nullable: true});
        break;
      case 'varchar':
        table.columns.add(columns[idx],sql.VarChar(dataType.length), {nullable: true});
        break;
      case 'nvarchar':
        table.columns.add(columns[idx],sql.NVarChar(dataType.length), {nullable: true});
        break;
      case 'xml':
        // Added to Unsupported
        // Invalid column data type for bulk load
        table.columns.add(columns[idx],sql.Xml, {nullable: true});
        break;
      case 'time':
        // sql.Time ([scale])
        table.columns.add(columns[idx],sql.Time(dataType.length), {nullable: true});
        break;
      case 'date':
        table.columns.add(columns[idx],sql.Date, {nullable: true});
        break;
      case 'datetime':
        table.columns.add(columns[idx],sql.DateTime, {nullable: true});
        break;
      case 'datetime2':
        // sql.DateTime2 ([scale]
        table.columns.add(columns[idx],sql.DateTime2(), {nullable: true});
        break;
      case 'datetimeoffset':
        // sql.DateTimeOffset ([scale])
        table.columns.add(columns[idx],sql.DateTimeOffset(dataType.length), {nullable: true});
        break;
      case 'smalldatetime':
        table.columns.add(columns[idx],sql.SmallDateTime, {nullable: true});
        break;
      case 'uniqueidentifier':
        // table.columns.add(columns[idx],sql.UniqueIdentifier, {nullable: true});
        // TypeError: parameter.type.validate is not a function
        table.columns.add(columns[idx],sql.Char(36), {nullable: true});
        break;
      case 'variant':
        table.columns.add(columns[idx],sql.Variant, {nullable: true});
        break;
      case 'binary':
        table.columns.add(columns[idx],sql.Binary, {nullable: true});
        break;
      case 'varbinary':
        // sql.VarBinary ([length])
         table.columns.add(columns[idx],sql.VarBinary(dataType.length), {nullable: true});
        break;
      case 'image':
        // Added to Unsupported
        // Invalid column data type for bulk load
        table.columns.add(columns[idx],sql.Image, {nullable: true});
        break;
      case 'udt':
        table.columns.add(columns[idx],sql.UDT, {nullable: true});
        break;
      case 'geography':
        // Added to Unsupported
        // TypeError: parameter.type.validate is not a function
        table.columns.add(columns[idx],sql.Geography, {nullable: true});
        break;
      case 'geometry':
        // Added to Unsupported
        // TypeError: parameter.type.validate is not a function
        table.columns.add(columns[idx],sql.Geometry, {nullable: true});
        break;
      case 'hierarchyid':
        table.columns.add(columns[idx],sql.VarChar(892),{nullable: true});
        break;
      default:
        console.log(`createBulkOperation(): Unmapped data type [${targetDataType}].`);
    }
  })
  
  return table
}

async function createPreparedStatement(transaction, insertStatement, targetDataTypes) {

  const ps = await new sql.PreparedStatement(transaction);
  
  targetDataTypes.forEach(function (targetDataType,idx) {
    const dataType = decomposeDataType(targetDataType);
    const column = 'C' + idx;
    switch (dataType.type) {
      case 'bit':
        ps.input(column,sql.Bit);
        break;
      case 'bigint':
        ps.input(column,sql.BigInt);
        break;
      case 'float':
        ps.input(column,sql.Float);
        break;
      case 'int':
        ps.input(column,sql.Int);
        break;
      case 'money':
        ps.input(column,sql.Money);
        break
      case 'decimal':
        // sql.Decimal ([precision], [scale])
        ps.input(column,sql.Decimal(dataType.length,dataType.scale));
        break;
      case 'smallint':
        ps.input(column,sql.SmallInt);
        break;
      case 'smallmoney':
        ps.input(column,sql.SmallMoney);
        break;
      case 'real':
        ps.input(column,sql.Real);
        break;
      case 'numeric':
        // sql.Numeric ([precision], [scale])
        ps.input(column,sql.Numeric(dataType.length,dataType.scale));
        break;
      case 'tinyint':
        ps.input(column,sql.TinyInt);
        break;
      case 'char':
        ps.input(column,sql.Char(dataType.length));
        break;
      case 'nchar':
        ps.input(column,sql.NChar(dataType.length));
        break;
      case 'text':
        ps.input(column,sql.Text);
        break;
      case 'ntext':
        ps.input(column,sql.NText);
        break;
      case 'varchar':
        ps.input(column,sql.VarChar(dataType.length));
        break;
      case 'nvarchar':
        ps.input(column,sql.NVarChar(dataType.length));
        break;
      case 'xml':
        ps.input(column,sql.Xml);
        break;
      case 'time':
        // sql.Time ([scale])
        ps.input(column,sql.Time(dataType.length));
        break;
      case 'date':
        ps.input(column,sql.Date);
        break;
      case 'datetime':
        ps.input(column,sql.DateTime);
        break;
      case 'datetime2':
        // sql.DateTime2 ([scale]
        ps.input(column,sql.DateTime2());
        break;
      case 'datetimeoffset':
        // sql.DateTimeOffset ([scale])
        ps.input(column,sql.DateTimeOffset(dataType.length));
        break;
      case 'smalldatetime':
        ps.input(column,sql.SmallDateTime);
        break;
      case 'uniqueidentifier':
        // ps.input(column,sql.UniqueIdentifier);
        // TypeError: parameter.type.validate is not a function
        ps.input(column,sql.Char(36));
        break;
      case 'variant':
        ps.input(column,sql.Variant);
        break;
      case 'binary':
        ps.input(column,sql.Binary);
        break;
      case 'varbinary':
        // sql.VarBinary ([length])
         ps.input(column,sql.VarBinary(dataType.length));
        break;
      case 'image':
        ps.input(column,sql.Image);
        break;
      case 'udt':
        ps.input(column,sql.UDT);
        break;
      case 'geography':
        // ps.input(column,sql.Geography);
        ps.input(column,sql.NVarChar(sql.MAX));
        break;
      case 'geometry':
        // ps.input(column,sql.Geometry);
        ps.input(column,sql.NVarChar(sql.MAX));
        break;
      case 'hierarchyid':
        ps.input(column,sql.VarChar(892));
        break;
      default:
        console.log(`createPreparedStatement(): Unmapped data type [${targetDataType}].`);
    }
  })
  await ps.prepare(insertStatement);
  return ps;
}

async function generateStatementCache(conn, database, schema, metadata, status, logWriter) {
    
  const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS";`;					   
 
  let request = await new sql.Request(conn);
  let results = await request.input('TARGET_DATABASE',sql.VARCHAR,schema).input('METADATA',sql.NVARCHAR,JSON.stringify({metadata : metadata})).execute('GENERATE_SQL');
  results = results.output[Object.keys(results.output)[0]]
  const statementCache = JSON.parse(results)
  const tables = Object.keys(metadata); 
  await Promise.all(tables.map(async function(table,idx) {
                                       statementCache[table] = JSON.parse(statementCache[table] );
                                       const tableInfo = statementCache[table];
                                       tableInfo.targetDataTypes = JSON.parse('[' + tableInfo.targetDataTypes + ']');
                                       // Create table before attempting to Prepare Statement..
                                       if (status.sqlTrace) {
                                         status.sqlTrace.write(`${tableInfo.ddl};\n--\n`);
                                       }
                                       try {
                                         const results = await request.batch(statementCache[table].ddl)   
                                       } catch (e) {
                                         logWriter.write(`${e}\n${tableInfo.ddl}\n`)
                                       } 
                                       tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + "\nVALUES (";
                                       metadata[table].columns.split(',').forEach(function(column,idx) {
                                         tableInfo.dml = tableInfo.dml + '@C' + idx + ','
                                       })
                                       tableInfo.dml = tableInfo.dml.slice(0,-1) + ")";
                                       tableInfo.bulkSupported = bulkSupported(tableInfo.targetDataTypes);
                                       if (tableInfo.bulkSupported) {
                                         tableInfo.bulkOperation = createBulkOperation(database,schema,table,metadata[table].columns,tableInfo.targetDataTypes);
                                       }
                                       else {
                                         // Just so we have somewhere to cache the data
                                         tableInfo.bulkOperation = new sql.Table();
                                         tableInfo.preparedStatement = await createPreparedStatement(conn, tableInfo.dml, tableInfo.targetDataTypes) 
                                       }    
  }));
  return statementCache;
}

class DbWriter extends Writable {
  
  constructor(conn,database,schema,batchSize,commitSize,mode,status,logWriter,options) {
    super({objectMode: true });
    const dbWriter = this;
    
    this.conn = conn;
    this.transaction = undefined;
  
    this.database = database;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.mode = mode;
    this.status = status;
    this.logWriter = logWriter;

    this.metadata = undefined;
    this.statementCache = undefined;
    
    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;

  }      
  
  async setTable(tableName) {
       
    this.tableName = tableName
    this.tableInfo = this.statementCache[tableName]
    this.tableInfo.bulkOperation.rows.length = 0;
    this.rowCount = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
   async writeBatch(status) {
    if (this.tableInfo.bulkSupported) {
      try {
        const request = await new sql.Request(this.transaction)
        const results = await request.bulk(this.tableInfo.bulkOperation);
        const endTime = new Date().getTime();
        // console.log(`Bulk(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Success`);
        this.tableInfo.bulkOperation.rows.length = 0;
        return endTime
      } catch (e) {
        this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Bulk Operation failed. Reason: ${e.message}\n`)
        this.logWriter.write(`${new Date().toISOString()}: Switching to conventional insert.\n`)
        this.tableInfo.bulkSupported = false;
        this.tableInfo.preparedStatement = await createPreparedStatement(this.transaction, this.tableInfo.dml, this.tableInfo.targetDataTypes) 
        // console.log(this.tableInfo.bulkOperation.columns);
        if (this.logDDLIssues) {
          this.logWriter.write(`${e.stack}\n`);
          this.logWriter.write(`{${JSON.stringify(this.tableInfo.bulkOperation.columns)}`);
        }      
      }
    }
        
    try {
      // // await this.transaction.rollback();
      // await this.transaction.begin();
      for (const r in this.tableInfo.bulkOperation.rows) {
        const args = {}
        for (const c in this.tableInfo.bulkOperation.rows[0]){
          args['C'+c] = this.tableInfo.bulkOperation.rows[r][c]
        }
        const results = await this.tableInfo.preparedStatement.execute(args);
      }
      
      
      const endTime = new Date().getTime();
      // console.log(`Conventional(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Success`);
      this.tableInfo.bulkOperation.rows.length = 0;
      return endTime
    } catch (e) {
      // console.log(`Conventional(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Failed`);
      this.tableInfo.bulkOperation.rows.length = 0;
      this.skipTable = true;
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Reason: ${e.message}\n${e.stack}\n`)
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.bulkOperation.columns}\n`);
        this.logWriter.write(`${this.tableInfo.bulkOperation.rows}\n`);
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
          this.statementCache = await generateStatementCache(this.conn, this.database, this.schema, this.metadata, this.status, this.logWriter);
          this.transaction = this.conn;
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName) {
            if (this.tableInfo.bulkOperation.rows.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
            }  
            if (!this.skipTable) {
              // await this.transaction.commit();
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Method ${this.tableInfo.bulkSupported ? 'Bulk' : 'Conventional'}. Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
            if (!this.tableInfo.bulkSupported) {
              await this.tableInfo.preparedStatement.unprepare();
            }
          }
          this.setTable(obj.table);
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          if (this.rowCount === 0) {
            // await this.transaction.begin();
          }
          // Perform SQL Server specific data type conversions before pushing row to bulkOperation row cache
          this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
                                                   const dataType = decomposeDataType(targetDataType);
                                                   if (obj.data[idx] !== null) {
                                                     switch (dataType.type) {
                                                       case "image" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "varbinary" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "datetime2" :
                                                         obj.data[idx] = new Date(Date.parse(obj.data[idx]));
                                                         break;
                                                       case "datetime" :
                                                         obj.data[idx] = new Date(Date.parse(obj.data[idx]));
                                                         break;
                                                       case "datetimeoffset" :
                                                         obj.data[idx] = new Date(Date.parse(obj.data[idx]));
                                                         break;
                                                       case "geography" :
                                                         // Code to convert to WellKnown Goes Here ???
                                                         obj.data[idx] = null;
                                                         break;
                                                       case "geometry" :
                                                         // Code to convert to WellKnown Goes Here ???
                                                         obj.data[idx] = null;
                                                         break;
                                                       case "hierarchyid" :
                                                         // Need to solve this later ?
                                                         obj.data[idx] = null;
                                                         break;
                                                       default :
                                                     }
                                                   }
                                                 },this)
          this.tableInfo.bulkOperation.rows.add(...obj.data);
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
          if (this.tableInfo.bulkOperation.rows.length  === this.batchSize) {
              //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             // await this.transaction.commit();
             // await this.transaction.begin();       
             const elapsedTime = this.endTime - this.startTime;
             // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
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
          if (this.tableInfo.bulkOperation.rows.length > 0) {
            // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.tableInfo.bulkOperation.rows.length} rows.`);
            this.endTime = await this.writeBatch();
          }  
          // await this.transaction.commit();
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Method ${this.tableInfo.bulkSupported ? 'Bulk' : 'Conventional'}. Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          // this.transaction.commit();
        }
        if (!this.tableInfo.bulkSupported) {
          await this.tableInfo.preparedStatement.unprepare();
        }
      }          
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }
      this.conn.close();
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n`);
      callback(e);
    } 
  } 
}

function processFile(conn, database, schema, dumpFilePath, batchSize, commitSize, mode, status, logWriter) {
  
  return new Promise(function (resolve,reject) {
    const dbWriter = new DbWriter(conn,database,schema,batchSize,commitSize,mode,status,logWriter);
    const rowGenerator = new RowParser(logWriter);
    const readStream = fs.createReadStream(dumpFilePath);    
    dbWriter.on('finish', function() { resolve()});
    readStream.pipe(rowGenerator).pipe(dbWriter);
  })
}
    
async function main() {

  let pool;	
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
      setTimeout((function() { console.log('Forced Exit'); return process.exit(); }), 5000);
    })
    
    parameters = common.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

	if (parameters.SQLTRACE) {
	  status.sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }
	
    const connectionDetails = {
      server    : parameters.HOSTNAME
     ,user      : parameters.USERNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
	 ,options: {
        encrypt: false // Use this if you're on Windows Azure

      }
    }

    pool = await new sql.ConnectionPool(connectionDetails).connect()
    const request = pool.request();
	results = await request.query(`SET QUOTED_IDENTIFIER ON`);
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
	
    if (parameters.LOGLEVEL) {
       status.loglevel = parameters.LOGLEVEL;
    }
    	
    await processFile(pool, parameters.DATABASE, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    
    await pool.close();
    
    status.statusMsg = status.warningRaised ? 'with warnings' : status.statusMsg;
    status.statusMsg = status.errorRaised ? 'with errors'  : status.statusMsg;
     
    logWriter.write(`Import operation completed ${status.statusMsg}.\n`);
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
    if (pool !== undefined) {
	  await pool.close();
	}
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

  if (parameters.SQLTRACE) {
    status.sqlTrace.close();
  }
  
  // setTimeout((function() { console.log('Forced Exit'); return process.exit(); }), 5000);

}
    
main()


 