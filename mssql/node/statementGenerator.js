"use strict";
const sql = require('mssql');

const Yadamu = require('../../common/yadamuCore.js');

class StatementGenerator {
  
  constructor(pool, status, logWriter) {
    
    // super();
    const statementGenerator = this;
    
    this.pool = pool;
    this.status = status;
    this.logWriter = logWriter;
    
  }
  
  decomposeDataType(targetDataType) {
    const dataType = Yadamu.decomposeDataType(targetDataType);
    if (dataType.length === -1) {
      dataType.length = sql.MAX;
    }
    return dataType;
  }
  
  bulkSupported(targetDataTypes) {
    
    let supported = true;
    targetDataTypes.forEach(function (targetDataType,idx) {
      const dataType = this.decomposeDataType(targetDataType);
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
     },this)
    return supported;
   
  }
  
  createBulkOperation(database,schema,tableName, columnList, targetDataTypes) {

    const table = new sql.Table(database + '.' + schema + '.' + tableName);
    table.create = false
    
    const columns = JSON.parse('[' +  columnList + ']')
  
    targetDataTypes.forEach(function (targetDataType,idx) {
      const dataType = this.decomposeDataType(targetDataType);
      switch (dataType.type) {
        case 'bit':
          table.columns.add(columns[idx],sql.Bit);
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
          table.columns.add(columns[idx],sql.Decimal(19,4), {nullable: true});
          // table.columns.add(columns[idx],sql.Money, {nullable: true});
          break
        case 'decimal':
          // sql.Decimal ([precision], [scale])
          table.columns.add(columns[idx],sql.Decimal(dataType.length,dataType.scale), {nullable: true});
          break;
        case 'smallint':
          table.columns.add(columns[idx],sql.SmallInt, {nullable: true});
          break;
        case 'smallmoney':
          table.columns.add(columns[idx],sql.Decimal(10,4), {nullable: true});
          // table.columns.add(columns[idx],sql.SmallMoney, {nullable: true});
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
        case 'json':
          table.columns.add(columns[idx],sql.NVarChar(sql.MAX), {nullable: true});
          break;
        case 'xml':
          // Added to Unsupported
          // Invalid column data type for bulk load
          table.columns.add(columns[idx],sql.Xml, {nullable: true});
          break;
        case 'time':
          // sql.Time ([scale])
          // Binding as sql.Time must supply values as type Date. 
          // table.columns.add(columns[idx],sql.Time(dataType.length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'date':
          // Binding as sql.Date must supply values as type Date. 
          // table.columns.add(columns[idx],sql.Date, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime':
          // Binding as sql.DateTime must supply values as type Date. 
          // table.columns.add(columns[idx],sql.DateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime2':
          // sql.DateTime2 ([scale]
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columns[idx],sql.DateTime2(), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetimeoffset':
          // sql.DateTimeOffset ([scale])
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columns[idx],sql.DateTimeOffset(dataType.length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'smalldatetime':
          // Binding as sql.SamllDateTime must supply values as type Date. 
          // table.columns.add(columns[idx],sql.SmallDateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
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
          table.columns.add(columns[idx],sql.VarChar(4000),{nullable: true});
          break;
        default:
          console.log(`createBulkOperation(): Unmapped data type [${targetDataType}].`);
      }
    },this)
    return table
  }

  async createPreparedStatement(pool, insertStatement, targetDataTypes) {
      
    const ps = await new sql.PreparedStatement(pool);
    targetDataTypes.forEach(function (targetDataType,idx) {
      const dataType = this.decomposeDataType(targetDataType);
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
          // ps.input(column,sql.Money);
          ps.input(column,sql.Decimal(19,4));
          break
        case 'decimal':
          // sql.Decimal ([precision], [scale])
          ps.input(column,sql.Decimal(dataType.length,dataType.scale));
          break;
        case 'smallint':
          ps.input(column,sql.SmallInt);
          break;
        case 'smallmoney':
          // ps.input(column,sql.SmallMoney);
          ps.input(column,sql.Decimal(10,4));
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
        case 'json':
          ps.input(column,NVarChar(sql.MAX));
        case 'xml':
          ps.input(column,sql.Xml);
          break;
        case 'time':
          // sql.Time ([scale])
          // ps.input(column,sql.Time(dataType.length));
          ps.input(column,sql.VarChar(32));
          break;
        case 'date':
          // ps.input(column,sql.Date);
          ps.input(column,sql.VarChar(32));
          break;
        case 'datetime':
          // ps.input(column,sql.DateTime);
          ps.input(column,sql.VarChar(32));
          break;
        case 'datetime2':
          // sql.DateTime2 ([scale]
          // ps.input(column,sql.DateTime2());
          ps.input(column,sql.VarChar(32));
          break;
        case 'datetimeoffset':
          // sql.DateTimeOffset ([scale])
          // ps.input(column,sql.DateTimeOffset(dataType.length));
          ps.input(column,sql.VarChar(32));
          break;
        case 'smalldatetime':
          // ps.input(column,sql.SmallDateTime);
          ps.input(column,sql.VarChar(32));
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
          ps.input(column,sql.VarChar(4000));
          break;
        default:
         this.logWriter.write(`${new Date().toISOString()}: statementGenerator.createPreparedStatement(): Unmapped data type [${targetDataType}].`);
      }
    },this)
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${insertStatement};\n--\n`);
    }
    await ps.prepare(insertStatement);
    return ps;
  }

  async generateStatementCache (database, schema, systemInformation, metadata) {
    

    const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS";`;					   
    let results = await this.pool.request().input('TARGET_DATABASE',sql.VARCHAR,schema).input('METADATA',sql.NVARCHAR,JSON.stringify({systemInformation: systemInformation, metadata : metadata})).execute('GENERATE_SQL');
    results = results.output[Object.keys(results.output)[0]]
    const statementCache = JSON.parse(results)
    const tables = Object.keys(metadata); 
    await Promise.all(tables.map(async function(table,idx) {
                                         statementCache[table] = JSON.parse(statementCache[table] );
                                         const tableInfo = statementCache[table];
                                         // Create table before attempting to Prepare Statement..
                                         if (this.status.sqlTrace) {
                                           this.status.sqlTrace.write(`${tableInfo.ddl};\n--\n`);
                                         }
                                         try {
                                           const results = await this.pool.request().batch(statementCache[table].ddl)   
                                         } catch (e) {
                                           this.logWriter.write(`${e}\n${tableInfo.ddl}\n`)
                                         } 
                                         tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + "\nVALUES (";
                                         metadata[table].columns.split(',').forEach(function(column,idx) {
                                           tableInfo.dml = tableInfo.dml + '@C' + idx + ','
                                         })
                                         tableInfo.dml = tableInfo.dml.slice(0,-1) + ")";
                                         tableInfo.bulkSupported = this.bulkSupported(tableInfo.targetDataTypes);
                                         try {
                                           if (tableInfo.bulkSupported) {
                                             tableInfo.bulkOperation = this.createBulkOperation(database,schema,table,metadata[table].columns,tableInfo.targetDataTypes);
                                           }
                                           else {
                                             // Just so we have somewhere to cache the data
                                             tableInfo.bulkOperation = new sql.Table();
                                             tableInfo.preparedStatement = await this.createPreparedStatement(this.pool, tableInfo.dml, tableInfo.targetDataTypes) 
                                           }
                                         } catch (e) {
                                           this.logWriter.write(`${new Date().toISOString()}:${e}\n${tableInfo.ddl}\n`)
                                         } 
    },this));
    return statementCache;
  }

}

module.exports = StatementGenerator;