"use strict";

const sql = require('mssql');

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi, batchSize, commitSize, status, logWriter) {
    
    this.dbi = dbi;
    this.batchSize = batchSize
    this.commitSize = commitSize;
    this.status = status;
    this.logWriter = logWriter;
  }
  
  bulkSupported(targetDataTypes) {
    
    let supported = true;
    targetDataTypes.forEach(function (targetDataType,idx) {
      const dataType = this.dbi.decomposeDataType(targetDataType);
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
  
  createBulkOperation(database, tableName, columnList, targetDataTypes) {

    const table = new sql.Table(database + '.' + this.dbi.parameters.TOUSER + '.' + tableName);
    table.create = false
    
    const columns = JSON.parse('[' +  columnList + ']')
  
    targetDataTypes.forEach(function (targetDataType,idx) {
      const dataType = this.dbi.decomposeDataType(targetDataType);
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

  async generateStatementCache (metadata, executeDDL, database) {
    
    const sqlStatement = `SET @RESULTS = '{}'; CALL master.dbo.sp_GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS";`;	
    
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement};\n--\n`)
    }

    let results = await this.dbi.getRequest().input('TARGET_DATABASE',sql.VARCHAR,this.dbi.parameters.TOUSER).input('METADATA',sql.NVARCHAR,JSON.stringify({metadata : metadata})).execute('master.dbo.sp_GENERATE_SQL');
    results = results.output[Object.keys(results.output)[0]]
    const statementCache = JSON.parse(results)
    const tables = Object.keys(metadata); 
    const ddlStatements = tables.map(function(table,idx) {
      const tableName = metadata[table].tableName;
      statementCache[tableName] = JSON.parse(statementCache[tableName] );
      const tableInfo = statementCache[tableName];
      tableInfo.batchSize =  this.batchSize;
      tableInfo.batchSize = this.commitSize;
      // Create table before attempting to Prepare Statement..
      tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + "\nVALUES (";
      metadata[table].columns.split(',').forEach(function(column,idx) {
        tableInfo.dml = tableInfo.dml + '@C' + idx + ','
      })
      tableInfo.dml = tableInfo.dml.slice(0,-1) + ")";
      tableInfo.bulkSupported = this.bulkSupported(tableInfo.targetDataTypes);
      try {
        if (tableInfo.bulkSupported) {
          tableInfo.bulkOperation = this.createBulkOperation(database,table,metadata[table].columns,tableInfo.targetDataTypes);
        }
        else {
          // Place holder for caching rows.
          tableInfo.bulkOperation = new sql.Table()                                            
        }
        return tableInfo.ddl;
      } catch (e) {
        this.logWriter.write(`${new Date().toISOString()}:${e}\n${tableInfo.ddl}\n`)
      } 
    },this);
    
    if (executeDDL === true) {
      await this.dbi.executeDDL(ddlStatements);
    }
    return statementCache;
  }
}

module.exports = StatementGenerator
