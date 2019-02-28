"use strict"

const sql = require('mssql');

const Yadamu = require('../../common/yadamu.js');

class TableWriter {
    
  async createPreparedStatement(insertStatement, targetDataTypes) {
    const ps = await this.dbi.getPreparedStatement();
    targetDataTypes.forEach(function (targetDataType,idx) {
      const dataType = this.dbi.decomposeDataType(targetDataType);
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
         this.logWriter.write(`${new Date().toISOString()}[tableWriter.createPreparedStatement()]: Unmapped data type [${targetDataType}].`);
      }
    },this)
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${insertStatement};\n--\n`);
    }
    await ps.prepare(insertStatement);
    return ps;
  }

  constructor(dbi,schema,tableName,tableInfo,status,logWriter) {
    this.dbi = dbi;
    this.schema = schema;
    this.tableName = tableName
    this.tableInfo = tableInfo;
    this.status = status;
    this.logWriter = logWriter;    

    
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.insertMode = 'Batch';

    this.skipTable = false;

    this.logDDLIssues   = (this.status.loglevel && (this.status.loglevel > 2));
    // this.logDDLIssues   = true
  }

  async initialize() {
  }

  batchComplete() {
     return (this.tableInfo.bulkOperation.rows.length  === this.batchSize)
  }
  
  commitWork(rowCount) {
    return (rowCount % this.tableInfo.commitSize) === 0;
  }

  async appendRow(row) {
      
    this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
       const dataType = Yadamu.decomposeDataType(targetDataType);
       if (row[idx] !== null) {
         switch (dataType.type) {
           case "image" :
             row[idx] = Buffer.from(row[idx],'hex');
             break;
           case "varbinary":
             row[idx] = Buffer.from(row[idx],'hex');
             break;
           case "json":
             if (typeof row[idx] === 'object') {
               row[idx] = JSON.stringify(row[idx]);
             }
             break;
           case "time":
           case "date":
           case "datetime":
           case "datetime2":
           case "datetimeoffset":
             if (typeof row[idx] === 'string') {
               row[idx] = row[idx].endsWith('Z') ? row[idx] : `${row[idx]}Z`
             }
             else {
               // Alternative is to rebuild the table with these data types mapped to date objects ....
               row[idx] = row[idx].toISOString();
             }
             break;
           default :
         }
       }
    },this)
    this.tableInfo.bulkOperation.rows.add(...row);
  }

  hasPendingRows() {
    return this.tableInfo.bulkOperation.rows.length > 0;
  }
      
  async writeBatch() {
    if (this.tableInfo.bulkSupported) {
      try {
        
        const results = await this.dbi.getRequest().bulk(this.tableInfo.bulkOperation);
        this.endTime = new Date().getTime();
        // console.log(`Bulk(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Success`);
        this.tableInfo.bulkOperation.rows.length = 0;
        return this.skipTable
      } catch (e) {
        if (this.logDDLIssues) {
          this.logWriter.write(`${new Date().toISOString()}[TableWriter "${this.tableName}"]: Bulk Operation failed. Reason: ${e.message}\n`)
        }
        this.tableInfo.bulkSupported = false;
        // console.log(this.tableInfo.bulkOperation.columns);
        if (this.logDDLIssues) {
          this.logWriter.write(`${new Date().toISOString()}[TableWriter "${this.tableName}"]: ${e.stack}\n`);
          this.logWriter.write(`{${JSON.stringify(this.tableInfo.bulkOperation.columns)}`);
        }      
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.

    if (this.tableInfo.preparedStatement === undefined) {
      this.tableInfo.preparedStatement = await this.createPreparedStatement(this.tableInfo.dml, this.tableInfo.targetDataTypes) 
    }

    try {
      for (const r in this.tableInfo.bulkOperation.rows) {
        const args = {}
        for (const c in this.tableInfo.bulkOperation.rows[0]){
          args['C'+c] = this.tableInfo.bulkOperation.rows[r][c]
        }
        const results = await this.tableInfo.preparedStatement.execute(args);
      }
            
      this.endTime = new Date().getTime();
      // console.log(`Conventional(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Success`);
      this.tableInfo.bulkOperation.rows.length = 0;
      return this.skipTable
    } catch (e) {
      // console.log(`Conventional(${this.tableName}). Batch size ${this.tableInfo.bulkOperation.rows.length}. Failed`);
      this.tableInfo.bulkOperation.rows.length = 0;
      this.skipTable = true;
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}[TableWriter "${this.tableName}"]: Skipping table. Reason: ${e.message}\n${e.stack}\n`)
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.bulkOperation.columns}\n`);
        this.logWriter.write(`${this.tableInfo.bulkOperation.rows}\n`);
      }      
    }
    return this.skipTable
  }

  async finalize() {
    if (this.hasPendingRows()) {
      this.skipTable = await this.writeBatch();   
    }
    await this.dbi.commitTransaction();
    if (!this.tableInfo.bulkSupported) {
      await this.tableInfo.preparedStatement.unprepare();
    }
    return {
      startTime    : this.startTime
    , endTime      : this.endTime
    , insertMode   : this.tableInfo.bulkSupported === true ? 'Bulk' : 'Iterative'
    , skipTable    : this.skipTable
    }    
  }

}

module.exports = TableWriter;