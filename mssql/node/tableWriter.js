"use strict"

const sql = require('mssql');

const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {
    
  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
  }

  batchComplete() {
     return (this.tableInfo.bulkOperation.rows.length  === this.tableInfo.batchSize)
  }
  
  batchRowCount() {
    return this.tableInfo.bulkOperation.rows.length
  }
  
  hasPendingRows() {
    return this.tableInfo.bulkOperation.rows.length > 0;
  }
      
  async finalize() {
    const results = await super.finalize()
    results.insertMode = this.tableInfo.bulkSupported === true ? 'Bulk' : 'Iterative'
    if (this.tableInfo.preparedStatement !== undefined){
      await this.tableInfo.preparedStatement.unprepare();
    }
    return results;
  }


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
          ps.input(column,sql.NVarChar(sql.MAX));
        case 'xml':
          // ps.input(column,sql.Xml);
          ps.input(column,sql.NVarChar(sql.MAX));
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
         this.yadamuLogger.log([`${this.constructor.name}.createPreparedStatement()`],`Unmapped data type [${targetDataType}].`);
      }
    },this)
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${insertStatement};\n--\n`);
    }
    await ps.prepare(insertStatement);
    return ps;
  }

  async appendRow(row) {
      
    this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
       const dataType = this.dbi.decomposeDataType(targetDataType);
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
               row[idx] = row[idx].endsWith('Z') ? row[idx] : (row[idx].endsWith('+00:00') ? `${row[idx].slice(0,-6)}Z` : `${row[idx]}Z`)
             }
             else {
               // Alternative is to rebuild the table with these data types mapped to date objects ....
               row[idx] = row[idx].toISOString();
             }
             switch (dataType.type) {
               case 'datetime':
                 if (row[idx].length > 23) {
                   row[idx] = `${row[idx].substr(0,23)}Z`;
                 }
             }
             break;
           case 'bit':
             if (typeof row[idx] === 'string') {
               switch (row[idx].toLowerCase()) {
                 case '00':
                   row[idx] = false;
                   break;
                 case '01':
                   row[idx] = true;
                   break;
                 case 'false':
                   row[idx] = true;
                   break;
                 case 'true':
                   row[idx] = true;
                   break;
               }
             }
             break;
           default :
         }
       }
    },this)
    this.tableInfo.bulkOperation.rows.add(...row);
  }

  async writeBatch() {
    
    this.batchCount++;

    if (this.tableInfo.bulkSupported) {
      try {
        
        const results = await this.dbi.getRequest().bulk(this.tableInfo.bulkOperation);
        this.endTime = new Date().getTime();
        this.tableInfo.bulkOperation.rows.length = 0;
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.tableInfo.bulkOperation.rows.length}].  Bulk Operation raised:\n${e.message}.`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          this.yadamuLogger.writeDirect(`{${JSON.stringify(this.tableInfo.bulkOperation.columns)}`);
          this.yadamuLogger.writeDirect(`${JSON.stringify(this.tableInfo.bulkOperation.rows[0])}\n...\n${JSON.stringify(this.tableInfo.bulkOperation.rows[this.batch.length-1])}\n`)
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
        }
        // await this.dbi.rollbackTransaction();
        this.tableInfo.bulkSupported = false;
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.

    if (this.tableInfo.preparedStatement === undefined) {
      this.tableInfo.preparedStatement = await this.createPreparedStatement(this.tableInfo.dml, this.tableInfo.targetDataTypes) 
    }

    for (const row in this.tableInfo.bulkOperation.rows) {
      try {
        const args = {}
        for (const col in this.tableInfo.bulkOperation.rows[0]){
           args['C'+col] = this.tableInfo.bulkOperation.rows[row][col]
        }
        const results = await this.tableInfo.preparedStatement.execute(args);
      } catch (e) {
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,JSON.stringify(this.tableInfo.bulkOperation.columns),] : []
        const abort = this.dbi.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.tableInfo.bulkOperation.rows.length,row,this.tableInfo.bulkOperation.rows[row],e,errInfo);
        if (abort) {
          await this.dbi.rollbackTransaction();
          this.skipTable = true;
          break;
        }
      }
    }       
    
    this.endTime = new Date().getTime();
    this.tableInfo.bulkOperation.rows.length = 0;
    return this.skipTable
  }
}

module.exports = TableWriter;