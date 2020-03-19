"use strict"

const { performance } = require('perf_hooks');

const sql = require('mssql');
// const WKX = require('wkx');

const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {
    
  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
	
	this.transformations = this.tableInfo.dataTypes.map(function(dataType) {
	  switch (dataType.type) {
        case "image" :
		  return function(col,idx) {
     	     // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
             return Buffer.from(col,'hex');
		  }
          break;
        case "varbinary":
		  return function(col,idx) {
             return Buffer.from(col,'hex');
 		  }
          break;
       case "geography":
       case "geometry":
		 switch (this.tableInfo.spatialFormat) {
		   case "WKB":
           case "EWKB":
             return function(col,idx) {
		       // Upload geography & Geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer.
	           return Buffer.from(col,'hex');
			 }
			 break;
		   default:
             return null;	 
		  }
          break;
        case "json":
		  return function(col,idx) {
             if (typeof col === 'object') {
               return JSON.stringify(col);
             }
			 return col
		  }
          break;
        case "datetime":
		  return function(col,idx) {
            if (typeof col === 'string') {
              col = col.endsWith('Z') ? col : (col.endsWith('+00:00') ? `${col.slice(0,-6)}Z` : `${col}Z`)
            }
            else {
              // Alternative is to rebuild the table with these data types mapped to date objects ....
              col = col.toISOString();
            }
            if (col.length > 23) {
               col = `${col.substr(0,23)}Z`;
            }
			return col;
		  }
          break;
		case "time":
        case "date":
        case "datetime2":
        case "datetimeoffset":
		  return function(col,idx) {
            if (typeof col === 'string') {
              col = col.endsWith('Z') ? col : (col.endsWith('+00:00') ? `${col.slice(0,-6)}Z` : `${col}Z`)
            }
            else {
              // Alternative is to rebuild the table with these data types mapped to date objects ....
              col = col.toISOString();
            }
			return col;
		  }
          break;
        case 'bit':
		  return function(col,idx) {
            if (typeof col === 'string') {
              switch (col.toLowerCase()) {
                case '00':
                  return false;
                  break;
                case '01':
                  return true;
                  break;
                case 'false':
                  return true;
                  break;
                case 'true':
                  return true;
                  break;
              }
            }
			return col
		  }
          break;
        default :
		  return null
      }
    },this)

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
    if (this.dbi.preparedStatement !== undefined){
      await this.dbi.preparedStatement.unprepare();
	  this.dbi.preparedStatement = undefined;
    }
    return results;
  }

  async createPreparedStatement(insertStatement, dataTypes) {
    const ps = await this.dbi.getPreparedStatement();
    dataTypes.forEach(function (dataType,idx) {
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
  	      // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
          // sql.VarBinary ([length])
           ps.input(column,sql.VarBinary(dataType.length));
          break;
        case 'image':
          // ps.input(column,sql.Image);
          ps.input(column,sql.VarBinary(sql.MAX));
          break;
        case 'udt':
          ps.input(column,sql.UDT);
          break;
        case 'geography':
          // ps.input(column,sql.Geography)
		  // Upload Geography as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer.
		  switch (this.tableInfo.spatialFormat) {
			case "WKB":
            case "EWKB":
              ps.input(column,sql.VarBinary(sql.MAX));
			  break;
			default:
		      ps.input(column,sql.VarChar(sql.MAX));
		  }
          break;
        case 'geometry':
          // ps.input(column,sql.Geometry);
		  // Upload Geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer.
		  switch (this.tableInfo.spatialFormat) {
			case "WKB":
            case "EWKB":
              ps.input(column,sql.VarBinary(sql.MAX));
			  break;
			default:
		      ps.input(column,sql.VarChar(sql.MAX));
		  }
          break;
        case 'hierarchyid':
          ps.input(column,sql.VarChar(4000));
          break;
        default:
         this.yadamuLogger.info([`${this.constructor.name}.createPreparedStatement()`],`Unmapped data type [${dataType.type}].`);
      }
    },this)
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${insertStatement};\n--\n`);
    }
    await ps.prepare(insertStatement);
    return ps;
  }

  async appendRow(row) {
      
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	  
	this.transformations.forEach(function (transformation,idx) {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	},this)
    this.tableInfo.bulkOperation.rows.add(...row);
  }

  async writeBatch() {

    this.batchCount++;
      
    // ### Savepoint Support ?

    if (this.tableInfo.bulkSupported) {
      try {        
        await this.dbi.createSavePoint();
        const results = await this.dbi.bulkInsert(this.tableInfo.bulkOperation);
        this.endTime = performance.now();
        this.tableInfo.bulkOperation.rows.length = 0;
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.tableInfo.bulkOperation.rows.length}].  Bulk Operation raised:\n${e.message}.`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          this.yadamuLogger.writeDirect(`{${JSON.stringify(this.tableInfo.bulkOperation.columns)}\n`);
          this.yadamuLogger.writeDirect(`${JSON.stringify(this.tableInfo.bulkOperation.rows[0])}\n...\n${JSON.stringify(this.tableInfo.bulkOperation.rows[this.tableInfo.bulkOperation.rows.length-1])}\n`)
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
        }
        await this.dbi.restoreSavePoint();
        this.tableInfo.bulkSupported = false;
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.

    this.dbi.preparedStatement = await this.createPreparedStatement(this.tableInfo.dml, this.tableInfo.dataTypes) 

    for (const row in this.tableInfo.bulkOperation.rows) {
      try {
        const args = {}
        for (const col in this.tableInfo.bulkOperation.rows[0]){
           args['C'+col] = this.tableInfo.bulkOperation.rows[row][col]
        }
		this.dbi.currentStatement = this.dbi.preparedStatement;
        const results = await this.dbi.execute(this.dbi.preparedStatement,args,this.tableInfo.dml);
      } catch (e) {
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,JSON.stringify(this.tableInfo.bulkOperation.rows[row])] : []
        this.skipTable = await this.dbi.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.tableInfo.bulkOperation.rows.length,row,this.tableInfo.bulkOperation.rows[row],e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }       
       
    await this.dbi.preparedStatement.unprepare();   
	this.dbi.preparedStatement = undefined;
    this.endTime = performance.now();
    this.tableInfo.bulkOperation.rows.length = 0;
    return this.skipTable
  }
}

module.exports = TableWriter;