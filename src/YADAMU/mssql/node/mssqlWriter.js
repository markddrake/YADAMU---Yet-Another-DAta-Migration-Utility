"use strict"

const sql = require('mssql');

const { performance } = require('perf_hooks');
const YadamuWriter = require('../../common/yadamuWriter.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const NullWriter = require('../../common/nullWriter.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');
const {DatabaseError,RejectedColumnValue} = require('../../common/yadamuException.js');

class MsSQLWriter extends YadamuWriter {
    
  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  setTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
	const transformations = this.dataTypes.map((dataType,idx) => {      
	  switch (dataType.type.toLowerCase()) {
        case "json":
		  return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
		  }
          break;
		case 'bit':
        case 'boolean':
		  return (col,idx) => {
            return YadamuLibrary.toBoolean(col)
		  }
          break;
        case "datetime":
		  return (col,idx) => {
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
		  return (col,idx) => {
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
 		case "real":
        case "float":
		case "double":
		case "double precision":
		case "binary_float":
		case "binary_double":
		  switch (this.dbi.INFINITY_MANAGEMENT) {
		    case 'REJECT':
              return (col, idx) => {
			    if (!isFinite(col)) {
			      throw new RejectedColumnValue(this.tableInfo.columnNames[idx],col);
			    }
				return col;
		      }
		    case 'NULLIFY':
			  return (col, idx) => {
			    if (!isFinite(col)) {
                  this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],`Column "${this.tableInfo.columnNames[idx]}" contains unsupported value "${col}". Column nullified.`);
	  		      return null;
				}
			    return col
		      }   
			default:
			  return null;
	      }
		default :
		  return null
      }
    })
	
    // Use a dummy rowTransformation function if there are no transformations required.

	return transformations.every((currentValue) => { currentValue === null}) 
	? (row) => {} 
	: (row) => {
      transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
      }) 
    }
  }	  

  createBulkOperation(database, tableName, columnList, dataTypes) {

    const table = new sql.Table(database + '.' + this.dbi.parameters.TO_USER + '.' + tableName);
    table.create = false
    let precision
    dataTypes.forEach((dataType,idx) => {
      const length = dataType.length > 0 && dataType.length < 65535 ? dataType.length : sql.MAX
      switch (dataType.type.toLowerCase()) {
        case 'bit':
          table.columns.add(columnList[idx],sql.Bit);
          break;
        case 'bigint':
		  // Bind as VarChar to avoid rounding issues
          // table.columns.add(columnList[idx],sql.BigInt, {nullable: true});
		  table.columns.add(columnList[idx],sql.VarChar(21), {nullable: true});  
          break;
        case 'float':
          table.columns.add(columnList[idx],sql.Float, {nullable: true});
          break;
        case 'int':
          table.columns.add(columnList[idx],sql.Int, {nullable: true});
          break;
        case 'money':
          // table.columns.add(columnList[idx],sql.Money, {nullable: true});
          table.columns.add(columnList[idx],sql.Decimal(19,4), {nullable: true});
          break
        case 'decimal':
		  precision = dataType.length || 18
		  if (precision > 15) {
			// Bind as VarChar to avoid rounding issues
			table.columns.add(columnList[idx],sql.VarChar(precision+2), {nullable: true});  
		  }
		  else {
            // sql.Decimal ([precision], [scale])
            table.columns.add(columnList[idx],sql.Decimal(dataType.length || 18,dataType.scale || 0), {nullable: true});
		  }
          break;
        case 'numeric':
		  precision = dataType.length || 18
		  if (precision > 15) {
			// Bind as VarChar to avoid rounding issues
			table.columns.add(columnList[idx],sql.VarChar(precision+2), {nullable: true});  
		  }
		  else {
            // sql.Numeric ([precision], [scale])
            table.columns.add(columnList[idx],sql.Numeric(dataType.length || 18,dataType.scale || 0), {nullable: true});
		  }
          break;
        case 'smallint':
          table.columns.add(columnList[idx],sql.SmallInt, {nullable: true});
          break;
        case 'smallmoney':
          // table.columns.add(columnList[idx],sql.SmallMoney, {nullable: true});
          table.columns.add(columnList[idx],sql.Decimal(10,4), {nullable: true});
          break;
        case 'real':
          table.columns.add(columnList[idx],sql.Real, {nullable: true}, {nullable: true});
          break;
        case 'tinyint':
          table.columns.add(columnList[idx],sql.TinyInt, {nullable: true});
          break;
        case 'char':
          table.columns.add(columnList[idx],sql.Char(length), {nullable: true});
          break;
        case 'nchar':
          table.columns.add(columnList[idx],sql.NChar(length), {nullable: true});
          break;
        case 'text':
          table.columns.add(columnList[idx],sql.Text, {nullable: true});
          break;
        case 'ntext':
          table.columns.add(columnList[idx],sql.NText, {nullable: true});
          break;
        case 'varchar':
          table.columns.add(columnList[idx],sql.VarChar(length), {nullable: true});
          break;
        case 'nvarchar':
          table.columns.add(columnList[idx],sql.NVarChar(length), {nullable: true});
          break;
        case 'json':
          table.columns.add(columnList[idx],sql.NVarChar(sql.MAX), {nullable: true});
          break;
        case 'xml':
          // Added to Unsupported
          // Invalid column data type for bulk load
          table.columns.add(columnList[idx],sql.Xml, {nullable: true});
          break;
        case 'time':
          // sql.Time ([scale])
          // Binding as sql.Time must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.Time(length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'date':
          // Binding as sql.Date must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.Date, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime':
          // Binding as sql.DateTime must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.DateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime2':
          // sql.DateTime2 ([scale]
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.DateTime2(), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetimeoffset':
          // sql.DateTimeOffset ([scale])
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.DateTimeOffset(length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'smalldatetime':
          // Binding as sql.SamllDateTime must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.SmallDateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'uniqueidentifier':
          // table.columns.add(columnList[idx],sql.UniqueIdentifier, {nullable: true});
          // TypeError: parameter.type.validate is not a function
          table.columns.add(columnList[idx],sql.Char(36), {nullable: true});
          break;
        case 'variant':
          table.columns.add(columnList[idx],sql.Variant, {nullable: true});
          break;
        case 'binary':
          table.columns.add(columnList[idx],sql.Binary(length), {nullable: true});
          break;
        case 'varbinary':
          // sql.VarBinary ([length])
           table.columns.add(columnList[idx],sql.VarBinary(length), {nullable: true});
          break;
        case 'image':
  	      // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
          // table.columns.add(columnList[idx],sql.Image, {nullable: true});
          table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
          break;
        case 'udt':
          table.columns.add(columnList[idx],sql.UDT, {nullable: true});
          break;
        case 'geography':
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columnList[idx],sql.Geography, {nullable: true});
  	      // Upload geography as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables bulk upload.
		  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
			case "WKB":
            case "EWKB":
              table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columnList[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case 'geometry':
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columnList[idx],sql.Geometry, {nullable: true});
  	      // Upload geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables bulk upload.
		  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
			case "WKB":
            case "EWKB":
              table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columnList[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case 'hierarchyid':
          table.columns.add(columnList[idx],sql.VarChar(4000),{nullable: true});
          break;
        default:
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,`BULK OPERATION`,`"${tableName}"`],`Unmapped data type [${dataType.type}].`);
      }
    })
    return table
  }

  setTableInfo(tableName) {
	super.setTableInfo(tableName)
	this.tableInfo.insertMode = 'Bulk';
    if (this.tableInfo.bulkSupported) {
	  this.bulkOperations = [
		this.createBulkOperation(this.dbi.DATABASE_NAME, this.tableInfo.tableName, this.tableInfo.columnNames, this.tableInfo.dataTypes)
      , this.createBulkOperation(this.dbi.DATABASE_NAME, this.tableInfo.tableName, this.tableInfo.columnNames, this.tableInfo.dataTypes)
      ]
    }
    else {
      // Place holder for caching rows.
      this.bulkOperations = [
	    new sql.Table()                                            
      , new sql.Table()                                            
	  ]
    }
	
	this.nextBatch = 0;
	this.newBatch()
    	
    this.dataTypes  = YadamuLibrary.decomposeDataTypes(this.tableInfo.targetDataTypes)
    this.rowTransformation  = this.setTransformations(this.tableInfo.targetDataTypes)
		
  }
      
  newBatch() {
	super.newBatch();
	// console.log('newBatch(): Using Operation',this.nextBatch)
	this.batch = this.bulkOperations[this.nextBatch]
	// Exclusive OR (XOR) operator 1 becomes 0, 0 becomes 1.
	this.nextBatch ^= 1;
  }
  
  releaseBatch(batch) {
	if (Array.isArray(batch.rows)) {
	  batch.rows.length = 0;
	}
  }
  
  getMetrics()  {
	const results = super.getMetrics()
	results.insertMode = this.tableInfo ? (this.tableInfo.bulkSupported === true ? 'Bulk' : 'Iterative' ) : 'Bulk'
	return results;
  }
  
  cacheRow(row) {
      
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	
	try {
      this.rowTransformation(row)	
      this.batch.rows.add.apply(this.batch.rows,row);
 
  	  this.metrics.cached++;
	  return this.skipTable;
	} catch (e) {
  	  if (e instanceof RejectedColumnValue) {
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName],e.message);
        this.dbi.yadamu.REJECTION_MANAGER.rejectRow(this.tableName,row);
		this.metrics.skipped++
        return
	  }
	  throw e
	}
  }

  reportBatchError(batch,operation,cause) {
   
    const additionalInfo = {
      columnDefinitions: batch.columns
	}
	try {
      super.reportBatchError(operation,cause,batch.rows[0],batch.rows[batch.rows.length-1],additionalInfo)
	} catch (e) { console.log(e)}
  }
  
  async _writeBatch(batch,rowCount) {
	  	  
    this.metrics.batchCount++;
    
    // console.log(this.constructor.name,'writeBatch()',this.tableInfo.bulkSupported,)
    // console.dir(batch,{depth:null})
    
    if (this.SPATIAL_FORMAT === 'GeoJSON') {
      YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,'WKT',this.tableInfo.targetDataTypes,batch.rows,true)
    } 

    if (this.tableInfo.bulkSupported) {
      try {       
        await this.dbi.createSavePoint()
        const results = await this.dbi.bulkInsert(batch);
		this.endTime = performance.now();
        this.metrics.written += rowCount;
		this.releaseBatch(batch)
		return this.skipTable
      } catch (cause) {
	    this.reportBatchError(batch,`INSERT MANY`,cause)
        await this.dbi.restoreSavePoint(cause);
		if (this.dbi.TRANSACTION_IN_PROGRESS && this.dbi.tediousTransactionError) {
		  // this.yadamuLogger.trace([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableName}"`],`Unexpected ROLLBACK during BCP Operation. Starting new Transaction`);          
		  await this.dbi.recoverTransactionState(true)
		}	
	  	this.yadamuLogger.warning([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableName}"`],`Switching to Iterative mode.`);          
        this.tableInfo.bulkSupported = false;
      }
    }
    
    // Cannot process table using BULK Mode. Prepare a statement use with record by record processing.
 
    try {
      await this.dbi.cachePreparedStatement(this.tableInfo.dml, this.dataTypes,this.SPATIAL_FORMAT) 
    } catch (cause) {
      if (this.rowsLost()) {
		throw cause
      }
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`INSERT ONE`,`"${this.tableName}"`],cause);
      this.endTime = performance.now();
      this.releaseBatch(batch)
      return this.skipTable          
    }	
	
	const sqlTrace = this.status.sqlTrace

    for (const row in batch.rows) {
      try {
        const args = {}
        for (const col in batch.rows[0]){
           args['C'+col] = batch.rows[row][col]
        }
        const results = await this.dbi.executeCachedStatement(args);
		this.metrics.written++
        this.status.sqlTrace = NullWriter.NULL_WRITER;
      } catch (cause) {
		if (this.dbi.TRANSACTION_IN_PROGRESS && this.dbi.tediousTransactionError) {
		  // this.yadamuLogger.trace([`${this.dbi.DATABASE_VENDOR}`,`WRITE`,`"${this.tableName}"`],`Unexpected ROLLBACK during BCP Operation. Starting new Transaction`);          
		  await this.dbi.recoverTransactionState(true)
		}	
        this.handleIterativeError(`INSERT ONE`,cause,row,batch.rows[row]);
        if (this.skipTable) {
          break;
		}
      }
    }       
      
	this.status.sqlTrace = sqlTrace
	this.status.sqlTrace.write(this.dbi.traceComment(`Previous Statement repeated ${rowCount} times.`))
	await this.dbi.clearCachedStatement();   
    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable
  }

  async finalize(cause) {
	await super.finalize(cause)
	this.bulkOperations.length = 0;
	await this.dbi.clearCachedStatement()
  }
  
}

module.exports = MsSQLWriter;