"use strict"

const { performance } = require('perf_hooks');
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
      
  getStatistics()  {
	const results = super.getStatistics()
	results.insertMode = this.tableInfo.bulkSupported === true ? 'Bulk' : 'Iterative'
	return results;
  }
  
  async finalize() {
    await super.finalize()
    if (this.dbi.preparedStatement !== undefined){
      await this.dbi.preparedStatement.unprepare();
	  this.dbi.preparedStatement = undefined;
    }
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

	this.rowsCached++;
	return this.skipTable;
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
		this.rowsWritten += this.rowsCached;
		this.rowsCached = 0;
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
 
    await this.dbi.cachePreparedStatement(this.tableInfo.dml, this.tableInfo.dataTypes,this.tableInfo.spatialFormat) 

    for (const row in this.tableInfo.bulkOperation.rows) {
      try {
        const args = {}
        for (const col in this.tableInfo.bulkOperation.rows[0]){
           args['C'+col] = this.tableInfo.bulkOperation.rows[row][col]
        }
        const results = await this.dbi.executeCachedStatement(args);
		this.rowsWritten++
      } catch (e) {
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,JSON.stringify(this.tableInfo.bulkOperation.rows[row])] : []
        this.skipTable = await this.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.tableInfo.bulkOperation.rows.length,row,this.tableInfo.bulkOperation.rows[row],e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }       
      
    await this.dbi.clearCachedStatement();   
	
    this.endTime = performance.now();
    this.tableInfo.bulkOperation.rows.length = 0;
	this.rowsCached = 0;
    return this.skipTable
  }
}

module.exports = TableWriter;