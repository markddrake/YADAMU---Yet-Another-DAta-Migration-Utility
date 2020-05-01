"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.tableInfo.columnCount = this.tableInfo.targetDataTypes.length;

    this.transformations = this.tableInfo.dataTypes.map(function(dataType,idx) {
      switch (dataType.type) {
        case "bit" :
		  return function(col,idx) {
            if (col === true) {
              return 1
            }
            else {
              return 0
            }  
		  }
          break;
        case "boolean" :
 		  return function(col,idx) {
           switch (col) {
              case "00" :
                return false;
                break;
              case "01" :
                return true;
                break;
              default:
			    return col
            }
          }
          break;
        case "bytea" :
		  return function(col,idx) {
            return Buffer.from(col,'hex');
		  }
          break;
        case "time" :
		  return function(col,idx) {
            if (typeof col === 'string') {
              let components = col.split('T')
              col = components.length === 1 ? components[0] : components[1]
              return col.split('Z')[0]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds();  
            }
		  }
          break;
        case 'date':
        case 'datetime':
        case 'timestamp':
		  return function(col,idx) {
            if (typeof col === 'string') {
              if (col.endsWith('Z') && col.length === 28) {
                col = col.slice(0,-2) + 'Z'
              }
              else {
                if (!col.endsWith('+00:00')) {
                  if (col.length === 27) {                                
                    col = col.slice(0,-1) 
                  }
                }
              }               
            }
            else {
              // Avoid unexpected Time Zone Conversions when inserting from a Javascript Date object 
              col = col.toISOString();
            }
			return col
		  }
          break;
        default :
		  return null
      }
    },this)

  }
  
  async appendRow(row) {
	  
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	  
	this.transformations.forEach(function (transformation,idx) {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	},this)
	
    this.batch.push(...row);
	
    this.rowsCached++
	return this.skipTable
  }
      
  async writeBatch() {

    this.batchCount++;
    let repackBatch = false;

    if (this.insertMode === 'Batch') {
               
      try {
        await this.dbi.createSavePoint();
        let argNumber = 1;
        const args = Array(this.rowsCached).fill(0).map(function() {return `(${this.tableInfo.insertOperators.map(function(operator) {return operator.replace('$%',`$${argNumber++}`)}).join(',')})`},this).join(',');
        const sqlStatement = this.tableInfo.dml + args
        const results = await this.dbi.insertBatch(sqlStatement,this.batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.batch.length = 0;
		this.rowsWritten += this.rowsCached;
        this.rowsCached = 0;
        return this.skipTable
      } catch (e) {
        if (this.status.showInfoMsgs) {
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Batch size [${this.rowsCached}].  Batch Insert raised:\n${e}.`);
          this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
          // Use Slice to print first and last row, rather than first and last value.
          this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch.slice(0,this.tableInfo.columnCount))}\n...\n${JSON.stringify(this.batch.slice(this.batch.length-this.tableInfo.columnCount,this.batch.length))}\n`);
          this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
        }
        await this.dbi.restoreSavePoint(e);
        this.tableInfo.insertMode = 'Iterative' 
        repackBatch = true;
      }
    } 
     
    let argNumber = 1;
    const args = Array(1).fill(0).map(function() {return `(${Array(this.tableInfo.targetDataTypes.length).fill(0).map(function(){return `$${argNumber++}`}).join(',')})`},this).join(',');
    const sqlStatement = this.tableInfo.dml + args
    for (let row =0; row < this.rowsCached; row++) {
      const nextRow = repackBatch ?  this.batch.splice(0,this.tableInfo.columnCount) : this.batch[row]
      try {
        this.dbi.createSavePoint();
        const results = await this.dbi.insertBatch(sqlStatement,nextRow);
        this.dbi.releaseSavePoint();
		this.rowsWritten++;
      } catch(e) {
        this.dbi.restoreSavePoint(e);
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,JSON.stringify(nextRow)] : []
        this.skipTable = await this.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.rowsCached,row,nextRow,e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }

    this.endTime = performance.now();
    this.batch.length = 0;
	this.rowsCached = 0;
    return this.skipTable   
  }
}

module.exports = TableWriter;