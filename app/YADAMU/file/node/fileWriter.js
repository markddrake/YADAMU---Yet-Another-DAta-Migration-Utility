"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class FileWriter extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
   
  setTableInfo(tableName) {
    super.setTableInfo(tableName)
    this.insertMode = 'JSON';    
    if (this.dbi.tableMappings && this.dbi.tableMappings.hasOwnProperty(tableName)) {
	  tableName = this.dbi.tableMappings[tableName].tableName
	}

    this.transformations = this.tableInfo.targetDataTypes.map((targetDataType,idx) => {      
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
	
	  if (YadamuLibrary.isBinaryDataType(dataType.type)) {
		return (row,idx) =>  {
          row[idx] = row[idx].toString('hex')
		}
      }
      
	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (row,idx)  => {
			  if (Buffer.isBuffer(row[idx])) {
			    row[idx] = row[idx].toString('hex')
			  }
			}
          }
		  return null;
        case "JSON":
          return (row,idx) =>  {
            if (typeof row[idx] === 'string') {
              row[idx] = JSON.parse(row[idx])
            } 
			if (Buffer.isBuffer(row[idx])) {
			  row[idx] = JSON.parse(row[idx].toString('utf8'))
			}
          }
        case "BOOLEAN":
          return (row,idx) =>  {
		    const bool = (typeof row[idx] === 'string') ? row[idx].toUpperCase() : (Buffer.isBuffer(row[idx])) ? row[idx].toString('hex') : row[idx]
			switch(bool) {
              case true:
              case "TRUE":
              case "01":
              case "1":
			  case 1:
                row[idx] = true;
				break;
              case false:
              case "FALSE":
              case "00":
              case "0":
			  case 0:
                row[idx] = false;
				break;
              default: 
            }
          }
        case "DATE":
          return (row,idx) =>  { 
            if (row[idx] instanceof Date) {
              row[idx] = row[idx].toISOString()
            }
          }
        case "TIMESTAMP":
          return (row,idx) =>  { 
            // A Ti7mestamp not explicitly marked as UTC is coerced to UTC.
			switch (true) {
              case (typeof row[idx] === 'string'):
                row[idx] = (row[idx].endsWith('Z') || row[idx].endsWith('+00:00')) ? row[idx] : row[idx] + 'Z';
				break;
              case (row[idx] instanceof Date):
                row[idx] = row[idx].toISOString()
            }
          }
		 default:
		   return null
      }
    }) 
	
    // Use a dummy rowTransformation function if there are no transformations required.

	this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
  }

  startOuterArray() {
 	this.rowSeperator = '';
    this.outputStream.write(`[`);
  }

  async initialize(tableName) {
	await super.initialize(tableName)
	this.outputStream = this.dbi.getFileOutputStream(tableName);
	this.startOuterArray()
  }
  
  batchComplete() {
    return false
  }
  
  commitWork(rowCount) {
    return false;
  }

  cacheRow(row) {
	  
	// if (this.rowCounters.received === 1) console.log(row)
    this.rowTransformation(row)
      
    this.outputStream.write(`${this.rowSeperator}${JSON.stringify(row)}`);
	this.rowSeperator = ','
    this.rowCounters.committed++;
  }

  async writeBatch() {
  }

  async endOuterArray() {
    this.outputStream.write(`]`);
  }

  
  async finalize(readerStatistics) {
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.hasPendingRows(),this.rowCounters.received,this.rowCounters.committed,this.rowCounters.written,this.rowCounters.cached],'_flushCahce()')  
	this.endOuterArray();
	super.finalize(readerStatistics);
  }

  async commitTransaction() {}

  async rollbackTransaction() {}

}

module.exports = FileWriter;