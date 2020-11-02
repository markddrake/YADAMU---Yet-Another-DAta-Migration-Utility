"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class JSONWriter extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,tableIdx,status,yadamuLogger) {
	super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
	this.startTable = tableIdx === 0 ? `"${tableName}":[` :  `,"${tableName}":[`
	this.rowSeperator = '';
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
          if (this.SPATIAL_FORMAT.endsWith('GeoJSON')) {
            return (row,idx)  => {
			  if (typeof row[idx] === 'string') {
				try {
			      row[idx] = JSON.parse(row[idx])
				} catch(e) {}
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
  
  beginTable() {
	this.push(this.startTable)  
  }

  async initialize(tableName) {
	await super.initialize(tableName)
	this.beginTable();
  }
  
  batchComplete() {
    return false
  }
  
  commitWork(rowCount) {
    return false;
  }
  
  formatRow(row) {
    return `${this.rowSeperator}${JSON.stringify(row)}`
  }

  cacheRow(row) {
    this.rowTransformation(row)
    this.push(this.formatRow(row));
	this.rowSeperator = ','
    this.metrics.committed++;
  }

  endTable() {
	this.push(']')
  }
  
  finalize(callback) {
	this.endTable();
    super.finalize();	
  }

  async _writeBatch /* OVERRIDE */ () {}

  async commitTransaction() { /* OVERRIDE */ }

  async rollbackTransaction() { /* OVERRIDE */ }

}

module.exports = JSONWriter;