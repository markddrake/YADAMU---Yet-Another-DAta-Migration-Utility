"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class JSONWriter extends YadamuWriter {
	     
  constructor(dbi,tableName,ddlComplete,firstTable,status,yadamuLogger) {
	super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
	this.startTable = firstTable ? `"${tableName}":[` :  `,"${tableName}":[`
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
	  if (YadamuLibrary.isBinaryType(dataType.type)) {
		return (col,idx) =>  {
          return col.toString('hex')
		}
      }
      
	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case "POINT":
        case "LSEG":
        case "BOX":
        case "PATH":
        case "POLYGON":
        case "CIRCLE":
        case "LINESTRING":
        case "MULTIPOINT":
        case "MULTILINESTRING":
        case "MULTIPOLYGON":
		case "GEOMCOLLECTION":
		case "GEOMETRYCOLLECTION":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (col,idx)  => {
			  if (Buffer.isBuffer(col)) {
			    return col.toString('hex')
			  }
			  return col
			}
          }
          if (this.SPATIAL_FORMAT.endsWith('GeoJSON')) {
            return (col,idx)  => {
			  if (typeof col === 'string') {
				try {
			      return JSON.parse(col)
				} catch(e) {}
			  }
			  return col
			}
          }
		  return null;
		case "REAL":
        case "FLOAT":
		case "DOUBLE":
		case "DOUBLE PRECISION":
		case "BINARY_FLOAT":
		case "BINARY_DOUBLE":
		   return (col,idx) => {
			 if (!isFinite(col)) {
			   switch (true) {
		         case isNaN(col): 
		   	       return "NaN"
				   break;
			     case (col === Infinity):
				   return "Infinity"
				   break;
				 case (col === -Infinity):
				   return "-Infinity"
				   break;
				 default:
			   }   
		     }
  		     return col
		   }			 
        case "JSON":
          return (col,idx) =>  {
            if (typeof col === 'string') {
              return JSON.parse(col)
            } 
			if (Buffer.isBuffer(col)) {
			  return JSON.parse(col.toString('utf8'))
			}
   		    return col
          }
        case "BOOLEAN":
          return (col,idx) =>  {
		    const bool = (typeof col === 'string') ? col.toUpperCase() : (Buffer.isBuffer(col)) ? col.toString('hex') : col
			switch(bool) {
              case true:
              case "TRUE":
              case "01":
              case "1":
			  case 1:
                return true;
				break;
              case false:
              case "FALSE":
              case "00":
              case "0":
			  case 0:
                return false;
				break;
              default: 
            }
            return col
          }
        case "DATE":
          return (col,idx) =>  { 
            if (col instanceof Date) {
              return col.toISOString()
            }
		    return col
          }
        case "TIMESTAMP":
          return (col,idx) =>  { 
            // A Timestamp not explicitly marked as UTC is coerced to UTC.
			switch (true) {
              case (typeof col === 'string'):
                return (col.endsWith('Z') || col.endsWith('+00:00')) ? col : col + 'Z';
				break;
              case (col instanceof Date):
                return col.toISOString()
            }
  		    return col
          }
		 default:
		   return null
      }
    }) 
	
    // Use a dummy rowTransformation function if there are no transformations required.

	this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
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

  async processRow(row) {
    // Be very careful about adding unecessary code here. This is executed once for each row processed by YADAMU. Keep it as lean as possible.
	this.checkColumnCount(row)

	this.rowTransformation(row)

	/*
	this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {
        transformation(row,idx)
      }
    })
	*/
	
    const x = this.push(this.formatRow(row));
	this.rowSeperator = ','
    this.metrics.committed++;
    if ((this.FEEDBACK_INTERVAL > 0) && ((this.metrics.committed % this.FEEDBACK_INTERVAL) === 0)) {
      this.yadamuLogger.info([`${this.tableName}`,this.dbi.OUTPUT_FORMAT],`Rows Written: ${this.metrics.committed}.`);
    }
  }

  async endTable() {
    // Called from YadamuWriter when 'eod' is recieved during import or from _final during direct copy
	this.push(']')
  }

  async _final(callback) {
	 /* OVERRIDE */ 
	await this.endTable()
    callback()
  }	  
  
  async _destroy(err,callback) {
	 /* OVERRIDE */ 
	this.endTime = performance.now()
    this.reportPerformance(err)
 	// this.emit('writerComplete')
	callback()
  }	  
  
  async _writeBatch /* OVERRIDE */ () {}

  async commitTransaction() { /* OVERRIDE */ }

  async rollbackTransaction() { /* OVERRIDE */ }
  
}

module.exports = JSONWriter;