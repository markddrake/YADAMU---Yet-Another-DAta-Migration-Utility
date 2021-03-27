"use strict" 
const util = require('util')
const stream = require('stream')
const pipeline = util.promisify(stream.pipeline);

const oracledb = require('oracledb');

const YadamuParser = require('../../common/yadamuParser.js')
const StringWriter = require('../../common/stringWriter.js');
const BufferWriter = require('../../common/bufferWriter.js');

const OracleError = require('./oracleException.js');

class OracleParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger); 
	// console.log(tableInfo)
  }
  
  async closeLob(lob) {
	let stack
	const operation = 'oracledb.Lob.close()'
    try {
	  stack = new Error().stack
      await lob.close()
	} catch (e) {
      this.yadamuLogger.handleException([this.constructor.name,'CLOSE_LOB'],new OracleError(e,stack,operation))
    }
  }	 

  async blobToBuffer(blob) {
	  
	let stack
	const operation = 'oracledb.Lob.pipe(Buffer)'
    try {
      const bufferWriter = new  BufferWriter();
	  stack = new Error().stack
  	  await pipeline(blob,bufferWriter)
	  await this.closeLob(blob)
      return bufferWriter.toBuffer()
	} catch(e) {
	  await this.closeLob(blob)
	  throw new OracleError(e,stack,operation)
	}
  }	
  
  async clobToString(clob) {
     
    let stack
	const operation = 'oracledb.Lob.pipe(String)'
	try {
      const stringWriter = new  StringWriter();
      clob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
	  stack = new Error().stack
  	  await pipeline(clob,stringWriter)
	  await this.closeLob(clob)
	  return stringWriter.toString()
	} catch(e) {
	  await this.closeLob(clob)
	  throw new OracleError(e,stack,operation)
	}
  };

  setColumnMetadata(metadata) { 

    this.jsonTransformations = new Array(metadata.length).fill(null)
    
    this.transformations = metadata.map((column,idx) => {
	  switch (column.fetchType) {
		case oracledb.CLOB:
		  return (row,idx)  => {
            row[idx] = this.clobToString(row[idx])
		  }           
	    case oracledb.BLOB:	
          if (this.tableInfo.jsonColumns.includes(idx)) {
            // Convert JSON store as BLOB to string
		    this.jsonTransformations[idx] = (row,idx)  => {
              row[idx] = row[idx].toString('utf8')
		    }
          }
          return (row,idx)  => {
            row[idx] = this.blobToBuffer(row[idx])
		  }
        default:
 		  switch (column.dbType) {
		    case oracledb.DB_TYPE_BINARY_FLOAT:
			case oracledb.DB_TYPE_BINARY_DOUBLE:
		      return (row,idx)  => {
			    switch (row[idx]) {
  			      case 'Inf':
					row[idx] = 'Infinity'
				    break;
				  case '-Inf':
					row[idx] = '-Infinity'
				    break;
				  case 'Nan':
					row[idx] = 'NaN'
				    break;
				  default:
                }				  
		      }
			default:
		  }
  		  return null;
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
	
    this.jsonTransformation = this.jsonTransformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.jsonTransformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
    
  }
  
  async _transform (data,encoding,callback) {
	try {
      this.rowCount++;
	  this.rowTransformation(data)
	  const row = await Promise.all(data)
	  this.jsonTransformation(row)
      // if (this.rowCount === 1) console.log(row)
	  this.push({data:row})
      callback();
	} catch (e) {
      callback(e)
	}
  }

}

module.exports = OracleParser
