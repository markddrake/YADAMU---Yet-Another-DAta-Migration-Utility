"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')
const YadamuLibrary = require('../../common/yadamuLibrary.js')

class CSVTransform extends YadamuParser {

  constructor(tableInfo,yadamuLogger) {
	super(tableInfo,yadamuLogger)

	this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {

      if (YadamuLibrary.isBinaryType(dataType)) {
        return (row,idx) =>  {
  		  row[idx] = Buffer.from(row[idx],'hex')
		}
      }

	  switch (dataType.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (tableInfo.SPATIAL_FORMAT.endsWith('WKB')) {
            return (row,idx)  => {
  		      row[idx] = row[idx] === null ? null : Buffer.from(row[idx],'hex')
			}
          }
		  return null;
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

  _transform(data,enc,callback) {
	// console.log(Object.values(data))
	this.rowCount++
	data = Object.values(data)
	data.forEach((col,idx) => {
	  if (col.length === 0) {
		data[idx] = null;
	  }
	})
	this.rowTransformation(data)
	this.push({data: data})
    callback()
  }
  
  _final(callback) {
	// this.yadamuLogger.trace([this.constructor.name,this.tableInfo.TABLE_NAME],'_final()');
	this.endTime = performance.now();
    this.push({
      eod: {
	    startTime : this.startTime
	  , endTime   : performance.now()
	  }
    })
	callback()
  } 
}

module.exports = CSVTransform
