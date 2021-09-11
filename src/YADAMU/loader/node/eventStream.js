"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')
const YadamuLibrary = require('../../common/yadamuLibrary.js')

class EventStream extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    
	super(tableInfo,yadamuLogger);      

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

  async _transform (data,encoding,callback) {
	try {
      this.rowCount++
      // if (this.rowCount === 1) console.log('_transform',data)	
      this.rowTransformation(data.data)
      // if (this.rowCount === 1) console.log('Push',data)
      this.push(data)
      callback();  
	} catch (e) {
      callback(e)
	}
  }
}

module.exports = EventStream