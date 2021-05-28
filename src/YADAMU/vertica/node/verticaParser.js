"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')
const YadamuLibrary = require('../../common/yadamuLibrary.js')

class VerticaParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);     
	
	this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
      switch (dataType.toLowerCase()) {
	    case 'geometry':
	    case 'geography':
	      return (row,idx) => {
			row[idx] = Buffer.from(row[idx],'hex')
		  }	
		case 'interval':
		case 'interval year to month':
		case 'interval day to second':
          switch (true) {
            case (tableInfo.TARGET_DATA_TYPES[idx].toLowerCase().startsWith('interval year')):
		      return (row,idx) => {
			    row[idx] = YadamuLibrary.intervalYearMonthTo8601(row[idx])
		     }
		   case (tableInfo.TARGET_DATA_TYPES[idx].startsWith('interval day')):
   		     return (row,idx) => {
			   row[idx] = YadamuLibrary.intervaDaySecondTo8601(row[idx])
		     }
		   default:
		      return (row,idx) => {
			    row[idx] = YadamuLibrary.intervalYearMonthTo8601(row[idx])
		     }
	      }
		default:
		  return null;
      }
    })
	
    tableInfo.TARGET_DATA_TYPES?.forEach((dataType,idx) => {
	  switch (dataType.toUpperCase()) {
		 case 'XML':
		 case 'XMLTYPE':
		    // Replace xsl:space with xml:space
		   this.transformations[idx] = (row,idx)  => {
             row[idx] = row[idx].replace(/>\\n/g,'>\n')
		   }     
		default:
  		   return null;
      }
    })

    this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
     
	 }) 
    }
  }
    
  async _transform (data,encoding,callback) {
    this.rowCount++;
	// data = Object.values(data)
    this.rowTransformation(data)
    // if (this.rowCount===1) console.log('verticaParser',data)	
	this.push({data:data})
    callback();
  }
}

module.exports = VerticaParser