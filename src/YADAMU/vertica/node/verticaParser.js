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
	    /*
	    case 'binary':
        case 'varbinary':
	    case 'long varbinary':
	      return (row,idx) => {
			row[idx] = Buffer.from(row[idx],'hex')
		  }
		*/
	    case 'boolean':
	      return (row,idx) => {
			row[idx] = YadamuLibrary.toBoolean(row[idx])
		  }		
		case 'interval year to month':
		  return (row,idx) => {
			const components = row[idx].split('-');
		    row[idx] = `P${components[0]}Y${components[1]}M`
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