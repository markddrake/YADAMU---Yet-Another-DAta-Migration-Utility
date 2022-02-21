"use strict" 

import YadamuParser from '../../common/yadamuParser.js'

class SnowflakeParser extends YadamuParser {

  generateTransformations(queryInfo) {
	  
    return queryInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {
	  switch (dataType) {
		 case 'XML':
		    // Replace xsl:space with xml:space
		   return (row,idx)  => {
             row[idx] = row[idx].replace(/xsl:space/g,'xml:space')
		   }     
         case 'TIMESTAMP_NTZ':		   
		    // Replace 10000-01-01 with
		   return (row,idx)  => {
             row[idx] = row[idx].startsWith('10000-01-01') ? `9999-12-31T23:59:59.${'9'.repeat(parseInt(queryInfo.SIZE_CONSTRAINT_ARRAY[idx]))}+00:00` : row[idx]
		   }     
 		case "REAL":
        case "FLOAT":
		case "DOUBLE":
		case "DOUBLE PRECISION":
		case "BINARY_FLOAT":
		case "BINARY_DOUBLE":
		  return (row, idx) => {
	        if (typeof row[idx] === 'string') {
			  switch(row[idx]) {
			    case 'INF':
			      row[idx] = Infinity
				  break;
		     	case '-INF':
				  row[idx] = -Infinity
				  break;
		     	case 'NAN':
				  row[idx] = NaN
				  break;
			    default:
				  row[idx] = Number(row[idx])
			  }
		    }
	      }         
		default:
  		   return null;
      }
    })

  }

  setTransformations(queryInfo) {

	this.transformations = this.generateTransformations(queryInfo)

	// Use a dummy rowTransformation function if there are no transformations required.

    this.rowTransformation = this.transformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
	    if (row[idx] === 'NULL') {
		  row[idx] = null;
	    }
		else {
          if (Buffer.isBuffer(row[idx])) {
            delete row[idx].toStringSf
            delete row[idx].getFormat
          }
          if ((transformation !== null) && (row[idx] !== null)) {
            transformation(row,idx)
		  }
        }
      }) 
    }
  }
  
  constructor(queryInfo,yadamuLogger) {
    super(queryInfo,yadamuLogger);      	
  }
  
  async doTransform(data) {
    // Snowflake generates o4bject based output, not array based outout. Transform object to array based on columnList
    data = Object.values(data)
	return super.doTransform(data)
  }
}

export { SnowflakeParser as default }