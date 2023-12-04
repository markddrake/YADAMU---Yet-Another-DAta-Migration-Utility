
import YadamuParser from '../base/yadamuParser.js'

class MongoParser extends YadamuParser {
  
  generateTransformations(queryInfo) {

    this.documentTransformations = []

	if (this.queryInfo.ID_TRANSFORMATION === 'STRIP') {
	  this.documentTransformations.push((document) => { delete document._id; return document })
    }
    	
	if (this.dbi.PASS_THROUGH_ENABLED) {
  	  queryInfo.TARGET_COLUMN_NAMES = queryInfo.COLUMN_NAME_ARRAY
	  this.documentTransformations.push((document) => { return [document] })
	  return new Array(queryInfo.DATA_TYPE_ARRAY.length).fill(null)
	}

	// Reorder the data types by the TARGET column names.
	
	if (queryInfo.hasOwnProperty('TARGET_DATA_TYPES')) {
      if ((queryInfo.TARGET_DATA_TYPES.length === 1) && (queryInfo.TARGET_DATA_TYPES[0] === 'json')) {
	    // Target is an empty Mongo Collection or potentially a relational tabld with a single JSON column 
		// Disable source transformation
	    this.documentTransformations.push((document) => { return [document] })
		queryInfo.TARGET_COLUMN_NAMES = queryInfo.COLUMN_NAME_ARRAY
	    return new Array(queryInfo.DATA_TYPE_ARRAY.length).fill(null)
      }
	}
	else {
      queryInfo.TARGET_COLUMN_NAMES = queryInfo.COLUMN_NAME_ARRAY
	} 
	
	const dataTypes = queryInfo.TARGET_COLUMN_NAMES.map((colName,idx) => {
	   return queryInfo.DATA_TYPE_ARRAY[queryInfo.COLUMN_NAME_ARRAY.indexOf(colName)]
    })
	
	 switch (this.queryInfo.READ_TRANSFORMATION) {
	  case 'DOCUMENT_TO_ARRAY' :
	    // Need to assemble array in correct order.
	    this.documentTransformations.push((document) => { return this.queryInfo.TARGET_COLUMN_NAMES.map((c) => { return document[c] }) })
		break;
      default:
    }
	
	return dataTypes.map((dataType) => {
	  switch (dataType) {
		 case 'binData':
		   return (row,idx)  => {
             row[idx] = row[idx].buffer;
		   }
         case 'objectId':
		   return (row,idx)  => {
             row[idx] = Buffer.from(row[idx].toHexString(),'hex')
		   }
		 case 'long':
		 case 'decimal':
		   return (row,idx)  => {
  		     row[idx] = row[idx].toString()
           }
		 /*
		 case "object":
		 case "array":
		    return (row,idx) => {
		      row[idx] = JSON.stringify(row[idx])
			}
	     */
         default:
		   return null;
      }
    })
  }
  
  async doTransform(document) {

	// if (this.COPY_METRICS.parsed === 1) console.log('mongoParser(sourceDocument)',document,this.documentTransformations.length)
		
    for (const documentTransformation of this.documentTransformations) {
	  document = documentTransformation(document)
	}

    const data = document

	// if (this.COPY_METRICS.parsed === 1) console.log('mongoParser(array)',data)
		
	return await super.doTransform(data)
  }
  
  constructor(dbi,queryInfo,yadamuLogger,parseDelay) {
    super(dbi,queryInfo,yadamuLogger,parseDelay)
  }

}

export { MongoParser as default }