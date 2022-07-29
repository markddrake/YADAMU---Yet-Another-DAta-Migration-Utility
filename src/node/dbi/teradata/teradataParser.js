
import YadamuParser from '../base/yadamuParser.js'

class TeradataParser extends YadamuParser {
  
  generateTransformations(queryInfo) {
	  
	return queryInfo.DATA_TYPE_ARRAY.map((dataType) => {
	  
	  switch (dataType.toUpperCase()) {
		case this.dbi.DATA_TYPES.CHAR_TYPE:
		   return (row,idx) => {
             const cfLength = parseInt(queryInfo.SIZE_CONSTRAINT_ARRAY[idx])
			 if (row[idx].length > cfLength) {
			   row[idx] = row[idx].substring(0,cfLength)
			 }
		   }
		   break;
        /*
		case this.dbi.DATA_TYPES.DECIMAL_TYPE:
		   return (row,idx) => {
			 row[idx] = Number(row[idx])
		   }
		   break;
        */
		case this.dbi.DATA_TYPES.TIME_TYPE:
		case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
		   return (row,idx) => {
			 row[idx] = `1970-01-01T${row[idx]}`.replace('-00:00','Z')
		   }
		   break;
		case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		   return (row,idx) => {
			 row[idx] = `${row[idx].replace(' ','T')}Z`
		   }
		   break;
		case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
		   return (row,idx) => {
			 row[idx] = row[idx].replace(' ','T').replace('-00:00','Z')
		   }
		   break;
		case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
		   return (row,idx) => {
			 row[idx] = Buffer.from(row[idx].buffer)
		   }
		   break;
        default :
		  return null
      }
    })
  }

  /*
  async doTransform(data) {
    if (this.COPY_METRICS.parsed === 1) console.log('P1',data)
    await super.doTransform(data)
    if (this.COPY_METRICS.parsed === 1) console.log('P2',data)
	return data 
  }
  */
  
  constructor(queryInfo,yadamuLogger,parserDelay) {
    super(queryInfo,yadamuLogger,parserDelay);      
  }
  
}

export { TeradataParser as default }