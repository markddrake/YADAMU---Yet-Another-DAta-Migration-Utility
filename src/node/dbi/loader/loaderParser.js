
import YadamuDataTypes  from '../base/yadamuDataTypes.js'
import YadamuParser     from '../base/yadamuParser.js'

class LoaderParser extends YadamuParser {
  
  constructor(tableInfo,yadamuLogger) {
    super(tableInfo,yadamuLogger);      

	this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {

      switch (true) {
        case (YadamuDataTypes.isBinary(dataType)) :
          return (row,idx) =>  {
  		    row[idx] = Buffer.from(row[idx],'hex')
		  }
		case (YadamuDataTypes.isSpatial(dataType)) :
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

	this.rowTransformation = this.transformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
    
  }

  async doTransform(data) {
    // if (this.rowCount === 1) console.log('_transform',data)	
    this.rowTransformation(data.data)
    // if (this.rowCount === 1) console.log('Push',data)
    return data.data
  }
  
}

export {LoaderParser as default }