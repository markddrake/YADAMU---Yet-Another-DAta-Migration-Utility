
import YadamuParser           from '../base/yadamuParser.js'
import YadamuDataTypes        from '../base/yadamuDataTypes.js'

class CSVParser extends YadamuParser {

  constructor(dbi,tableInfo,pipelineState,yadamuLogger) {
	  console.log(1)
	super(dbi,tableInfo,pipelineState,yadamuLogger)
	this.transformations = tableInfo.DATA_TYPE_ARRAY.map((dataType,idx) => {

      if (YadamuDataTypes.isBinary(dataType)) {
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

	this.rowTransformation = this.transformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
   }
   
  async doTransform(data) {
    data = Object.values(data)
	data.forEach((col,idx) => {
	  if (col.length === 0) {
		data[idx] = null;
	  }
	})
	this.rowTransformation(data)
  } 
  
  _final(callback) {
	// this.LOGGER.trace([this.constructor.name,this.tableInfo.TABLE_NAME],'_final()');
    this.push({
      eod: {
	    startTime : this.startTime
	  , endTime   : performance.now()
	  }
    })
	callback()
  } 
}

export {CSVParser as default }