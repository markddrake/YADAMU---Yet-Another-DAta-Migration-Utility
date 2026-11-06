
import YadamuParser from '../base/yadamuParser.js'

import TeradataIntervals from './teradataIntervals.js'

class TeradataParser extends YadamuParser {

  // "2026-10-19T12:34:56.123456-08:00" => "2026-10-19T20:34:56.123456Z". A JavaScript Date applies the offset, and keeps the fraction's microseconds aside.

  static toUTC(value) {
	const components = value.match(/^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?([+-]\d\d:\d\d)$/)
	if (!components) {
	  return value
	}
	const [, dateTime, fraction = '', offset] = components
	return new Date(`${dateTime}${offset}`).toISOString().replace(/\.\d{3}Z$/,`${fraction}Z`)
  }

  generateTransformations(queryInfo) {
	  
	return queryInfo.DATA_TYPE_ARRAY.map((dataType) => {

	  // INTERVALs become ISO 8601 durations and PERIODs JSON (see teradataIntervals.js)
	  if (TeradataIntervals.isInterval(dataType.toUpperCase())) {
		return (row,idx) => { row[idx] = TeradataIntervals.intervalToISO(dataType.toUpperCase(),row[idx]) }
	  }
	  if (TeradataIntervals.isPeriod(dataType.toUpperCase())) {
		return (row,idx) => { row[idx] = TeradataIntervals.periodToJSON(dataType.toUpperCase(),row[idx]) }
	  }

	  switch (dataType.toUpperCase()) {
		case this.dbi.DATA_TYPES.BIGINT_TYPE:
		   // teradatasql returns BIGINT as a JavaScript BigInt, which JSON cannot serialize
		   return (row,idx) => {
			 if (typeof row[idx] === 'bigint') {
			   row[idx] = row[idx].toString()
			 }
		   }
		   break;
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
		   return (row,idx) => {
			 row[idx] = `1970-01-01T${row[idx]}`
		   }
		   break;
		case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
		   // A value with a time zone is exchanged in UTC, as the Oracle and Postgres drivers do. A TIME is placed on 1970-01-01, so its date can change.
		   return (row,idx) => {
			 row[idx] = TeradataParser.toUTC(`1970-01-01T${row[idx]}`)
		   }
		   break;
		case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		   return (row,idx) => {
			 row[idx] = `${row[idx].replace(' ','T')}Z`
		   }
		   break;
		case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
		   return (row,idx) => {
			 row[idx] = TeradataParser.toUTC(row[idx].replace(' ','T'))
		   }
		   break;
		case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
		   return (row,idx) => {
			 row[idx] = Buffer.from(row[idx].buffer)
		   }
		   break;
		case this.dbi.DATA_TYPES.XML_TYPE:
		   // Teradata serializes a carriage return as itself, and XML parsers turn CR LF into LF. A carriage return outside a CDATA section, comment
		   // or processing instruction is written as a character reference, so it survives.
		   return (row,idx) => {
			 if ((typeof row[idx] === 'string') && row[idx].includes('\r')) {
			   row[idx] = row[idx].split(/(<!\[CDATA\[[\s\S]*?\]\]>|<!--[\s\S]*?-->|<\?[\s\S]*?\?>)/).map((segment,position) => { return (position % 2 === 0) ? segment.replaceAll('\r','&#xD;') : segment }).join('')
			 }
		   }
		   break;
		case this.dbi.DATA_TYPES.SPATIAL_TYPE:
		case 'ST_GEOMETRY':
		   // WKB, read with ST_AsBinary() (see TeradataDBI.getClientSelectList())
		   return (row,idx) => {
			 if (row[idx] instanceof Uint8Array) {
			   row[idx] = Buffer.from(row[idx].buffer,row[idx].byteOffset,row[idx].byteLength)
			 }
		   }
		   break;
        default :
		  return null
      }
    })
  }

  /*
  async doTransform(data) {
    if (this.PIPELINE_STATE.parsed === 1) console.log('P1',data)
    await super.doTransform(data)
    if (this.PIPELINE_STATE.parsed === 1) console.log('P2',data)
	return data 
  }
  */
  
  constructor(dbi,queryInfo,yadamuLogger,pipelineState) {
    super(dbi,queryInfo,yadamuLogger,pipelineState);      
  }
  
}

export { TeradataParser as default }