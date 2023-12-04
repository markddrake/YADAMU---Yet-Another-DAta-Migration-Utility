
import { 
  performance 
}                               from 'perf_hooks';
						
import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

class DB2OutputManager extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }

  generateTransformations(targetDataTypes) {

    // console.log(targetDataTypes)

    // Set up Transformation functions to be applied to the incoming rows
    this.tableInfo.paramsTemplate = []
	
    return  this.tableInfo.targetDataTypes.map((targetDataType,idx) => {        
	  const paramDefinition = { ParamType: "ARRAY", SQLType: this.dbi.DATA_TYPES.VARCHAR_TYPE, Data: []}
	  this.tableInfo.paramsTemplate.push(paramDefinition)
      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType);
	  switch (dataTypeDefinition.type.toUpperCase()) {
		case this.dbi.DATA_TYPES.JSON_TYPE:
          paramDefinition.SQLType = this.dbi.DATA_TYPES.CLOB_TYPE
		  return (col,idx) => {
		    // Top Level of a BSON object must be an object 
			let val = col
			switch (true) {
			  case (Array.isArray(col)):
                val = JSON.stringify({ yadamu : col })
				break
		      case (typeof col === 'object'):
			    val = JSON.stringify(col)
				break
			  case ((typeof col === 'string') && (col[col.search((/\S|$/))] === '[')):
			    val = JSON.stringify({ yadamu: JSON.parse(col)})
				break
			  case ((typeof col === 'string') && (col[col.search((/\S|$/))] === '{')):
				break
			  default:
			    val = JSON.stringify({yadamu: col})
			}
			return val
		  }
		case this.dbi.DATA_TYPES.SMALLINT_TYPE:
          paramDefinition.SQLType = this.dbi.DATA_TYPES.SMALLINT_TYPE
		  return (col, idx) => {
			  return typeof col === "string" ? parseInt(col) : col
		  }
		case this.dbi.DATA_TYPES.INTEGER_TYPE:
          paramDefinition.SQLType = this.dbi.DATA_TYPES.INTEGER_TYPE
		  return (col, idx) => {
			  return typeof col === "string" ? parseInt(col) : col
		  }
		case this.dbi.DATA_TYPES.BIGINT_TYPE:
		  paramDefinition.Length = 25
          paramDefinition.SQLType = this.dbi.DATA_TYPES.VARCHAR_TYPE
		  return null
		case this.dbi.DATA_TYPES.FLOAT_TYPE:
		case this.dbi.DATA_TYPES.DOUBLE_TYPE:
		  paramDefinition.Length = 25
          paramDefinition.SQLType = this.dbi.DATA_TYPES.VARCHAR_TYPE
		  switch (this.dbi.INFINITY_MANAGEMENT) {
		    case 'REJECT':
              return (col, idx) => {
  		        if (isFinite(col)) {
				  return typeof col === 'string' ? col : col.toExponential(17) 
				}
				throw new RejectedColumnValue(this.tableInfo.columnNames[idx],col)
		      }
		    case 'NULLIFY':
		      return (col, idx) => {
			    if (isFinite(col)) {
				  return typeof col === 'string' ? col : col.toExponential(17)
			    }	
    	        this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName],`Column "${this.tableInfo.columnNames[idx]}" contains unsupported value "${col}". Column nullified.`)
                return null
		      }   
            default :
		      return (col, idx) => {
                return typeof col === 'string' ? col : col.toExponential(17) 	       
			  }
		  }
          return null;
		case this.dbi.DATA_TYPES.NUMERIC_TYPE:
		case this.dbi.DATA_TYPES.DECIMAL_TYPE:
		case this.dbi.DATA_TYPES.IBMDB2_DECFLOAT_TYPE:
          paramDefinition.SQLType = this.dbi.DATA_TYPES.VARCHAR_TYPE
	      return (col,idx) => {
			return typeof col === 'number' ? col.toString() : col
		  }			
		case this.dbi.DATA_TYPES.BINARY_TYPE:
		case this.dbi.DATA_TYPES.VARBINARY_TYPE:
		  /*
		  **
		  
          paramDefinition.SQLType = this.dbi.DATA_TYPES.VARCHAR_TYPE
		  return (col,idx) => {
			return Buffer.isBuffer(col) ? col.toString('hex') : col
		  }
		  
		  **
		  */
		  paramDefinition.SQLType = this.dbi.DATA_TYPES.BINARY_TYPE
		  return (col,idx) => {
			return typeof col === 'string' ? Buffer.from(col,'hex') : col
		  }
		case this.dbi.DATA_TYPES.BLOB_TYPE:
		  /*
		  **

          paramDefinition.SQLType = this.dbi.DATA_TYPES.CLOB_TYPE
 		  return (col,idx) => {
            return Buffer.isBuffer(col) ?  col.toString('hex') : col
		  }     

		  **
		  */

          paramDefinition.SQLType = this.dbi.DATA_TYPES.BLOB_TYPE
		  return (col,idx) => {
			return typeof col === 'string' ? Buffer.from(col,'hex') : col
		  }
		case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
          paramDefinition.SQLType = this.dbi.DATA_TYPES.BOOLEAN_TYPE
          return (col,idx) => {
            return typeof col === 'boolean' ? col : YadamuLibrary.toBoolean(col)
	      }
          break;
   		case this.dbi.DATA_TYPES.TIME_TYPE:
		  return (col,idx) => {
		    return typeof col === 'string' && col[10] === 'T' ? col.substring(11) : col
		  }	       
		case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		  return (col,idx) => {
			let val = ((typeof col === 'object') && (typeof col.toISOString === 'function')) ? col.toISOString() : col
		    val = val.replace('T','-').replace('Z','').replace(/\+00:00$/,'').replace(/-00:00$/,'')
			
			return val
		  }	       
		case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
		  return (col,idx) => {
			let val = ((typeof col === 'object') && (typeof col.toISOString === 'function')) ? col.toISOString() : col
			// ### ToDo - Shift to UTC and preserve > 6 digit precision
		    val = val.replace('T','-').replace('Z','')
			return val
		  }	       
		case this.dbi.DATA_TYPES.DATE_TYPE:
		  return (col,idx) => {
			return typeof col === 'string' ? col.substring(0,10) : col.toISOString().substring(0,10)
		  }
		case this.dbi.DATA_TYPES.XML_TYPE:
		  // Avoid use of CLOBs due to issues related to client and server attempting to negotiate character set Conversion
		  paramDefinition.SQLType = this.dbi.DATA_TYPES.BLOB_TYPE
		  return (col,idx) => {
			 return Buffer.from(typeof col === 'object' ? JSON.stringify(col) : col)
		  }
		case this.dbi.DATA_TYPES.CHAR_TYPE:
		case this.dbi.DATA_TYPES.NCHAR_TYPE:
		case this.dbi.DATA_TYPES.VARCHAR_TYPE:
		case this.dbi.DATA_TYPES.NVARCHAR_TYPE:
		case this.dbi.DATA_TYPES.CLOB_TYPE:
		case this.dbi.DATA_TYPES.NCLOB_TYPE:
	      return (col,idx) => {
			/*
     		**
		    ** Starting with ibm_db release 3.x empty strings are handled correctly
		    **
		   
			// https://github.com/ibmdb/node-ibm_db/issues/875 
			// Disable Batch Mode if data contains empty strings 
			this.tableInfo.insertMode = ((col.length > 0) || (this.tableInfo.instMode !== 'Batch')) ?  this.tableInfo.insertMode : 'Iterative'
			
			**
			*/
			return typeof col === 'object' ? JSON.stringify(col) : col
		  }
	    case this.dbi.DATA_TYPES.SPATIAL_TYPE:
		  if (!this.dbi.DB2GSE_INSTALLED) {
		    switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
		      case 'WKB':
		      case 'EWKB':
			    /* 
				**
 
                paramDefinition.SQLType = this.dbi.DATA_TYPES.CLOB_TYPE
 		        return (col,idx) => {
                  return Buffer.isBuffer(col) ?  col.toString('hex') : col
		        }     

				**
				*/
                paramDefinition.SQLType = this.dbi.DATA_TYPES.BLOB_TYPE
		        return (col,idx) => {
    			  return typeof col === 'string' ? Buffer.from(col,'hex') : col
		        }
              case "WKT":
              case "EWKT":
      		    paramDefinition.SQLType = this.dbi.DATA_TYPES.CLOB_TYPE
                return null
              case "GeoJSON":
                paramDefinition.SQLType = this.dbi.DATA_TYPES.CLOB_TYPE
		        return (col,idx) => {
			      return typeof col === 'object' ? JSON.stringify(col) : col
		        }
              default :
                return null
		    }
		  }
          return null
        default :
          return null
      }
	})
  }
  
  createParams() {
	
	return JSON.parse(JSON.stringify(this.tableInfo.paramsTemplate))
  }
	
  createBatch() {
    return   {
	  sql    : this.tableInfo.dml
	, params : this.createParams()
    }
  }

  resetBatch(batch) {
    batch.params = this.createParams()
  }
    
  async setTableInfo(tableInfo) {
    await super.setTableInfo(tableInfo)
  }
  
  /*
  **
  ** The default implimentation is shown below. It applies any transformation functions that have were defiend in setTableInfo andt
  ** pushes the row into an array or rows waiting to fed to a batch insert mechanism
  **
  ** If your override this function you must ensure that this.COPY_METRICS.cached is incremented once for each call to cache row.
  ** 
  ** Also if your solution does not cache one row in this.batch for each row processed you will probably need to override the following 
  ** functions in addtion to cache row.
  **
  **  batchComplete() : returns true when it it time to perform a bulk insert.
  **   
  **  handleBatchException(): creates an exception containing a summary of the records being inserted if an error occurs during a batch insert.
  **  
    
  this.rowTransformation(row)
  this.batch.push(row);
    
  this.COPY_METRICS.cached++
  return this.skipTable;
   
  **
  */

  cacheRow(row) {
	this.rowTransformation(row)
	row.forEach((col,idx) => { this.batch.params[idx].Data.push(col)})
    this.COPY_METRICS.cached++
    return this.skipTable;  
  }
      
}

export { DB2OutputManager as default }