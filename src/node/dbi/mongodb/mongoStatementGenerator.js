
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class MongoStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  /*
  **
  ** A Decimal128 holds 34 significant digits. A Teradata DECIMAL or NUMBER with a larger precision is stored as a string, so that every value
  ** survives exactly, e.g. a DECIMAL(38,10). A NUMBER without a precision has up to 38 digits. Only Teradata sources, for now: other vendors'
  ** wide decimals still become Decimal128, which rejects a value with more than 34 digits.
  **
  */

  static get DECIMAL128_PRECISION() { return 34 }

  static get STRING_DECIMAL_VENDORS() {
	// The precision of a NUMBER without one
	return { Teradata: 38 }
  }

  refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint) {
	const dataType = super.refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint)
	if ((dataType === this.dbi.DATA_TYPES.MONGO_DECIMAL128_TYPE) && MongoStatementGenerator.STRING_DECIMAL_VENDORS.hasOwnProperty(this.SOURCE_VENDOR)) {
	  const precision = sizeConstraint?.length > 0 ? sizeConstraint[0] : MongoStatementGenerator.STRING_DECIMAL_VENDORS[this.SOURCE_VENDOR]
	  if (precision > MongoStatementGenerator.DECIMAL128_PRECISION) {
		return this.dbi.DATA_TYPES.VARCHAR_TYPE
	  }
	}
	return dataType
  }

  generateStorageClause(mappedDataType) {
	return mappedDataType
  }
  
  emptyTable(tableMetadata) {
	return ((tableMetadata.columnNames.length === 1) && (tableMetadata.columnNames[0] === 'DOCUMENT') && (tableMetadata.dataTypes[0] === 'json'))
  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'DOCUMENT';
    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)
	
    let columnNames = tableMetadata.columnNames
    let dataTypes = tableMetadata.dataTypes
    let sizeConstraints = tableMetadata.sizeConstraints
	let targetDataTypes = this.getTargetDataTypes(tableMetadata)
  
    /*
    **
    ** ARRAY_TO_DOCUMENT uses the column name, data types and size constraint information from the source database to set up 
    ** the transformations required to convert the incoming array into a document. 
    **   
    */

    if (((this.dbi.WRITE_TRANSFORMATION === 'ARRAY_TO_DOCUMENT') || this.emptyTable(tableMetadata)) && (tableMetadata.hasOwnProperty("source"))) {
      columnNames = tableMetadata.source.columnNames 
      dataTypes = tableMetadata.source.dataTypes;
      sizeConstraints = tableMetadata.source.sizeConstraints 
	  targetDataTypes = this.getTargetDataTypes(tableMetadata.source)
    }

	if (this.dbi.WRITE_TRANSFORMATION === 'PASS_THROUGH') {
      columnNames = ['DOCUMENT'];
	  dataTypes = ['json']
	  sizeConstraints = [[]]
	  targetDataTypes = 'object'
	}
	
	if ((tableMetadata.hasOwnProperty("source")) &&  (tableMetadata.source.columnNames.length === 1) && (tableMetadata.source.dataTypes[0].toUpperCase() === "JSON")) {
	  // If the source table consists of a single JSON Column then insert each row into MongoDB 'As Is'   																									
      insertMode = "DOCUMENT"
	}
    else {
      switch (this.dbi.WRITE_TRANSFORMATION) {
        case 'ARRAY_TO_DOCUMENT':
          insertMode = 'OBJECT'
          break;
        case 'PRESERVE':
          insertMode = 'ARRAY'             
		  break;
		case 'PASS_THROUGH':
		  insertMode = 'PASS_THROUGH'
		  break;
        default:
          insertMode = 'OBJECT'
      }
    }
    	
    const tableInfo =  { 
      ddl             : tableMetadata.tableName
    , dml             : `insertMany(${tableMetadata.tableName})`
    , columnNames     : columnNames
    , targetDataTypes : targetDataTypes
    , sizeConstraints : sizeConstraints
    , insertMode      : insertMode
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.SPATIAL_FORMAT
    }
	
	return tableInfo
  }

}

export { MongoStatementGenerator as default }

