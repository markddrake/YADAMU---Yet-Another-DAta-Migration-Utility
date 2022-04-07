
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class MongoStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  generateStorageClause(mappedDataType) {
	return mappedDataType
  }
  
  emptyTable(tableMetadata) {
	return ((tableMetadata.columnNames.length === 1) && (tableMetadata.columnNames[0] === 'JSON_DATA') && (tableMetadata.dataTypes[0] === 'json'))
  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'DOCUMENT';
    
    let columnNames = tableMetadata.columnNames
    let dataTypes = tableMetadata.dataTypes
    let sizeConstraints = tableMetadata.sizeConstraints

    /*
    **
    ** ARRAY_TO_DOCUMENT uses the column name, data types and size constraint information from the source database to set up 
    ** the transformations required to convert the incoming array into a document. 
    **   
    */

    if (((this.dbi.WRITE_TRANSFORMATION === 'ARRAY_TO_DOCUMENT') || this.emptyTable(tableMetadata)) && (tableMetadata.source)) {
      columnNames = tableMetadata.source.columnNames 
      dataTypes = tableMetadata.source.dataTypes;
      sizeConstraints = tableMetadata.source.sizeConstraints 
    }
     
	const mappedDataTypes = columnNames.map((columnName,idx) => { 
	  const mappedDataType = tableMetadata.source ? tableMetadata.source.dataTypes[idx] : this.getMappedDataType(dataTypes[idx],sizeConstraints[idx])
	  return mappedDataType
	})
   	
    if ((columnNames.length === 1) && (dataTypes[0] === 'JSON')) {
        // If the source table consists of a single JSON Column then insert each row into MongoDB 'As Is'   
        insertMode = 'DOCUMENT'
    }
    else {
      switch (this.dbi.writeTransformation) {
        case 'ARRAY_TO_DOCUMENT':
          insertMode = 'OBJECT'
          break;
        case 'PRESERVE':
          insertMode = 'ARRAY'
          break;
        default:
          insertMode = 'OBJECT'
      }
    }    
    
    const tableInfo =  { 
      ddl             : tableMetadata.tableName
    , dml             : `insertMany(${tableMetadata.tableName})`
    , columnNames     : columnNames
    , targetDataTypes : mappedDataTypes
    , sizeConstraints : sizeConstraints
    , insertMode      : insertMode
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
	
	return tableInfo
  }

}

export { MongoStatementGenerator as default }

