
import YadamuDataTypes           from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator  from '../base/yadamuStatementGenerator.js'

class CockroachStatementGenerator extends YadamuStatementGenerator {

  get MAX_ARGUMENT_COUNT()    { return 65535 } // more than 65535 arguments to prepared statement: 65536

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
	
    // Do not include rowid in the DDL even when the rowid is to be perserved. This will force Cockroach to automatically re-instate it with the correct indexes etc.
	
	const colDefs = !this.dbi.COCKROACH_STRIP_ROWID ? columnDefinitions.filter((colDef) => { return colDef !== '"rowid" bigint' }) : columnDefinitions  
    return `create table "${schema}"."${tableName}"(\n  ${colDefs.join(',')})`;
    
  }
  
  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}") values`;
  }
  
  
  getInsertOperator(targetDataType) {

    const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType)

    switch (targetDataType) {
      case this.dbi.DATA_TYPES.SPATIAL_TYPE:
      case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
      case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
        switch (this.SPATIAL_FORMAT) {
          case "WKB":
            return `ST_GeomFromWKB($%)::${dataTypeDefinition.type}`
          case "EWKB":
            return `ST_GeomFromEWKB($%)::${dataTypeDefinition.type}`
          case "WKT":
            return `ST_GeomFromText($%)::${dataTypeDefinition.type}`
          case "EWKT":
            return `ST_GeomFromEWKT($%)::${dataTypeDefinition.type}`
          case "GeoJSON":
            return `ST_GeomFromGeoJSON($%)::${dataTypeDefinition.type}`
          default:
            return `$%::${dataTypeDefinition.type}`
        }
        return '$%';
    }
    return '$%';
  }
    
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';
    const maxBatchSize = Math.trunc(this.MAX_ARGUMENT_COUNT / tableMetadata.columnNames.length);
    let batchSize = this.dbi.BATCH_SIZE > maxBatchSize ? maxBatchSize : this.dbi.BATCH_SIZE

    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)
    
    const insertOperators = []
    
	const targetDataTypes = this.getTargetDataTypes(tableMetadata)
	const columnDataTypes = targetDataTypes.map((targetDataType)  => {
	  return targetDataType
	})
	const sizeConstraints = [...tableMetadata.sizeConstraints]
	
	// this.debugStatementGenerator(null,null)

    const columnDefinitions = targetDataTypes.map((targetDataType,idx) => {		

      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType)  	  
	  const sourceDataType = tableMetadata.dataTypes[idx]
      const columnName = tableMetadata.columnNames[idx]
	  switch (targetDataType) {
	    case this.dbi.DATA_TYPES.BLOB_TYPE:
		case this.dbi.DATA_TYPES.CLOB_TYPE:
	      sizeConstraints[idx] = [] 
		  break;
		case this.dbi.DATA_TYPES.XML_TYPE:
		  columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.XML_TYPE
	      sizeConstraints[idx] = [] 
		  break;
	  }
	  
	  columnDataTypes[idx] = this.generateStorageClause(columnDataTypes[idx],sizeConstraints[idx])
	  insertOperators.push(this.getInsertOperator(targetDataType))
  	  return `"${columnName}" ${columnDataTypes[idx]}`    
    })
   
    this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
	    
    const tableInfo =  { 
      ddl              : tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml              : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators)
    , columnNames      : tableMetadata.columnNames
    , targetDataTypes  : targetDataTypes
	, insertOperators  : insertOperators
	, sizeConstraints  : sizeConstraints
    , insertMode       : insertMode
    , _BATCH_SIZE      : batchSize
    , _SPATIAL_FORMAT  : this.SPATIAL_FORMAT
    }
    
	return tableInfo
  }
  
}

export { CockroachStatementGenerator as default }