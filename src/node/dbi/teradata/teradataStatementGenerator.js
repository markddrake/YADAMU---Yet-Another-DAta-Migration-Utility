
import YadamuDataTypes           from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator  from '../base/yadamuStatementGenerator.js'

class TeradataStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
	
  }
  
  generateDDLStatement(schema,tableName,columnDefinitions,targetDataTypes) {
	
    const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataTypes[0])

	const npiClause = (() => {
	  switch (dataTypeDefinition.type) {
	    case this.dbi.DATA_TYPES.CLOB_TYPE:
	    case this.dbi.DATA_TYPES.BLOB_TYPE:
	    case this.dbi.DATA_TYPES.XML_TYPE:
	    case this.dbi.DATA_TYPES.JSON_TYPE:
        case this.dbi.DATA_TYPES.SPATIAL_TYPE:
	   	  return ' NO PRIMARY INDEX' 
		default:
		  return ''
	  }
	})()
	  
    return`create multiset table "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})${npiClause}`;
	
  }
  
  refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint) {

    const dataType = super.refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint)
      
    switch (dataType) {
      case this.dbi.DATA_TYPES.CLOB_TYPE:
        return ((sizeConstraint.length > 0) && (sizeConstraint[0] > 0)) ? `${dataType}(${sizeConstraint[0] > this.dbi.DATA_TYPES.CLOB_LENGTH ? this.dbi.DATA_TYPES.CLOB_LENGTH : sizeConstraint[0]})` : dataType
      case this.dbi.DATA_TYPES.BLOB_TYPE:
        return ((sizeConstraint.length > 0) && (sizeConstraint[0] > 0)) ? `${dataType}(${sizeConstraint[0] > this.dbi.DATA_TYPES.BLOB_LENGTH ? this.dbi.DATA_TYPES.BLOB_LENGTH : sizeConstraint[0]})` : dataType
	  case this.dbi.DATA_TYPES.NUMERIC_TYPE:
	  case this.dbi.DATA_TYPES.DECIMAL_TYPE:
	    return (sizeConstraint.length === 0) ? this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE : dataType
      default:
        return dataType
    }
  }

  getInsertOperator(targetDataType) {

    switch (targetDataType) {
	  case this.dbi.DATA_TYPES.JSON_TYPE:
		return 'NEW JSON(?, UNICODE)'
  	  case this.dbi.DATA_TYPES.XML_TYPE:
		return 'NEW XML(?)'
      case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
		return 'NEW ST_GEOMETRY(?)'
	  default:
		return '?'
	}		   
  }
  
  translateDataType(dataType) {
    const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType)
    dataTypeDefinition.type = this.dbi.DATA_TYPES.sqlDataTypeNames[dataTypeDefinition.type] || dataTypeDefinition.type 
	return YadamuDataTypes.recomposeDataType(dataTypeDefinition)
  }	  
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';
    let batchSize = this.dbi.BATCH_SIZE
    
    const insertOperators = []
    
	const targetDataTypes = this.getTargetDataTypes(tableMetadata)
	const columnDataTypes = targetDataTypes.map((targetDataType)  => {
	  return (targetDataType === this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE) ? targetDataType : this.translateDataType(targetDataType)
	})
	
	// this.debugStatementGenerator(null,null)

    const columnDefinitions = targetDataTypes.map((targetDataType,idx) => {		

      let characterSetClause = ''
      
      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType)  	  
	  const sourceDataType = tableMetadata.dataTypes[idx]
      const columnName = tableMetadata.columnNames[idx]
	  
	  switch (dataTypeDefinition.type) {
        case this.dbi.DATA_TYPES.CHAR_TYPE:
        case this.dbi.DATA_TYPES.VARCHAR_TYPE:
        case this.dbi.DATA_TYPES.CLOB_TYPE:
          switch (true) {    
            case ((sourceDataType === this.SOURCE_DATA_TYPES.NCHAR_TYPE)    && (this.SOURCE_DATA_TYPES.CHAR_TYPE    !== this.SOURCE_DATA_TYPES.NCHAR_TYPE)):
            case ((sourceDataType === this.SOURCE_DATA_TYPES.NVARCHAR_TYPE) && (this.SOURCE_DATA_TYPES.VARCHAR_TYPE !== this.SOURCE_DATA_TYPES.NVARCHAR_TYPE)):
            case ((sourceDataType === this.SOURCE_DATA_TYPES.NCLOB_TYPE)    && (this.SOURCE_DATA_TYPES.CLOB_TYPE    !== this.SOURCE_DATA_TYPES.NCLOB_TYPE)):
              characterSetClause = 'CHARACTER SET UNICODE';
         }
	     break
	   case this.dbi.DATA_TYPES.JSON_TYPE:
	     batchSize = 1000
         characterSetClause = 'CHARACTER SET UNICODE';
         break;	   
	   case this.dbi.DATA_TYPES.SPATIAL_TYPE:
	     batchSize = 1000
         break;	   
       case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
         columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.BOOLEAN_TYPE
	     break 
       case this.dbi.DATA_TYPES.YEAR_TYPE:
         columnDataTypes[idx] = dataTypeDefinition.length === undefined ? `${columnDataTypes[idx]}(4)` : columnDataTypes[idx]
	     break 
	  }
	  
	  columnDataTypes[idx] = this.generateStorageClause(columnDataTypes[idx],tableMetadata.sizeConstraints[idx])
	  insertOperators.push(this.getInsertOperator(targetDataType))
  	  return `"${columnName}" ${columnDataTypes[idx]} ${characterSetClause}`    
    })
   
    this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
	    
    const tableInfo =  { 
      ddl              : tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml              : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators)
    , columnNames      : tableMetadata.columnNames
    , targetDataTypes  : targetDataTypes
    , insertMode       : insertMode
    , _BATCH_SIZE      : batchSize
    , _SPATIAL_FORMAT  : this.dbi.INBOUND_SPATIAL_FORMAT
    }
    
	return tableInfo
  }
  
}

export { TeradataStatementGenerator as default }