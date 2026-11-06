
import YadamuDataTypes           from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator  from '../base/yadamuStatementGenerator.js'

import TeradataIntervals         from './teradataIntervals.js'

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
  
  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    // The partitions of a partitioned source table are written by concurrent sessions, and their row hash locks deadlock (Error 2631).
    // Locking the table for the whole transaction makes them wait for each other instead.
    return `LOCKING TABLE "${schema}"."${tableName}" FOR WRITE ${super.generateDMLStatement(schema,tableName,columnNames,insertOperators)}`
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
      case this.dbi.DATA_TYPES.TIME_TYPE:
      case this.dbi.DATA_TYPES.DATETIME_TYPE:
      case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        return targetDataType
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
	switch (dataTypeDefinition.type) {
	  case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
	    return `TIME${dataTypeDefinition.hasOwnProperty('length') ? `(${dataTypeDefinition.length})` : ``} WITH TIME ZONE`
	  case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
	    return `TIMESTAMP${dataTypeDefinition.hasOwnProperty('length') ? `(${dataTypeDefinition.length})` : ``} WITH TIME ZONE`
      default:
        dataTypeDefinition.type = this.dbi.DATA_TYPES.sqlDataTypeNames[dataTypeDefinition.type] || dataTypeDefinition.type 
		return YadamuDataTypes.recomposeDataType(dataTypeDefinition)
	}		   
  }	  
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';
    let batchSize = this.dbi.BATCH_SIZE
    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)
    
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
          // Source character data is Unicode, whatever its type, and an export from Teradata doesn't record the character set.
          // The VARCHAR and CLOB limits in teradataTypeMappings.json are the UNICODE limits.
          characterSetClause = 'CHARACTER SET UNICODE';
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
	  switch (dataTypeDefinition.type) {
	    case this.dbi.DATA_TYPES.TIME_TYPE:
	    case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
	    case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
	    case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
	      // generateStorageClause() ignores a precision of 0, and Teradata's default is 6
	      if ((tableMetadata.sizeConstraints[idx]?.[0] === 0) && !columnDataTypes[idx].includes('(')) {
	        const components = columnDataTypes[idx].split(' ')
	        columnDataTypes[idx] = [`${components.shift()}(0)`,...components].join(' ')
	      }
	  }
	  switch (true) {
	    case TeradataIntervals.isInterval(dataTypeDefinition.type):
	      columnDataTypes[idx] = TeradataIntervals.intervalDDL(dataTypeDefinition.type,tableMetadata.sizeConstraints[idx])
	      break
	    case TeradataIntervals.isPeriod(dataTypeDefinition.type):
	      columnDataTypes[idx] = TeradataIntervals.periodDDL(dataTypeDefinition.type,tableMetadata.sizeConstraints[idx])
	  }
	  insertOperators.push(this.getInsertOperator(targetDataType))
  	  return `"${columnName}" ${columnDataTypes[idx]} ${characterSetClause}`    
    })
   
    this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
	    
    const tableInfo =  { 
      ddl              : tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml              : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators)
    , columnNames      : tableMetadata.columnNames
    , targetDataTypes  : targetDataTypes
    , sizeConstraints  : tableMetadata.sizeConstraints
    , insertMode       : insertMode
    , _BATCH_SIZE      : batchSize
    , _SPATIAL_FORMAT  : this.SPATIAL_FORMAT
    }
    
	return tableInfo
  }
  
}

export { TeradataStatementGenerator as default }