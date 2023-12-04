
import YadamuDataTypes           from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator  from '../base/yadamuStatementGenerator.js'

class DB2StatementGenerator extends YadamuStatementGenerator{
  
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
    
  getInsertOperator(targetDataType,storageClause) {
	  
	const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType)
	switch (dataTypeDefinition.type) {
	  case this.dbi.DATA_TYPES.BIGINT_TYPE:
	  case this.dbi.DATA_TYPES.NUMERIC_TYPE:
	  case this.dbi.DATA_TYPES.DECIMAL_TYPE:
	  case this.dbi.DATA_TYPES.FLOAT_TYPE:
	  case this.dbi.DATA_TYPES.DOUBLE_TYPE:
	    return `cast( ? as ${storageClause})`
	  case this.dbi.DATA_TYPES.IBMDB2_DECFLOAT_TYPE:
	    return `cast( ? as ${dataTypeDefinition.type})`
	  case this.dbi.DATA_TYPES.JSON_TYPE:
	    return 'SYSTOOLS.JSON2BSON(?)' 
	  case this.dbi.DATA_TYPES.XML_TYPE:
	    return 'XMLPARSE(DOCUMENT CAST(? AS BLOB) PRESERVE WHITESPACE)' 
   	  case this.dbi.DATA_TYPES.SPATIAL_TYPE:
	    switch (this.SPATIAL_FORMAT) {
		  case 'WKB':
    	  case 'EWKB':
		  case 'WKT':
		  case 'EWKT':
    	    return '?'
		  case 'GeoJSON':
    	    return 'SYSTOOLS.JSON2BSON(?)' 
		}
	  default:
	    return '?'
	}
  }
  
  
  getColumnConstraint(columnName,targetDataType) {

	const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType)
	
	switch (dataTypeDefinition.type) {
	  case this.dbi.DATA_TYPES.JSON_TYPE:
	    return ` check(SYSTOOLS.BSON_VALIDATE("${columnName}")  = 1) NOT ENFORCED`
	  case this.dbi.DATA_TYPES.SPATIAL_TYPE:
	    switch (this.dbi.SPATIAL_FORMAT) {
		  case 'WKT':
    	    return ` check(YADAMU.IS_WKT("${columnName}")) NOT ENFORCED`
		  case 'WKB':
    	    return ` check(YADAMU.IS_WKB("${columnName}")) NOT ENFORCED`
		  case 'GeoJSON':
    	    return ` check(YADAMU.IS_GeoJSON("${columnName}")) NOT ENFORCED`
		}
	  default:
	    return ''
	}
  }

  refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint) {

	 /*
	 **
	 ** Adjust data type based on size constraints
	 **
	 */

     // console.log(sourceDataType,targetDataType,sizeConstraint)

     switch (targetDataType) {
	   case this.dbi.DATA_TYPES.DATE_TYPE:
	   case this.dbi.DATA_TYPES.TIME_TYPE:
	     if (sizeConstraint[0] > 0 ) {
		   return this.dbi.DATA_TYPES.TIMESTAMP_TYPE
		 }
		 break
	 }
	 
	 return super.refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint)

  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';
    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)
   
    const insertOperators = []
	const columnConstraints = []
    
	const targetDataTypes = this.getTargetDataTypes(tableMetadata)
	const columnDataTypes = [...targetDataTypes]
	const sizeConstraints = [...tableMetadata.sizeConstraints]
	
	// this.debugStatementGenerator(null,null)
	
    const columnDefinitions = targetDataTypes.map((targetDataType,idx) => {		
	  switch (targetDataType) {
     	case this.dbi.DATA_TYPES.CLOB_TYPE:
		  sizeConstraints[idx] = ((sizeConstraints[idx].length === 0) || (sizeConstraints[idx][0] > this.dbi.DATA_TYPES.CLOB_LENGTH)) ? [this.dbi.DATA_TYPES.CLOB_LENGTH] : sizeConstraints[idx]
		  break;
     	case this.dbi.DATA_TYPES.BLOB_TYPE:
		  sizeConstraints[idx] = ((sizeConstraints[idx].length === 0) || (sizeConstraints[idx][0] > this.dbi.DATA_TYPES.BLOB_LENGTH)) ? [this.dbi.DATA_TYPES.BLOB_LENGTH] : sizeConstraints[idx]
		  break;
     	case this.dbi.DATA_TYPES.NCLOB_TYPE:
		  sizeConstraints[idx] = ((sizeConstraints[idx].length === 0) || (sizeConstraints[idx][0] > this.dbi.DATA_TYPES.NCLOB_LENGTH)) ? [this.dbi.DATA_TYPES.NCLOB_LENGTH] : sizeConstraints[idx]
		  break
     	case this.dbi.DATA_TYPES.JSON_TYPE:
           columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.JSON_TYPE
		   // Store BSON as Unconstrained BLOBS
		   sizeConstraints[idx]= []
		   break
     	case this.dbi.DATA_TYPES.CHAR_TYPE:
     	case this.dbi.DATA_TYPES.VARCHAR_TYPE:
     	case this.dbi.DATA_TYPES.NCHAR_TYPE:
     	case this.dbi.DATA_TYPES.NVARCHAR_TYPE:
		   // GRAPHIC and VARGRAPHIC appear to be length in bytes, not characters...
		   sizeConstraints[idx][0] = (this.dbi.DATABASE_VENDOR === this.SOURCE_VENDOR) ? sizeConstraints[idx][0] : Math.ceil(sizeConstraints[idx][0] * this.dbi.BYTE_TO_CHAR_RATIO)
		   // TODO ### Check if Max Size for Type is exceeded and Change Type.
		   break
     	case this.dbi.DATA_TYPES.SPATIAL_TYPE:
		  if (!this.dbi.DB2GSE_INSTALLED) {
			switch (this.SPATIAL_FORMAT) {
			  case 'WKB':
			  case 'EWKB':
			    columnDataTypes[idx] = this.dbi.DATA_TYPES.BLOB_TYPE
		        sizeConstraints[idx] = [this.dbi.DATA_TYPES.BLOB_LENGTH]
				break;
			  case 'WKT':
			  case 'EWKT':
			    columnDataTypes[idx] = this.dbi.DATA_TYPES.NCLOB_TYPE
		        sizeConstraints[idx] = [this.dbi.DATA_TYPES.NCLOB_LENGTH]
				break;
			  case 'GeoJSON':
			    columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.JSON_TYPE
				sizeConstraints[idx] = [this.dbi.DATA_TYPES.BLOB_LENGTH]
				break;
			}
			break;
		  }
		default:
	  }
	  const storageClause = this.generateStorageClause(columnDataTypes[idx],sizeConstraints[idx])
	  insertOperators.push(this.getInsertOperator(targetDataTypes[idx],storageClause))
	  columnConstraints.push(this.getColumnConstraint(tableMetadata.columnNames[idx],targetDataType))
      return `"${tableMetadata.columnNames[idx]}" ${storageClause}${columnConstraints[idx]}`    
    })
	
    this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
	    
    const tableInfo =  { 
      ddl              : tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml              : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators)
	
	

    , columnNames      : tableMetadata.columnNames
    , targetDataTypes  : targetDataTypes
	, sizeConstraints  : tableMetadata.sizeConstraints
    , insertMode       : insertMode
    , _BATCH_SIZE      : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT  : this.SPATIAL_FORMAT
    }
	
	return tableInfo
  }

}


export { DB2StatementGenerator as default }