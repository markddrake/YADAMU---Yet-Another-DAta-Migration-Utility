
import path                     from 'path';

import YadamuLibrary            from '../../lib/yadamuLibrary.js';

import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

import SnowflakeConstants       from './snowflakeConstants.js';

class SnowflakeStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {	  
    return `create ${this.dbi.TRANSIENT_TABLES ? 'transient ' : ''}table if not exists "%%YADAMU_DATABASE%%"."${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')}) ${this.dbi.DATA_RETENTION_TIME !== undefined ? `DATA_RETENTION_TIME_IN_DAYS=${this.dbi.DATA_RETENTION_TIME}` : ''} `
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators,parserRequired) {
    
	// Cannot pass JSON or XML (There is no JavaScript XML object) directly to an insert
    // Cannot pass strings (Expression type does not match column data type, expecting VARIANT but got VARCHAR(236) for column data',)
    // Cannot use JSON_PARSE or XML_PARSE directly in the bind list.
    // Array Binds are not supported with simple insert ... select ?, JSON_PARSE(?) (QL compilation error: Array bind currently not supported for this query type)
      
    // Benoit Dageville's solution using "INSERT ... SELECT JSON_PARSE() FROM VALUES (?,?,...),..."
    
	return `insert into "${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."${tableName}" ("${columnNames.join('","')}") ${parserRequired ? `select ${insertOperators.join(',')} from` : ''} values `;
             
    // Batch needs to consist of a single array of values rather than an Array of Arrays when the table contains a VARIANT column
    // Bind list is added at execution time since the full bind list is a function of the number of rows in the batch being inserted. 
     
  }

  generateCopyOperation(schema,tableName,dataFile) {
      
    /*
    let copyOperation 
    if (tableMetadata.dataFile) {
      copyOperation = `copy into "${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."${tableName}" from '@"${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."YADAMU_STAGE"/${path.relative(this.dbi.REMOTE_STAGING_AREA,.dataFile).split(path.sep).join(path.posix.sep)}' ON_ERROR = SKIP_FILE_${this.dbi.TABLE_MAX_ERRORS}`
	}
    */

    let copyOperation
	if (dataFile) {
	  if (Array.isArray(dataFile)) {
		const partitionCount = dataFile.length
        copyOperation = dataFile.map((dataFile,idx) => {
	      return  {
	        dml             : `copy into "${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."${tableName}" from '@"${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."YADAMU_STAGE"/${path.relative(this.dbi.REMOTE_STAGING_AREA,dataFile).split(path.sep).join(path.posix.sep)}' ON_ERROR = SKIP_FILE_${this.dbi.TABLE_MAX_ERRORS}`
		  , partitionCount  : partitionCount
		  , partitionID     : idx+1
	      }
	    })
	  }
      else {
	    copyOperation = {
	     dml         : `copy into "${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."${tableName}" from '@"${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."YADAMU_STAGE"/${path.relative(this.dbi.REMOTE_STAGING_AREA,dataFile).split(path.sep).join(path.posix.sep)}' ON_ERROR = SKIP_FILE_${this.dbi.TABLE_MAX_ERRORS}`
	    }
	  }
    }
	return copyOperation
  }

  generateTableInfo(tableMetadata) {

    let parserRequired = false;
    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)

    const dataTypes = tableMetadata.dataTypes
	
	// Fill with Column Numbers (1..n)

    const insertOperators = Object.keys(new Array(dataTypes.length).fill(null)).map((idx) => {return(`COLUMN${parseInt(idx)+1}`)})
	const columnClause = new Array(dataTypes.length).fill('')
    
	const targetDataTypes = this.getTargetDataTypes(tableMetadata)
	const columnDataTypes = [...targetDataTypes]
	
	// this.debugStatementGenerator(null,null)

    const columnDefinitions = targetDataTypes.map((targetDataType,idx) => {		

       // If the 'class' of a VARIANT datatype cannot be determned by insepecting the information available from Snowflake type it based on the incoming data stream 
       
       if ((dataTypes[idx] === SnowflakeConstants.VARIANT_DATA_TYPE) && tableMetadata.source) {
         if (this.dbi.DATA_TYPES.STRONGLY_TYPED_VARIANTS.includes(tableMetadata.source.dataTypes[idx]?.toUpperCase())) {
           dataTypes[idx] = tableMetadata.source.dataTypes[idx].toUpperCase()
         }
       }
      
	   const columnName = tableMetadata.columnNames[idx]
	   
       switch (targetDataType) {
		 case this.dbi.DATA_TYPES.JSON_TYPE:
           parserRequired =  true;
           insertOperators[idx] = `TRY_PARSE_JSON(${insertOperators[idx]})`
		   columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.JSON_TYPE
		   break
		 case this.dbi.DATA_TYPES.XML_TYPE:
		   columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.XML_TYPE
		   switch (true) {
   	         case (columnDataTypes[idx] === SnowflakeConstants.VARIANT_DATA_TYPE):
		       parserRequired =  true;
               insertOperators[idx] = `PARSE_XML("${insertOperators[idx]}")`
			   columnClause[idx] = ''
			   break
		     default:
		       insertOperators[idx] = `case when check_xml("${insertOperators[idx]}") is NULL then "${insertOperators[idx]}" else NULL end`
		       columnClause[idx] = `check((CHECK_XML("${tableMetadata.columnNames[idx]}") is NULL)) COMMENT 'CHECK(CHECK_XML("${tableMetadata.columnNames[idx]}") IS NULL)'`
		    } 
			break
		  case this.dbi.DATA_TYPES.SNOWFLAKE_VARIANT_TYPE:
            parserRequired =  true;
            insertOperators[idx] = `case 
			  when ${insertOperators[idx]} is NULL then   
                NULL
              when CHECK_JSON(${insertOperators[idx]}) is NULL then 
                PARSE_JSON(${insertOperators[idx]})			  
              when CHECK_XML(${insertOperators[idx]}) is NULL then 
                PARSE_XML(${insertOperators[idx]})
			  else
				NULL
			 end`
	   }
	   return `"${columnName}" ${this.generateStorageClause(columnDataTypes[idx],tableMetadata.sizeConstraints[idx])} ${columnClause[idx]}`
    })

    const args = `(${columnDefinitions.map((col) => {return '?'}).join(',')})`
    
	this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
    
	const tableInfo = { 
      ddl             : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml             : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators,parserRequired)
	, copy            : this.generateCopyOperation(this.targetSchema,tableMetadata.tableName,tableMetadata.dataFile)
    , args            : args
    , columnNames     : tableMetadata.columnNames
    , targetDataTypes : targetDataTypes
    , insertMode      : 'Batch'
    , parserRequired  : parserRequired
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.SPATIAL_FORMAT
    }
	return tableInfo
  }
  
}

export { SnowflakeStatementGenerator as default }