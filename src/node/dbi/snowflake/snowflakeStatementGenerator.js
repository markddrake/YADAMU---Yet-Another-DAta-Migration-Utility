
import path                     from 'path';

import YadamuLibrary            from '../../lib/yadamuLibrary.js';

import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

import SnowflakeConstants       from './snowflakeConstants.js';

class SnowflakeStatementGenerator extends YadamuStatementGenerator {

 static get UNBOUNDED_TYPES() { 
    StatementGenerator._UNBOUNDED_TYPES = StatementGenerator._UNBOUNDED_TYPES || Object.freeze([SnowflakeConstants.VARIANT_DATA_TYPE,'GEOGRAPHY','DOUBLE','FLOAT','BOOLEAN'])
    return this._UNBOUNDED_TYPES;
  }

  static get SPATIAL_TYPES() { 
    StatementGenerator._SPATIAL_TYPES = StatementGenerator._SPATIAL_TYPES || Object.freeze(['GEOMETRY'])
    return this._SPATIAL_TYPES;
  }

  static get INTEGER_TYPES() { 
    StatementGenerator._INTEGER_TYPES = StatementGenerator._INTEGER_TYPES || Object.freeze(['TINYINT','MEDIUMINT','SMALLINT','INT','BIGINT'])
    return this._INTEGER_TYPES;
  }
  
  static get STRONGLY_TYPED_VARIANTS() { 
    StatementGenerator._STRONGLY_TYPED_VARIANTS = StatementGenerator._STRONGLY_TYPED_VARIANTS || Object.freeze(['XML','XMLTYPE','JSON','JSONB','SET','OBJECT','ARRAY'])
    return this._STRONGLY_TYPED_VARIANTS;
  }
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {	  
    return `create ${this.dbi.TRANSIENT_TABLES ? 'transient ' : ''}table if not exists "%%YADAMU_DATABASE%%"."${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')}) ${this.dbi.DATA_RETENTION_TIME !== undefined ? `DATA_RETENTION_TIME_IN_DAYS=${this.dbi.DATA_RETENTION_TIME}` : ''} `
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators,parserRequired) {
    let insertStatement
    const valuesBlock = `(${columnNames.map((dataType,idx) => {return '?'}).join(',')})`

    if (parserRequired) {
      // Cannot pass JSON or XML (There is no JavaScript XML object) directly to an insert
      // Cannot pass strings (Expression type does not match column data type, expecting VARIANT but got VARCHAR(236) for column data',)
      // Cannot use JSON_PARSE or XML_PARSE directly in the bind list.
      // Array Binds are not supported with simple insert ... select ?, JSON_PARSE(?) (QL compilation error: Array bind currently not supported for this query type)
      
      // Benoit Dageville's solution using "INSERT ... SELECT JSON_PARSE() FROM VALUES (?,?,...),..."
             
      insertStatement = `insert into "${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."${tableName}" ("${tableMetadata.columnNames.join('","')}") select ${insertOperators.join(',')} from values `
			
      // Batch needs to consist of a single array of values rather than an Array of Arrays when the table contains a VARIANT column
      // Bind list is added at execution time since the full bind list is a function of the number of rows in the batch being inserted. 
      
    } 
    else {
      insertStatement = `insert into "${this.dbi.parameters.YADAMU_DATABASE}"."${this.targetSchema}"."${tableMetadata.tableName}" ("${columnNames.join('","')}") values ${valuesBlock}`;
    }
    return insertStatement
  }

  generateCopyOperation(schema,tableName,datafile) {
      
    /*
    let copyOperation 
    if (tableMetadata.dataFile) {
      copyOperation = `copy into "${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."${tableName}" from '@"${this.dbi.parameters.YADAMU_DATABASE}"."${schema}"."YADAMU_STAGE"/${path.relative(this.dbi.REMOTE_STAGING_AREA,.dataFile).split(path.sep).join(path.posix.sep)}' ON_ERROR = SKIP_FILE_${this.dbi.TABLE_MAX_ERRORS}`
	}
    */
	
    let copyOperation
	if (dataFile) {
	  if (Array.isArray(datafile)) {
		const partitionCount = datafile.length
        copyOperation = tableMetadata.dataFile.map((dataFile,idx) => {
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
    const columnNames = tableMetadata.columnNames
    const dataTypes = tableMetadata.dataTypes
    const sizeConstraints = tableMetadata.sizeConstraints
	
	// Fill with Column Numbers (1..n)
    const insertOperators = Object.keys(new Array(dataTypes.length).fill(null)).map((idx) => {return(`COLUMN${parseInt(idx)+1}`)})
	const columnClause = new Array(dataTypes.length).fill('')
    const targetDataTypes = [];
	
	const columnDefinitions = columnNames.map((columnName,idx) => {    

       // If the 'class' of a VARIANT datatype cannot be determned by insepecting the information available from Snowflake type it based on the incoming data stream 
       
       if ((dataTypes[idx] === SnowflakeConstants.VARIANT_DATA_TYPE) && tableMetadata.source) {
         if (StatementGenerator.STRONGLY_TYPED_VARIANTS.includes(tableMetadata.source.dataTypes[idx]?.toUpperCase())) {
           dataTypes[idx] = tableMetadata.source.dataTypes[idx]
         }
       }
        
       const dataType = {
         type : dataTypes[idx]
       }
	  
       const sizeConstraint = sizeConstraints[idx]
       if ((sizeConstraint !== null) && (sizeConstraint.length > 0)) {
          const components = sizeConstraint.split(',');
          dataType.length = parseInt(components[0])
          if (components.length > 1) {
            dataType.scale = parseInt(components[1])
          }
       }
           
       const mappedDataType = tableMetadata.source ? tableMetadata.dataTypes[idx] : this.getMappedDataType(tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx])
	   
       switch (mappedDataType) {
		 case this.dbi.DATA_TYPES.YADAMU_JSON_TYPE:
           parserRequired =  true;
           insertOperators[idx] = `TRY_PARSE_JSON(${insertOperators[idx]})`
		   break
		 case this.dbi.DATA_TYPES.YADAMU_XML_TYPE:
		   switch (true) {
   	         case (this.dbi.SNOWFLAKE_XML_TYPE === SnowflakeConstants.SNOWFLAKE_XML_TYPE): 
		       insertOperators[idx] = `PARSE_XML("${insertOperators[idx]}")`
			   columnClause[idx] = ''
			   break
		     default:
		       insertOperators[idx] = `case when check_xml("${insertOperators[idx]}") is NULL then "${insertOperators[idx]}" else NULL end`
		       columnClause[idx] = `check((CHECK_XML("${columnNames[idx]}") is NULL)) COMMENT 'CHECK(CHECK_XML("${columnNames[idx]}") IS NULL)'`
		    } 
			break
		 case this.dbi.DATA_TYPES.SFLAKE_VARIANT_TYPE:
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
           targetDataTypes.push(targetDataType);
	   }
       return `"${columnName}" ${this.columnDataType(targetDataType,dataType.length,dataType.scale)} ${columnClause[idx]}`
    })
	
    const tableInfo = { 
      ddl             : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml             : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
	, copy            : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.datafile)
    , valuesBlock     : valuesBlock
    , columnNames     : columnNames
    , targetDataTypes : mappedDataTypes
    , insertMode      : 'Batch'
    , parserRequired  : parserRequired
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
  }
  
}

export { SnowflakeStatementGenerator as default }