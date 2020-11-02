"use strict";

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const SnowflakeConstants = require('./snowflakeConstants.js');

class StatementGenerator {
  
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

  constructor(dbi, targetSchema, metadata, spatialFormat) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
  }
  
   mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeSize) {

     switch (vendor) {
       case 'Oracle':
         switch (dataType) {
           case 'VARCHAR2':                return 'VARCHAR';
           case 'NVARCHAR2':               return 'VARCHAR';
           case 'NUMBER':                  return 'NUMBER';
           case 'BINARY_FLOAT':            return 'FLOAT';
           case 'BINARY_DOUBLE':           return 'FLOAT';
           case 'CLOB':                    return SnowflakeConstants.CLOB_TYPE;
           case 'BLOB':                    return SnowflakeConstants.BLOB_TYPE;
           case 'NCLOB':                   return SnowflakeConstants.CLOB_TYPE;
           case 'XMLTYPE':                 return SnowflakeConstants.XML_TYPE;
           case 'JSON':                    return SnowflakeConstants.JSON_TYPE;
           case 'TIMESTAMP':               return 'datetime';
           case 'BFILE':                   return 'VARCHAR(2048)';
           case 'ROWID':                   return 'VARCHAR(32)';
           case 'RAW':                     return 'BINARY';
           case 'ROWID':                   return 'VARCHAR(32)';
           case 'ANYDATA':                 return 'VARCHAR(16777216)';
           case '"MDSYS"."SDO_GEOMETRY"':  return 'GEOGRAPHY';
           default :
             if (dataType.indexOf('LOCAL TIME ZONE') > -1) {
               return 'TIMESTAMP_LTZ'; 
             }
             if (dataType.indexOf('TIME ZONE') > -1) {
               return 'TIMESTAMP_NTZ'; 
             }
             if (dataType.indexOf('INTERVAL') === 0) {
               return 'VARCHAR(16)'; 
             }
             if (dataType.indexOf('XMLTYPE') > -1) { 
               return SnowflakeConstants.XML_TYPE;
             }
             if (dataType.indexOf('.') > -1) { 
               return SnowflakeConstants.CLOB_TYPE;
             }
             return dataType.toUpperCase();
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType) {
           case 'smallmoney':                                        return 'DECIMAL(10,4)';
           case 'money':                                             return 'DECIMAL(19,4)';
           case 'real':                                              return 'FLOAT';
           case 'text': 
           case 'bit':                                               return 'BOOLEAN';
           case 'ntext':                                             return SnowflakeConstants.CLOB_TYPE;
           case 'image':                                             return SnowflakeConstants.BLOB_TYPE;
           case 'xml':                                               return SnowflakeConstants.XML_TYPE;
           case 'json':                                              return SnowflakeConstants.JSON_TYPE;
           case 'datetime':
           case 'datetime2':                                         return 'TIMESTAMP_NTZ';
           case 'datetimeoffset':                                    return 'TIMESTAMP_NTZ';
           case 'geography':
           case 'geometry':                                          return 'GEOGRAPHY';
           case 'varchar': 
             switch (true) {
               case (dataTypeLength === -1):                         return SnowflakeConstants.CLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_CHARACTER_SIZE):  return SnowflakeConstants.CLOB_TYPE;
               default:                                              return 'TEXT';
             }
           case 'binary':                                           
           case 'varbinary':
             switch (true) {                                        
               case (dataTypeLength === -1):                         return SnowflakeConstants.BLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_BINARY_SIZE):     return SnowflakeConstants.BLOB_TYPE;
               default:                                              return 'BINARY';
             }
           case 'uniqueidentifier':                                  return 'VARCHAR(64)';
           case 'hierarchyid':                                       return 'VARCHAR(4000)';
           case 'rowversion':                                        return 'BINARY(8)';
           default:                                                  return dataType.toUpperCase();
         }
         break;
       case 'Postgres':                            
         switch (dataType) {
           case 'character varying':       
           case 'character':
             switch (true) {
               case (dataTypeLength === -1):                         return SnowflakeConstants.CLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_CHARACTER_SIZE):  return SnowflakeConstants.CLOB_TYPE;
               default:                                              return 'TEXT';
             }
           case 'bytea':
             switch (true) {                                        
               case (dataTypeLength === -1):                         return SnowflakeConstants.BLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_BINARY_SIZE):     return SnowflakeConstants.BLOB_TYPE;
               default:                                              return 'BINARY';
             }
	       case 'longtext':                                          return SnowflakeConstants.CLOB_TYPE;
           case 'timestamp with time zone':                          return 'TIMESTAMP_LTZ';
           case 'timestamp': 
           case 'time without time zone': 
           case 'timestamp without time zone':                       return 'TIMESTAMP_NTZ';
           case 'numeric':                                           return 'DECIMAL';
           case 'double precision':                                  return 'DOUBLE';
           case 'real':                                              return 'FLOAT';
           case 'integer':                                           return 'INT';
           case 'jsonb':
           case 'json':                                              return SnowflakeConstants.JSON_TYPE;
           case 'xml':                                               return SnowflakeConstants.XML_TYPE;     
           case 'text':                                              return SnowflakeConstants.CLOB_TYPE;
           case 'geometry':       
           case 'geography':                                         return 'GEOGRAPHY';     
           default:
             if (dataType.indexOf('interval') === 0) {
               return 'TEXT(16)'; 
             }
             return dataType.toUpperCase();
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
           case 'mediumint':                      return 'INT';
           case 'year':                           return 'NUMBER(4)';
           case 'longblob':                     
           case 'mediumblob':                     return SnowflakeConstants.BLOB_TYPE;
           case 'blob':                           return 'BINARY(65535)';
		   case 'tinyblob':                       return 'BINARY(256)';
           case 'longtext':                       return SnowflakeConstants.CLOB_TYPE;
           case 'mediumtext':                     return 'TEXT(16777215)'
           case 'text':                           return 'TEXT(65535)'
           case 'tinytext':                       return 'TEXT(256)'
		   case 'varchar':                        return 'TEXT'
           case 'geometry':                       return 'GEOGRAPHY';
           case 'set':                            return SnowflakeConstants.JSON_TYPE;
           case 'enum':                           return 'TEXT(512)';
           case 'json':                           return SnowflakeConstants.JSON_TYPE;
           case 'xml':                            return SnowflakeConstants.XML_TYPE;
           default:                               return dataType.toUpperCase();
         }
         break;
       case 'SNOWFLAKE':
         switch (dataType.toUpperCase()) {
           case 'JSON':                           return SnowflakeConstants.JSON_TYPE;
           case 'SET':                            return SnowflakeConstants.JSON_TYPE;
           case 'XML':                            return SnowflakeConstants.XML_TYPE;
           case 'XMLTYPE':                        return SnowflakeConstants.XML_TYPE;
           default:                               return dataType.toUpperCase();
         }
       case 'MongoDB':
         switch (dataType.toUpperCase()) {
           // Uncommnent following line to test error recovery. ### 2020-10-19 Causes Hang on exit during Parallel mode dbRoundtrip with HR dataset
           // case 'OBJECTID':                       return BINARY(12);
           case 'OBJECTID':                       return 'BINARY(12)';
		   case 'JSON':                           return SnowflakeConstants.JSON_TYPE;
    	   case 'SET':                            return SnowflakeConstants.JSON_TYPE;
           case 'XML':                            
           case 'XMLTYPE':                        return SnowflakeConstants.XML_TYPE;
		   case 'ARRAY':
           case 'OBJECT':                         return SnowflakeConstants.JSON_TYPE;
           case 'BINDATA':                        return 'BINARY';
		   case 'BOOL':                           return 'BOOLEAN';
		   case 'STRING':                         return 'TEXT';
           default:                               return dataType.toUpperCase();
         }
       default: 
         return dataType.toLowerCase();
    }  
  } 
  
  columnDataType(targetDataType, length, scale) {
  
     length = ((length > SnowflakeConstants.MAX_CHARACTER_SIZE) || (length < 0)) ? SnowflakeConstants.MAX_CHARACTER_SIZE : length
     length = ((targetDataType === 'VARBINARY') && (length > SnowflakeConstants.MAX_BINARY_SIZE)) ? SnowflakeConstants.MAX_BINARY_SIZE : length
     
     if (RegExp(/\(.*\)/).test(targetDataType)) {
       return targetDataType
     }
     
     if (StatementGenerator.INTEGER_TYPES.includes(targetDataType)) {
       return targetDataType
     }
       
     if (StatementGenerator.UNBOUNDED_TYPES.includes(targetDataType)) {
       return targetDataType
     }
  
  
     if (StatementGenerator.SPATIAL_TYPES.includes(targetDataType)) {
       return targetDataType
     }
  
     if (scale) {
       return targetDataType + '(' + length + ',' + scale + ')';
     }                                                   
  
     if (length) {
       return targetDataType + '(' + length + ')';
     }
  
     return targetDataType;     
  }
  
  generateTableInfo(tableMetadata) {
        
    let parserRequired = false;
    const columnNames = tableMetadata.columnNames
    const dataTypes = tableMetadata.dataTypes
    const sizeConstraints = tableMetadata.sizeConstraints
    const selectList = Object.keys(new Array(dataTypes.length).fill(null)).map((idx) => {return(`column${parseInt(idx)+1}`)})
    const targetDataTypes = [];

    const columnClauses = columnNames.map((columnName,idx) => {    
        
       // If the 'class' of a VARIANT datatype cannot be determned by insepecting the information available from Snowflake type it based on the incoming data stream 
       
       if ((dataTypes[idx] === SnowflakeConstants.VARIANT_DATA_TYPE) && tableMetadata.source) {
         if (StatementGenerator.STRONGLY_TYPED_VARIANTS.includes(tableMetadata.source.dataTypes[idx].toUpperCase())) {
           dataTypes[idx] = tableMetadata.source.dataTypes[idx]
         }
       }
        
       const dataType = {
         type : dataTypes[idx]
       }
       
       if ((StatementGenerator.STRONGLY_TYPED_VARIANTS.includes(dataType.type.toUpperCase())) || (dataType.type.toUpperCase() === 'VARIANT')) {
         parserRequired =  true;
		 switch (dataType.type.toUpperCase()) {
		   case 'XML':
		   case 'XMLTYPE':
		     selectList[idx] = `PARSE_XML(${selectList[idx]})` 
			 break
		   case 'JSON':
		   case 'JSONB':
		   case 'SET':
		   case 'OBJECT':
		   case 'ARRAY':
             selectList[idx] = `TRY_PARSE_JSON(${selectList[idx]})`
		     break;
		   default:
             selectList[idx] = `case 
			   when ${selectList[idx]} is NULL then   
                 NULL
              when CHECK_JSON(${selectList[idx]}) is NULL then 
                 PARSE_JSON(${selectList[idx]})			  
              when CHECK_XML(${selectList[idx]}) is NULL then 
                 PARSE_XML(${selectList[idx]})
			  else
				NULL
			 end`
		 }
       }
       
       const sizeConstraint = sizeConstraints[idx]
       if ((sizeConstraint !== null) && (sizeConstraint.length > 0)) {
          const components = sizeConstraint.split(',');
          dataType.length = parseInt(components[0])
          if (components.length > 1) {
            dataType.scale = parseInt(components[1])
          }
       }
           
       let targetDataType = this.mapForeignDataType(tableMetadata.vendor,dataType.type,dataType.length,dataType.scale);
      
       targetDataTypes.push(targetDataType);
       return `"${columnName}" ${this.columnDataType(targetDataType,dataType.length,dataType.scale)}`
    })
	
    const createStatement = `create ${this.dbi.TRANSIENT_TABLES ? 'transient ' : ''}table if not exists "%%YADAMU_DATABASE%%"."${this.targetSchema}"."${tableMetadata.tableName}"(\n  ${columnClauses.join(',')}) ${this.dbi.DATA_RETENTION_TIME !== undefined ? `DATA_RETENTION_TIME_IN_DAYS=${this.dbi.DATA_RETENTION_TIME}` : ''} `;

    let insertStatement
    const valuesBlock = `(${columnNames.map((dataType,idx) => {return '?'}).join(',')})`

    if (parserRequired) {
      // Cannot pass JSON or XML (There is no JavaScript XML object) directly to an insert
      // Cannot pass strings (Expression type does not match column data type, expecting VARIANT but got VARCHAR(236) for column data',)
      // Cannot use JSON_PARSE or XML_PARSE directly in the bind list.
      // Array Binds are no support with simple insert ... select ?, JSON_PARSE(?) (QL compilation error: Array bind currently not supported for this query type)
      
      // Benoit Dageville's solution using "INSERT ... SELECT JSON_PARSE() FROM VALUES (?,?,...),..."
             
      insertStatement = `insert into "${this.dbi.parameters.YADAMU_DATABASE}"."${this.targetSchema}"."${tableMetadata.tableName}" ("${tableMetadata.columnNames.join('","')}") select ${selectList.join(',')} from values `
			
      // Batch needs to consist of a single array of values rather than an Array of Arrays when the table contains a VARIANT column
      // Bind list is added at execution time since the full bind list is a function of the number of rows in the batch being inserted. 
      
    } 
    else {
      insertStatement = `insert into "${this.dbi.parameters.YADAMU_DATABASE}"."${this.targetSchema}"."${tableMetadata.tableName}" ("${columnNames.join('","')}") values ${valuesBlock}`;
    }

    return { 
       ddl             : createStatement, 
       dml             : insertStatement,
       valuesBlock     : valuesBlock,
       columnNames     : columnNames,     
       targetDataTypes : targetDataTypes, 
       insertMode      : 'Batch',
       parserRequired  : parserRequired,
       _BATCH_SIZE     : this.dbi.BATCH_SIZE,
       _COMMIT_COUNT   : this.dbi.COMMIT_COUNT,
       _SPATIAL_FORMAT : this.spatialFormat
    }
  }
  
  async generateStatementCache(executeDDL,vendor) {
      
    const statementCache = {}
    const tables = Object.keys(this.metadata); 

    const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
      statementCache[this.metadata[table].tableName] = tableInfo;
      return tableInfo.ddl;
    })
    if (executeDDL === true) {
      await this.dbi.executeDDL(ddlStatements)
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;