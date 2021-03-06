"use strict";

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const SnowflakeConstants = require('./snowflakeConstants.js');

class StatementGenerator {

  static get LARGEST_VARCHAR_SIZE()    { return SnowflakeConstants.MAX_CHARACTER_SIZE }
  static get LARGEST_VARBINARY_SIZE()  { return SnowflakeConstants.MAX_BINARY_SIZE }
  static get LARGEST_VARCHAR_TYPE()    { return SnowflakeConstants.CLOB_TYPE }
  static get LARGEST_VARBINARY_TYPE()  { return SnowflakeConstants.BLOB_TYPE }

  /*
  **
  ** MySQL BIT Column truncates leading '0's 
  **
  
  static get LARGEST_BIT_TYPE()        { return 'varchar(64)'      }
  static get BIT_TYPE()                { return 'varchar'          }

  **
  */

  static get LARGEST_NUMERIC_TYPE()    { return 'NUMBER(38)' }
   
  static get LARGEST_BIT_TYPE()        { return 'bit(64)'          }
  static get BIT_TYPE()                { return 'bit'              }

  static get TINYINT_TYPE()            { return 'TINYINT'}
  static get SMALLINT_TYPE()           { return 'SMALLINT'}
  static get MEDIUMINT_TYPE()          { return 'INT'}
  static get INT_TYPE()                { return 'INTEGER'}
  static get BIGINT_TYPE()             { return 'BIGINT'}

									 
  static get BFILE_TYPE()              { return 'TEXT(2048)'    }
  static get ROWID_TYPE()              { return 'TEXT(32)'      }
  static get XML_TYPE()                { return SnowflakeConstants.XML_TYPE         }
  static get UUID_TYPE()               { return 'TEXT(36)'      }
  static get ENUM_TYPE()               { return 'TEXT(255)'     }
  static get INTERVAL_TYPE()           { return 'TEXT(16)'      }
  static get BOOLEAN_TYPE()            { return 'tinyint(1)'       }
  static get HIERARCHY_TYPE()          { return 'TEXT(4000)'    }
  static get ORACLE_NUMBERIC_TYPE()    { return 'NUMBER(38,19)'    }
  static get MSSQL_MONEY_TYPE()        { return 'NUMBER(19,4)'    }
  static get MSSQL_SMALL_MONEY_TYPE()  { return 'NUMBER(10,4)'    }
  static get MSSQL_ROWVERSION_TYPE()   { return 'BINARY(8)'        }
  static get PGSQL_MONEY_TYPE()        { return 'NUMBER(21,2)'    }
  static get PGSQL_NAME_TYPE()         { return 'TEXT(64)'      }
  static get PGSQL_SINGLE_CHAR_TYPE()  { return 'char(1)'          }
  static get PGSQL_NUMERIC_TYPE()      { return 'NUMBER(38,19)'    } 
  static get PGSQL_INTERVAL_TYPE()     { return 'VARCHAR2(16)'}
  static get ORACLE_NUMERIC_TYPE()     { return 'NUMBER'    } 
  static get INET_ADDR_TYPE()          { return 'TEXT(39)'      }
  static get MAC_ADDR_TYPE()           { return 'TEXT(23)'      }
  static get UNSIGNED_INT_TYPE()       { return 'NUMBER(10)'    }
  static get PGSQL_IDENTIFIER()        { return 'BINARY(4)'    }
  static get MYSQL_YEAR_TYPE()         { return 'NUMBER(4,0)'}
  static get MONGO_OBJECT_ID()         { return 'BINARY(12)'       }
  static get MONGO_UNKNOWN_TYPE()      { return 'TEXT(2048)'    }
  static get MONGO_REGEX_TYPE()        { return 'TEXT(2048)'    }
  
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
  
  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
  
   mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeScale) {

     switch (vendor) {
       case 'Oracle':
         switch (dataType) {
           case 'VARCHAR2':                                           return 'TEXT';
           case 'NVARCHAR2':                                          return 'TEXT';
           case 'NUMBER':                                             return dataTypeLength === undefined ? StatementGenerator.ORACLE_NUMERIC_TYPE : 'NUMBER';
           case 'BINARY_FLOAT':                                       return 'FLOAT';
           case 'BINARY_DOUBLE':                                      return 'FLOAT';
           case 'CLOB':                                               return SnowflakeConstants.CLOB_TYPE;
           case 'BLOB':                                               return SnowflakeConstants.BLOB_TYPE;
           case 'NCLOB':                                              return SnowflakeConstants.CLOB_TYPE;
           case 'XMLTYPE':                                            return this.dbi.XML_TYPE;
           case 'JSON':                                               return SnowflakeConstants.JSON_TYPE;
           case 'TIMESTAMP':                                          return 'DATETIME';
           case 'BFILE':                                              return StatementGenerator.BFILE_TYPE;
           case 'ROWID':                                              return StatementGenerator.ROWID_TYPE;
           case 'RAW':                                                return 'BINARY';
           case 'ANYDATA':                                            return SnowflakeConstants.CLOB_TYPE;
           case '"MDSYS"."SDO_GEOMETRY"':                             return 'GEOGRAPHY';
           default:
		     switch (true) {
               case (dataType.indexOf('LOCAL TIME ZONE') > -1):       return 'TIMESTAMP_LTZ'; 
               case (dataType.indexOf('TIME ZONE') > -1):             return 'TIMESTAMP_NTZ'; 
               case (dataType.indexOf('INTERVAL') === 0):             return StatementGenerator.INTERVAL_TYPE; 
               case (dataType.indexOf('XMLTYPE') > -1):               return SnowflakeConstants.XML_TYPE;
               case (dataType.indexOf('.') > -1):                     return SnowflakeConstants.CLOB_TYPE;
			   default:                                               return dataType.toUpperCase();
         	 }	 
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType.toLowerCase()) {
           case 'smallmoney':                                         return StatementGenerator.MSSQL_SMALL_MONEY_TYPE;
           case 'money':                                              return StatementGenerator.MSSQL_MONEY_TYPE;
           case 'real':                                               return 'FLOAT';
           case 'text':                                             
           case 'bit':                                                return 'BOOLEAN';
           case 'ntext':                                              return SnowflakeConstants.CLOB_TYPE;
           case 'image':                                              return SnowflakeConstants.BLOB_TYPE;
           case 'xml':                                                return this.dbi.XML_TYPE;
           case 'json':                                               return SnowflakeConstants.JSON_TYPE;
           case 'datetime':                                         
           case 'datetime2':                                          return 'TIMESTAMP_NTZ';
           case 'datetimeoffset':                                     return 'TIMESTAMP_NTZ';
           case 'geography':                                        
           case 'geometry':                                           return 'GEOGRAPHY';
           case 'varchar':                                          
             switch (true) {                                        
               case (dataTypeLength === -1):                          return SnowflakeConstants.CLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_CHARACTER_SIZE):   return SnowflakeConstants.CLOB_TYPE;
               default:                                               return 'TEXT';
             }                                                      
           case 'binary':                                            
           case 'varbinary':                                        
             switch (true) {                                         
               case (dataTypeLength === -1):                          return SnowflakeConstants.BLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_BINARY_SIZE):      return SnowflakeConstants.BLOB_TYPE;
               default:                                               return 'BINARY';
             }                                                      
           case 'uniqueidentifier':                                   return StatementGenerator.UUID_TYPE;
           case 'hierarchyid':                                        return StatementGenerator.HIERARCHY_TYPE;
           case 'rowversion':                                         return StatementGenerator.MSSQL_ROWVERSION_TYPE;
           default:                                                   return dataType.toUpperCase();
         }
         break;
       case 'Postgres':                            
         switch (dataType.toLowerCase()) {
           case 'character varying':       
           case 'character':
             switch (true) {
               case (dataTypeLength === undefined):                   return SnowflakeConstants.CLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_CHARACTER_SIZE):   return SnowflakeConstants.CLOB_TYPE;
               default:                                               return 'TEXT';
             }                                                       
		   case 'char':                                               return StatementGenerator.PGSQL_SINGLE_CHAR_TYPE;
		   case 'name':                                               return StatementGenerator.PGSQL_NAME_TYPE
		   case 'bpchar':                                             return 'nchar';
           case 'bytea':                                             
             switch (true) {                                         
               case (dataTypeLength === undefined):                   return SnowflakeConstants.BLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_BINARY_SIZE):      return SnowflakeConstants.BLOB_TYPE;
               default:                                               return 'BINARY';
             }                                                       
		   case 'decimal':
           case 'numeric':                                            return dataTypeLength === undefined ? StatementGenerator.PGSQL_NUMERIC_TYPE : 'NUMBER';
		   case 'money':                                              return StatementGenerator.PGSQL_MONEY_TYPE
           case 'time with time zone':
           case 'timestamp with time zone':                           return 'TIMESTAMP_LTZ';
           case 'timestamp':                                         
           case 'time without time zone':                            
           case 'timestamp without time zone':                        return 'TIMESTAMP_NTZ';
           case 'numeric':                                            return 'DECIMAL';
           case 'double precision':                                   return 'DOUBLE';
           case 'real':                                               return 'FLOAT';
           case 'integer':                                            return 'INT';
           case 'jsonb':                                             
           case 'json':                                               return SnowflakeConstants.JSON_TYPE;
           case 'xml':                                                return this.dbi.XML_TYPE;;     
           case 'text':                                               return SnowflakeConstants.CLOB_TYPE;
           case 'geometry':                                          
           case 'geography':                                             
           case 'point':                                                
           case 'lseg':                                             
           case 'path':                                                  
           case 'box':                                                
           case 'polygon':                                            return 'GEOGRAPHY';  
           case 'circle':                                             return this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE' ? SnowflakeConstants.JSON_TYPE : 'GEOGRAPHY';
           case 'line':                                               return SnowflakeConstants.JSON_TYPE;    
           case 'uuid':                                               return StatementGenerator.UUID_TYPE
		   case 'bit':
		   case 'bit varying':    
 		     switch (true) {
           //  case (dataTypeLength === undefined):                   return StatementGenerator.LARGEST_BIT_TYPE;
               case (dataTypeLength === undefined):                   return StatementGenerator.LARGEST_VARCHAR_TYPE;
			   case (dataTypeLength > 64):                            return 'TEXT';
           //  default:                                               return StatementGenerator.BIT_TYPE;
               default:                                               return 'TEXT'
			 }
		   case 'cidr':
		   case 'inet':                                               return StatementGenerator.INET_ADDR_TYPE
		   case 'macaddr':                                           
		   case 'macaddr8':                                           return StatementGenerator.MAC_ADDR_TYPE
		   case 'int4range':                                         
		   case 'int8range':                                         
		   case 'numrange':                                          
		   case 'tsrange':                                           
		   case 'tstzrange':                                         
		   case 'daterange':                                          return SnowflakeConstants.JSON_TYPE;
		   case 'tsvector':                                          
		   case 'gtsvector':                                          return SnowflakeConstants.JSON_TYPE;
		   case 'tsquery':                                            return StatementGenerator.LARGEST_VARCHAR_TYPE;
           case 'oid':                                               
		   case 'regcollation':                                      
		   case 'regclass':                                          
		   case 'regconfig':                                         
		   case 'regdictionary':                                     
		   case 'regnamespace':                                      
		   case 'regoper':                                           
		   case 'regoperator':                                       
		   case 'regproc':                                           
		   case 'regprocedure':                                      
		   case 'regrole':                                           
		   case 'regtype':                                            return StatementGenerator.UNSIGNED_INT_TYPE;
		   case 'tid':                                                
		   case 'xid':                                               
		   case 'cid':                                               
		   case 'txid_snapshot':                                      return StatementGenerator.PGSQL_IDENTIFIER;
		   case 'aclitem':                                           
		   case 'refcursor':                                          return SnowflakeConstants.JSON_TYPE;
           default:
		     switch (true) {
               case (dataType.indexOf('interval') === 0):             return StatementGenerator.INTERVAL_TYPE; 
               default:			                                      return dataType.toUpperCase();  
             }
         }
         break	 
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
           case 'mediumint':                                          return 'INT';
           case 'decimal':                                           
             switch (true) {
               case (dataTypeLength > 38 && dataTypeScale === 0):     return StatementGenerator.LARGEST_NUMERIC_TYPE
               case (dataTypeLength > 38 && dataTypeScale !==0 ):     return `${StatementGenerator.LARGEST_NUMERIC_TYPE.substr(0,StatementGenerator.LARGEST_NUMERIC_TYPE.length-1)},${Math.round(dataTypeScale*(38/dataTypeLength))})`;
               default:                                               return 'NUMBER'                                                      
             }
           case 'year':                                               return StatementGenerator.MYSQL_YEAR_TYPE;
           case 'longblob':                                          
           case 'mediumblob':                                         return SnowflakeConstants.BLOB_TYPE;
           case 'blob':                                               return 'BINARY(65535)';
		   case 'tinyblob':                                           return 'BINARY(256)';
           case 'longtext':                                           return SnowflakeConstants.CLOB_TYPE;
           case 'mediumtext':                                         return 'TEXT(16777215)'
           case 'text':                                               return 'TEXT(65535)'
           case 'tinytext':                                           return 'TEXT(256)'
		   case 'varchar':                                            return 'TEXT'
           case 'geometry':                                           return 'GEOGRAPHY';
           case 'set':                                                return SnowflakeConstants.JSON_TYPE;
           case 'enum':                                               return 'TEXT(512)';
           case 'json':                                               return SnowflakeConstants.JSON_TYPE;
           case 'xml':                                                return this.dbi.XML_TYPE;
		   case 'point':
		   case 'linestring':
		   case 'polygon':
		   case 'geometry':
		   case 'multipoint':
		   case 'multilinestring':
		   case 'multipolygon':
		   case 'geometrycollection':
		   case 'geomcollection':                                     return 'GEOGRAPHY';
		   default:                                                   return dataType.toUpperCase();
         }                                                           
         break;                                                      
       case 'SNOWFLAKE':                                             
         switch (dataType.toUpperCase()) {                           
           case 'JSON':                                               return SnowflakeConstants.JSON_TYPE;
           case 'SET':                                                return SnowflakeConstants.JSON_TYPE;
           case 'XML':                                                return SnowflakeConstants.XML_TYPE;
           case 'XMLTYPE':                                            return SnowflakeConstants.XML_TYPE;
           default:                                                   return dataType.toUpperCase();
         }
       case 'MongoDB':
         switch (dataType.toLowerCase()) {
		   case 'string':
		     switch(true) {
               case (dataTypeLength === undefined):                   return SnowflakeConstants.CLOB_TYPE;
               case (dataTypeLength > this.dbi.MAX_CHARACTER_SIZE):   return SnowflakeConstants.CLOB_TYPE;
               default:                                               return 'TEXT';
		     }  
           case 'int':                                                return StatementGenerator.INT_TYPE;
           case 'long':                                               return StatementGenerator.BIGINT_TYPE;
           case 'decimal':                                            return 'NUMBER';
           case 'bindata':                                            return 'BINARY';
		   case 'bool':                                               return 'BOOLEAN';
		   case 'date':                                               return 'TIMESTAMP_LTZ(3)';
		   case 'timestamp':                                          return 'TIMESTAMP_LTZ(9)';
           case 'objectid':                                           return StatementGenerator.MONGO_OBJECT_ID;
		   case 'array':                                            
           case 'object':                                             return SnowflakeConstants.JSON_TYPE;
           case 'null':                                               return StatementGenerator.MONGO_UNKNOWN_TYPE;
           case 'regex':                                              return StatementGenerator.MONGO_REGEX_TYPE;
           case 'javascript':                                         return SnowflakeConstants.CLOB_TYPE;
           case 'javascriptWithScope':                                return SnowflakeConstants.CLOB_TYPE;
           case 'minkey':                                             return SnowflakeConstants.JSON_TYPE;
           case 'maxKey':                                             return SnowflakeConstants.JSON_TYPE;
           case 'undefined':
		   case 'dbPointer':
		   case 'function':
		   case 'symbol':                                             return SnowflakeConstants.JSON_TYPE;
           // No data in the Mongo Collection
           case 'json':                                               return SnowflakeConstants.JSON_TYPE;
		   default:                                                   return dataType.toUpperCase();
         }                                                          
       default: 
         return dataType.toUpperCase();
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
   
     if ((targetDataType === 'NUMBER') && (length > 38)) {
       return targetDataType;     
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
	// Fill with Column Numbers (1..n)
    const selectList = Object.keys(new Array(dataTypes.length).fill(null)).map((idx) => {return(`COLUMN${parseInt(idx)+1}`)})
	const columnClause = new Array(dataTypes.length).fill('')
    const targetDataTypes = [];
	
	const columnClauses = columnNames.map((columnName,idx) => {    
        
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
           
       let targetDataType = this.mapForeignDataType(tableMetadata.vendor,tableMetadata.vendor === 'SNOWFLAKE' ? tableMetadata.storageTypes[idx] : dataType.type,dataType.length,dataType.scale);
	   
        if (targetDataType === 'VARIANT') {
         parserRequired =  true;
		 switch (dataType.type.toUpperCase()) {
		   case 'XML':
		   case 'XMLTYPE':
		     switch (true) {
			   case (this.dbi.XML_TYPE === SnowflakeConstants.XML_TYPE): 
			   case (tableMetadata.storageTypes[idx] === SnowflakeConstants.XML_TYPE):
			     selectList[idx] = `PARSE_XML("${selectList[idx]}")`
				 columnClause[idx] = ''
				 break
			   default:
			     selectList[idx] = `case when check_xml("${selectList[idx]}") is NULL then "${selectList[idx]}" else NULL end`
			     columnClause[idx] = `check((CHECK_XML("${columnNames[idx]}") is NULL)) COMMENT 'CHECK(CHECK_XML("${columnNames[idx]}") IS NULL)'`
		     } 
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
            
       targetDataTypes.push(targetDataType);
       return `"${columnName}" ${this.columnDataType(targetDataType,dataType.length,dataType.scale)} ${columnClause[idx]}`
    })
	
    const createStatement = `create ${this.dbi.TRANSIENT_TABLES ? 'transient ' : ''}table if not exists "%%YADAMU_DATABASE%%"."${this.targetSchema}"."${tableMetadata.tableName}"(\n  ${columnClauses.join(',')}) ${this.dbi.DATA_RETENTION_TIME !== undefined ? `DATA_RETENTION_TIME_IN_DAYS=${this.dbi.DATA_RETENTION_TIME}` : ''} `;

    let insertStatement
    const valuesBlock = `(${columnNames.map((dataType,idx) => {return '?'}).join(',')})`

    if (parserRequired) {
      // Cannot pass JSON or XML (There is no JavaScript XML object) directly to an insert
      // Cannot pass strings (Expression type does not match column data type, expecting VARIANT but got VARCHAR(236) for column data',)
      // Cannot use JSON_PARSE or XML_PARSE directly in the bind list.
      // Array Binds are not supported with simple insert ... select ?, JSON_PARSE(?) (QL compilation error: Array bind currently not supported for this query type)
      
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
       _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
  }
  
  async generateStatementCache() {
      
    const statementCache = {}
    const tables = Object.keys(this.metadata); 

    const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
      statementCache[this.metadata[table].tableName] = tableInfo;
      return tableInfo.ddl;
    })
    return statementCache;
  }
}

module.exports = StatementGenerator;