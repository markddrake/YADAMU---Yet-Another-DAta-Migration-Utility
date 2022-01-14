"use strict";

const crypto = require('crypto');
const path = require('path');

const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {

  static get LARGEST_CHAR_SIZE()       { return 65000 }
  static get LARGEST_BINARY_SIZE()     { return 65000 }
  static get LARGEST_VARCHAR_SIZE()    { return 65000 }
  static get LARGEST_VARBINARY_SIZE()  { return 65000 }
  static get LARGEST_LOB_SIZE()        { return 32000000 }
  static get LARGEST_ROW_SIZE()        { return 32768000  }
  static get DEFAULT_SPATIAL_SIZE()    { return 1050000 }
  static get LARGEST_CHAR_TYPE()       { return `char(${StatementGenerator.LARGEST_CHAR_SIZE})`}
  static get LARGEST_BINARY_TYPE()     { return `binary(${StatementGenerator.LARGEST_BINARY_SIZE})`}
  static get LARGEST_VARCHAR_TYPE()    { return `varchar(${StatementGenerator.LARGEST_VARCHAR_SIZE})`}
  static get LARGEST_VARBINARY_TYPE()  { return `varbinary(${StatementGenerator.LARGEST_VARBINARY_SIZE})`}
  
  static get CLOB_TYPE()               { return `long varchar` }
  static get BLOB_TYPE()               { return `long varbinary` }
  static get MAX_CLOB_TYPE()           { return `long varchar(${StatementGenerator.LARGEST_LOB_SIZE})` }
  static get MAX_BLOB_TYPE()           { return `long varbinary(${StatementGenerator.LARGEST_LOB_SIZE})` }
  static get UNBOUNDED_NUMBER_TYPE()   { return 'decimal(65,30)' }
  
  static get ROWID_TYPE()              { return 'varchar(32)';      }
  static get XML_TYPE()                { return StatementGenerator.CLOB_TYPE }
  static get JSON_TYPE()               { return StatementGenerator.CLOB_TYPE }
  static get ENUM_TYPE()               { return 'varchar(255)';     }
  static get BFILE_TYPE()              { return 'varchar(2048)';    }
  static get HIERARCHY_TYPE()          { return 'varchar(4000)';    }
  static get MSSQL_MONEY_TYPE()        { return 'decimal(19,4)';    }
  static get MSSQL_SMALL_MONEY_TYPE()  { return 'decimal(10,4)';    }
  static get MSSQL_ROWVERSION_TYPE()   { return 'binary(8)';        }
  static get PGSQL_MONEY_TYPE()        { return 'decimal(21,2)';    }
  static get PGSQL_NAME_TYPE()         { return 'varchar(64)';      }
  static get PGSQL_SINGLE_CHAR_TYPE()  { return 'char(1)';          }
  static get PGSQL_NUMERIC_TYPE()      { return StatementGenerator.UNBOUNDED_NUMBER_TYPE }; 
  static get ORACLE_NUMERIC_TYPE()     { return StatementGenerator.UNBOUNDED_NUMBER_TYPE }; 
  static get MONGO_DECIMAL_TYPE()      { return StatementGenerator.UNBOUNDED_NUMBER_TYPE }; 
  static get INET_ADDR_TYPE()          { return 'varchar(39)';      }
  static get MAC_ADDR_TYPE()           { return 'varchar(23)';      }
  static get PGSQL_IDENTIFIER()        { return 'bigint';        }
  static get MYSQL_YEAR_TYPE()         { return 'smallint';         }
  static get MONGO_OBJECT_ID()         { return 'binary(12)';       }
  static get MONGO_UNKNOWN_TYPE()      { return 'varchar(2048)';    }
  static get MONGO_REGEX_TYPE()        { return 'varchar(2048)';    }
  static get C_UNTYPED_INTERVAL_TYPE() { return 'varchar(16)';    }

  static get UNBOUNDED_TYPES() { 
  StatementGenerator._UNBOUNDED_TYPES = StatementGenerator._UNBOUNDED_TYPES || Object.freeze(['date','time','datetime','timestamp','time with timezone','timestamp with timezone','float','int','tinyint','smallint','bigint','geometry','geography','interval day to second','interval year to month'])
    return StatementGenerator._UNBOUNDED_TYPES;
  }

  set TABLE_LOB_COUNT(v)        { this._TABLE_LOB_COUNT = v }
  set TABLE_UNUSED_BYTES(v)     { this._TABLE_UNUSED_BYTES = v }
  
  get TABLE_LOB_SIZE()          { 
    const  allocatedSize = Math.floor(this._TABLE_UNUSED_BYTES / (this._TABLE_LOB_COUNT || 1)) 
    return allocatedSize < StatementGenerator.LARGEST_LOB_SIZE ? allocatedSize : StatementGenerator.LARGEST_LOB_SIZE
  }
  
  
  get CLOB_TYPE()               { return `long varchar(${this.TABLE_LOB_SIZE})` }
  get BLOB_TYPE()               { return `long varbinary(${this.TABLE_LOB_SIZE})` }
  
  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
    
  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeScale) {
    switch (vendor) {
	   case "Vertica": 
         switch (dataType.toUpperCase()) {
           case 'JSON':                                                                  return StatementGenerator.JSON_TYPE;
           case 'XML':                                                                   return StatementGenerator.XML_TYPE;
           default:                                                                  	 return dataType.toLowerCase();
		 }
		 break;
       case 'Oracle':
         switch (dataType.toUpperCase()) {
           case 'VARCHAR2':                                                              return 'varchar';
           case 'NVARCHAR2':                                                             return 'varchar';
           case 'NUMBER':                                                                return dataTypeLength === undefined ? StatementGenerator.ORACLE_NUMERIC_TYPE : 'decimal';
           case 'BINARY_FLOAT':                                                          return 'float';
           case 'BINARY_DOUBLE':                                                         return 'float';
           case 'CLOB':                                                                  return StatementGenerator.CLOB_TYPE;
           case 'BLOB':                                                                  return StatementGenerator.BLOB_TYPE;
           case 'NCLOB':                                                                 return StatementGenerator.CLOB_TYPE;
           case 'XMLTYPE':                                                               return StatementGenerator.XML_TYPE;
           case 'TIMESTAMP':                                                             return dataTypeLength > 6 ? 'datetime' : 'datetime';
           case 'BFILE':                                                                 return StatementGenerator.BFILE_TYPE;
           case 'ROWID':                                                                 return StatementGenerator.ROWID_TYPE;
           case 'RAW':                                                                   return 'varbinary';
           case 'ANYDATA':                                                               return StatementGenerator.CLOB_TYPE;
           case 'JSON':                                                                  return StatementGenerator.JSON_TYPE;
           case '"MDSYS"."SDO_GEOMETRY"':                                                return 'geometry';
           case 'BOOLEAN':                                                               return 'boolean'
           default :
		     switch (true) {
               case (dataType.indexOf('INTERVAL') > -1):
   		         switch (true) {
                   case (dataType.indexOf('YEAR') > -1):                                 return 'interval year to month';
                   case (dataType.indexOf('DAY') > -1):                                  return 'interval day to second';
				   default:                                                              return 'interval year to month';
				 }                                                           
			   case (dataType.indexOf('TIME ZONE') > -1):                                return 'datetime'; 
               case (dataType.indexOf('XMLTYPE') > -1):                                  return StatementGenerator.XML_TYPE;
               case (dataType.indexOf('.') > -1):                                        return StatementGenerator.CLOB_TYPE;
               default:                                                                  return dataType.toLowerCase();
			 }
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType.toLowerCase()) {
           case 'varchar':
             switch (true) {
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar';
             }                                                                          
           case 'char':                                                                 
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):             return  tatementGenerator.CLOB_TYPE;
               default:                                                                  return 'char';
             }                                                                          
           case 'nvarchar':                                                             
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar';
             }                                                                          
           case 'nchar':                                                                
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):             return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'char';
             }
			 
           case 'text':                                                                  return StatementGenerator.CLOB_TYPE;                   
           case 'ntext':                                                                 return StatementGenerator.CLOB_TYPE;
           case 'binary':
             switch (true) {
               case (dataTypeLength === -1):                                             return StatementGenerator.BLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_BINARY_SIZE_SIZE):      return StatementGenerator.BLOB_TYPE;
               default:                                                                  return 'binary';
             }
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):                                             return StatementGenerator.BLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):   return StatementGenerator.BLOB_TYPE;
               default:                                                                  return 'varbinary';
             }
           case 'image':                                                                 return StatementGenerator.BLOB_TYPE;
           case 'boolean':                                                               return 'boolean'
           case 'tinyint':                                                               return 'smallint';
           case 'mediumint':                                                             return 'int';
           case 'money':                                                                 return StatementGenerator.MSSQL_MONEY_TYPE
           case 'smallmoney':                                                            return StatementGenerator.MSSQL_SMALL_MONEY_TYPE;
           case 'real':                                                                  return 'float';
           case 'bit':                                                                   return 'boolean'
           case 'datetime':                                                              return 'datetime';
           case 'time':                                                                  return 'datetime';
           case 'datetime2':                                                             return 'datetime';
           case 'datetimeoffset':                                                        return 'datetime';
           case 'smalldate':                                                             return 'datetime';
           case 'geography':                                                             return 'geography';
           case 'geometry':                                                              return 'geometry';
           case 'hierarchyid':                                                           return StatementGenerator.HIERARCHY_TYPE
           case 'rowversion':                                                            return 'binary(8)';
           case 'uniqueidentifier':                                                      return 'uuid';
           case 'json':                                                                  return StatementGenerator.JSON_TYPE;
           case 'xml':                                                                   return StatementGenerator.XML_TYPE;
           default:                                                                      return dataType.toLowerCase();
         }
         break;
       case 'Postgres':    
         switch (dataType.toLowerCase()) {
           case 'character varying':     
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):           return StatementGenerator.CLOB_TYPE;
               default:                                                                   return 'varchar';
             }
           case 'character':
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):              return StatementGenerator.CLOB_TYPE;
               default:                                                                   return 'char';
             }
		   case 'text':                                                                   return StatementGenerator.CLOB_TYPE;
		   case 'char':                                                                   return StatementGenerator.PGSQL_SINGLE_CHAR_TYPE;
		   case 'name':                                                                   return StatementGenerator.PGSQL_NAME_TYPE
		   case 'bpchar':                     
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):              return StatementGenerator.CLOB_TYPE;
               default:                                                                   return 'char';
             }
           case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.BLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):    return StatementGenerator.BLOB_TYPE;
               default:                                                                   return 'varbinary';
             }
		   case 'decimal':
           case 'numeric':                                                               return dataTypeLength === undefined ? StatementGenerator.PGSQL_NUMERIC_TYPE : 'decimal';
		   case 'money':                                                                 return StatementGenerator.PGSQL_MONEY_TYPE
           case 'integer':                                                               return 'int';
           case 'real':                                                                  return 'float';
           case 'double precision':                                                      return 'float';
           case 'boolean':                                                               return 'boolean'
           case 'timestamp':                                                             return 'timestamp'
           case 'timestamp with time zone':                                              return 'timestamp with timezone'                                 
           case 'timestamp without time zone':                                           return 'timestamp'
           case 'time with time zone':                                                   return 'time with timezone'
           case 'time without time zone':                                                return 'time';
		   case 'json':
           case 'jsonb':                                                                 return StatementGenerator.JSON_TYPE;
           case 'xml':                                                                   return StatementGenerator.XML_TYPE;
           case 'geography':                                                             return 'geography'; 
           case 'geometry':                                                             
           case 'point':                                                                 
           case 'lseg':                                                               
           case 'path':                                                                  
           case 'box':                                                                   
           case 'polygon':                                                               return 'geometry';     
           case 'circle':                                                                return this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE' ? StatementGenerator.JSON_TYPE : 'geometry';
           case 'line':                                                                  return StatementGenerator.JSON_TYPE;     
           case 'uuid':                                                                  return 'uuid'
		   case 'bit':
		   case 'bit varying':    
 		     switch (true) {
               case (dataTypeLength === undefined):                                      return StatementGenerator.LARGEST_VARCHAR_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar'
			 }
		   case 'cidr':
		   case 'inet':                                                                  return StatementGenerator.INET_ADDR_TYPE
		   case 'macaddr':                                                              
		   case 'macaddr8':                                                              return StatementGenerator.MAC_ADDR_TYPE
		   case 'int4range':                                                            
		   case 'int8range':                                                            
		   case 'numrange':                                                             
		   case 'tsrange':                                                              
		   case 'tstzrange':                                                            
		   case 'daterange':                                                             return StatementGenerator.JSON_TYPE;
		   case 'tsvector':                                                             
		   case 'gtsvector':                                                             return StatementGenerator.JSON_TYPE;
		   case 'tsquery':                                                               return StatementGenerator.LARGEST_VARCHAR_TYPE;
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
		   case 'regtype':                                                              return StatementGenerator.PGSQL_IDENTIFIER
		   case 'tid':                                                                  
		   case 'xid':                                                                  
		   case 'cid':                                                                  
		   case 'txid_snapshot':                                                        return StatementGenerator.PGSQL_IDENTIFIER;
		   case 'aclitem':                                                              
		   case 'refcursor':                                                            return StatementGenerator.JSON_TYPE;
           default :
		     switch (true) {
               case (dataType.indexOf('interval') > -1):
   		         switch (true) {
                   case (dataType.indexOf('year') > -1):                                return 'interval day to second'
                   case (dataType.indexOf('day') > -1):                                 return 'interval year to month'
				   default:                                                             return StatementGenerator.C_UNTYPED_INTERVAL_TYPE
				 }                                                           
               default:                                                                 return dataType.toLowerCase();
			 }
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
           case 'boolean':                                                             return 'boolean'
           case 'double':                                                              return 'float';
           case 'mediumint':                                                           return 'int'
           case 'longtext':                 
           case 'mediumtext':               
           case 'text':                                                                return StatementGenerator.CLOB_TYPE;
           case 'year':                                                                return StatementGenerator.MYSQL_YEAR_TYPE;
           case 'longblob':                 
           case 'mediumblob':                 
           case 'blob':                                                                return StatementGenerator.BLOB_TYPE;
           case 'json':                                                                return StatementGenerator.JSON_TYPE;
           case 'set':                                                                 return StatementGenerator.JSON_TYPE;
           case 'enum':                                                                return StatementGenerator.ENUM_TYPE
		   case 'point':
		   case 'linestring':
		   case 'polygon':
		   case 'multipoint':
		   case 'multilinestring':
		   case 'multipolygon':
		   case 'geometrycollection':
		   case 'geomcollection':      
		   case 'geometry':                                                            return 'geometry';
           case 'geography':                                                           return 'geography';
           default:                                                                    return dataType.toLowerCase();
         }
         break;
       case 'MongoDB':
         switch (dataType) {
           case "string":
		     switch (true) {
               case (dataTypeLength === undefined):                                    return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):        return StatementGenerator.CLOB_TYPE;
               default:                                                                return 'varchar';
             }
		   case "int":                                                                 return 'int';
		   case "long":                                                                return 'bigint';
		   case "double":                 	                                           return 'float';
		   case "decimal":              		                                       return StatementGenerator.MONGO_DECIMAL_TYPE;
		   case "binData":             		                                           return StatementGenerator.BLOB_TYPE;
		   case "bool":                                                                return 'boolean';
		   case "date":                                                                return 'datetime';
		   case "timestamp":		                                                   return 'datetime';
		   case "objectId":            		                                           return StatementGenerator.MONGO_OBJECT_ID
		   case "json":                                                            
		   case "object":                                                            
		   case "array":                                                               return StatementGenerator.JSON_TYPE;
           case "null":                                                                return StatementGenerator.MONGO_UNKNOWN_TYPE
           case "regex":                		                                       return StatementGenerator.MONGO_REGEX_TYPE
           case "javascript":		                                                   return StatementGenerator.CLOB_TYPE;
		   case "javascriptWithScope":    	                                           return StatementGenerator.CLOB_TYPE;
		   case "minkey":                                                            
		   case "maxkey":                                                              return StatementGenerator.JSON_TYPE;
		   case "undefined":                                                         
		   case 'dbPointer':                                                         
		   case 'function':                                                          
		   case 'symbol':                                                              return StatementGenerator.JSON_TYPE;
           default:                                                                    return dataType.toLowerCase();
		 }
		 break;
       case 'SNOWFLAKE':
         switch (dataType.toLowerCase()) {
		   case "number":		                                                       return 'decimal';
		   case "float":		                                                       return 'float';
		   case "geography":   	                                                       return 'geography';
		   case "text":                                                                return dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE ? StatementGenerator.CLOB_TYPE: 'varchar'; 
		   case "binary":                                                              return dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE ? StatementGenerator.BLOB_TYPE : 'varbinary'; 
           case 'json':                                                                return StatementGenerator.JSON_TYPE;
		   case "xml":       	                                                       return StatementGenerator.XML_TYPE
		   case "variant":                                                             return StatementGenerator.BLOB_TYPE;
		   case "timestamp_ltz":                                                    
		   case "timestamp_ntz":                                                       return 'datetime'; 
		   default:
             return dataType.toLowerCase();
	     }
	   default :
         return dataType.toLowerCase();
    }  
  }

  getColumnDataType(targetDataType, length, scale) {
	
    if (RegExp(/\(.*\)/).test(targetDataType)) {
      return targetDataType
    }
     
    if (StatementGenerator.UNBOUNDED_TYPES.includes(targetDataType)) {
      return targetDataType
    }
     	  
    if (scale) {
      return targetDataType + '(' + length + ',' + scale + ')';
    }                                                   
  
    if (length && (length > 0)) {	 
      return targetDataType + '(' + length + ')'
    }
	
    return targetDataType;     
  }
      
  generateTableInfo(tableMetadata) {
	  
    let insertMode = 'Copy';
	
    const columnNames = tableMetadata.columnNames
    const dataTypes = tableMetadata.dataTypes
    const sizeConstraints = tableMetadata.sizeConstraints

    const targetDataTypes = [];
    const copyColumnList = [];
	const insertOperators =[]

    const columnClauses = columnNames.map((columnName,idx) => {
	  const column_suffix = String(idx+1).padStart(3,"0");
	 
	  const dataType = YadamuLibrary.composeDataType(dataTypes[idx],sizeConstraints[idx])       
	  
	  let targetDataType = this.mapForeignDataType(tableMetadata.vendor,dataType.type,dataType.length,dataType.scale)
	  targetDataTypes.push(targetDataType)
	  
	  let targetLength = dataType.length
      
	  switch (targetDataType) {
		// Disable char to byte calculation for char as this leads to issues with blank padding.
        // case 'char':
		case 'varchar':
		case 'long varchar':
		  targetLength = tableMetadata.vendor === 'Vertica' ? targetLength : Math.ceil(targetLength * this.dbi.BYTE_TO_CHAR_RATIO);
		  if (targetLength > StatementGenerator.LARGEST_VARCHAR_SIZE) {
			targetDataType = 'long varchar';
		  }
		  if (targetLength > StatementGenerator.LARGEST_LOB_SIZE) {
			targetLength = undefined;
		  }
      }		
	  
      sizeConstraints[idx] = targetLength?.toString() || sizeConstraints[idx] ||  ''
	  
      const typeInfo = targetDataType.split('(')
	  const targetType = typeInfo[0].toUpperCase()
	  switch (targetType) {		
        case 'BINARY':
		case 'VARBINARY':
		case 'LONG VARBINARY':
		  let columnLength = typeInfo.length > 1 ? typeInfo[1].split(')')[0] : sizeConstraints[idx];
		  columnLength = ( isNaN(columnLength) || ((columnLength < 1) || (columnLength > StatementGenerator.LARGEST_LOB_SIZE))) ? StatementGenerator.LARGEST_LOB_SIZE : parseInt(columnLength)
		  let hexLength = columnLength * 2
		  hexLength = ((hexLength < 2) || (hexLength > StatementGenerator.LARGEST_LOB_SIZE)) ? StatementGenerator.LARGEST_LOB_SIZE : hexLength
		  switch (true) {
			 case (columnLength > StatementGenerator.LARGEST_BINARY_SIZE) :
			   // LONG VARBINARY
               // copyColumnList[idx] = `"${columnName}" FORMAT 'HEX'`
			   copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${hexLength}), "${columnName}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`
			   break;
			 case (hexLength > StatementGenerator.LARGEST_VARCHAR_SIZE) :
			   // VARBINARY > 32500 < 65000 - Use LONG VARCHAR for HEX can cast result to VARBINARY
               // copyColumnList[idx] = `"${columnName}" FORMAT 'HEX'`
			   copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${hexLength}), "${columnName}" as CAST(YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}") as ${targetType}(${columnLength}))`
			   break;
			 default:   
			   copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER varchar(${hexLength}), "${columnName}" as HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`
               // copyColumnList[idx] = `"${columnName}" FORMAT 'HEX'`
          }
   	      insertOperators[idx] = {
	        prefix  : 'X'
		  , suffix  : `::${targetType.toUpperCase()}(${columnLength})`
		  }
		  break
		case 'CIRCLE':
		  if (this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE') {
  	        copyColumnList.push(`"${columnName}"`)
		    break;
		  }
	    case 'GEOMETRY':
		  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
			case 'WKT':
			case 'EWKT':
            case 'GeoJSON':
    		  copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromText("YADAMU_COL_${column_suffix}")`
	  	      insertOperators[idx] = { 
    			prefix  : '('
	    	  , suffix  : ')'
		      }
		      break;
			case 'WKB':
			case 'EWKB':
		      copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromWKB(HEX_TO_BINARY("YADAMU_COL_${column_suffix}"))`
	  	      insertOperators[idx] = { 
   			    prefix  : 'ST_GeomFromWKB(HEX_TO_BINARY('
		      , suffix  : '))'
		      }
		      break;
			/*
            case 'GeoJSON':
		      copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromGeoJSON("YADAMU_COL_${column_suffix}")`
	  	      insertOperators[idx] = { 
   			    prefix  : 'ST_GeomFromGeoJSON('
		      , suffix  : ')'
		      }
		      break;
			*/
		  }
		  break;
	    case 'GEOGRAPHY':
		  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
			case 'WKT':
			case 'EWKT':
            case 'GeoJSON':
		      copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromText("YADAMU_COL_${column_suffix}")`
	  	      insertOperators[idx] = { 
   			    prefix  : 'ST_GeographyFromText('
		      , suffix  : ')'
		      }
		      break;
			case 'WKB':
			case 'EWKB':
		      copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromWKB(HEX_TO_BINARY("YADAMU_COL_${column_suffix}"))`
	  	      insertOperators[idx] = { 
   			    prefix  : 'ST_GeographyFromWKB(HEX_TO_BINARY('
		      , suffix  : '))'
		      }
		      break;
			/*
            case 'GeoJSON':
		      copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromGeoJSON("YADAMU_COL_${column_suffix}")`
	  	      insertOperators[idx] = { 
   			    prefix  : 'ST_GeographyFromGeoJSON('
		      , suffix  : ')'
		      }
		      break;
			*/
		  }
		  break;
		case 'TIME':
		  if (tableMetadata.hasOwnProperty('dataFile')) {
            copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast(TO_TIMESTAMP("YADAMU_COL_${column_suffix}",'YYYY-MM-DD"T"HH24:MI:SS.US') as TIME)`
		  }
		  else {
  		    switch (tableMetadata.vendor) {
		      case "Postgres":
			  case "MySQL":
			  case "MariaDB":
			  case "Vertica":
  		        copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast("YADAMU_COL_${column_suffix}" as TIME)`
			    break;
			  default:
		        copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast(TO_TIMESTAMP("YADAMU_COL_${column_suffix}",'YYYY-MM-DD"T"HH24:MI:SS.US') as TIME)`
		    }
		  }
	  	  insertOperators[idx] = { 
			prefix  : 'cast('
		  , suffix  : ' as TIME)'
		  }
		  break;
		case 'TIME WITH TIMEZONE':
		  copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast("YADAMU_COL_${column_suffix}" as TIME WITH TIME ZONE)`
	  	  insertOperators[idx] = { 
			prefix  : 'cast('
		  , suffix  : ' as TIME WITH TIME ZONE)'
		  }
		  break;
		case 'INTERVAL DAY':
		case 'INTERVAL DAY TO SECOND':
		  copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS INTERVAL DAY TO SECOND)`
	  	  insertOperators[idx] = { 
			prefix  : 'cast('
		  , suffix  : ' as INTERVAL DAY TO SECOND)'
		  }
		  break;
		case 'INTERVAL YEAR':
		case 'INTERVAL YEAR TO MONTH':
		  copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS INTERVAL YEAR TO MONTH)`
	  	  insertOperators[idx] = { 
			prefix  : 'cast('
		  , suffix  : ' as INTERVAL YEAR TO MONTH)'
		  }
		  break;
		case 'UUID':
		  copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS UUID)`
	  	  insertOperators[idx] = null
		  break;
	    default:
		  copyColumnList[idx] =`"${columnName}"`
	  	  insertOperators[idx] = null
	  }
	  return `"${columnName}" ${this.getColumnDataType(targetDataType,targetLength,dataType.scale)}`
    })
                                   
    const args = dataTypes.map((dataType,idx) => {
      return '?';
    })

    let bytesUsed = 128
	let lobList = []

    targetDataTypes.forEach((targetDataType,idx) => {
	  switch (targetDataType) {
		case 'boolean':
		  bytesUsed+=1;
		  break;
		case 'date':
		case 'time':
		case 'time with timezone':
	    case 'datetime':
		case 'smalldatetime':
		case 'timestamp':
		case 'timestamp with timezone':
		case 'interval':
		case 'interval day to second':
		case 'interval year to month':
		case 'double precision':
		case 'float':
		case 'float8':
		case 'real':
		  bytesUsed+=8
		  break;
        case 'integer': 
        case 'int': 
        case 'bigint': 
        case 'imt8': 
        case 'smallint': 
        case 'tinyint': 
        case 'integer': 
		  bytesUsed+=8
		  break;
		case 'decimal':
		case 'numeric':
		case 'number':
		case 'money':
		  let precision = parseInt(sizeConstraints[idx].split()[0])
		  precision = precision - 18
      	  bytesUsed+=8
		  while (precision > 0) {
         	bytesUsed+=8
		    precision = precision - 18
	      }
		  break;
		case 'geometry':
		case 'geography':
		  bytesUsed+=StatementGenerator.DEFAULT_SPATIAL_SIZE
		  break;
		case 'uuid':
      	  bytesUsed+=16
		  break;
		case 'binary':
		case 'varbinary':
		case 'char':
		case 'varchar':
		  bytesUsed+= parseInt(sizeConstraints[idx])
          break;
		case StatementGenerator.ROWID_TYPE:
		  bytesUsed+= 32
          break;
		case StatementGenerator.ENUM_TYPE:
		  bytesUsed+= 255
          break;
		case StatementGenerator.HIERARCHY_TYPE:
		  bytesUsed+= 4000
          break;
		case StatementGenerator.ROWID_TYPE:
		  bytesUsed+= 32
          break;
  	   case StatementGenerator.MSSQL_MONEY_TYPE:
		  bytesUsed+= 1626
          break;
		case StatementGenerator.MSSQL_SMALL_MONEY_TYPE:
		  bytesUsed+= 8
          break;
		case StatementGenerator.MSSQL_ROWVERSION_TYPE:
		  bytesUsed+= 8
          break;
		case StatementGenerator.PGSQL_MONEY_TYPE:
		  bytesUsed+= 16
          break;
		case StatementGenerator.PGSQL_NAME_TYPE:
		  bytesUsed+= 64
          break;
		case StatementGenerator.PGSQL_SINGLE_CHAR_TYPE:
		  bytesUsed+= 1
          break;
		case StatementGenerator.PGSQL_NUMERIC_TYPE:
		  bytesUsed+= 32
          break;
		case StatementGenerator.ORACLE_NUMERIC_TYPE:
		  bytesUsed+= 32
          break;
		case StatementGenerator.MONGO_DECIMAL_TYPE:
		  bytesUsed+= 32
          break;
		case StatementGenerator.INET_ADDR_TYPE:
		  bytesUsed+= 39
          break;
		case StatementGenerator.MAC_ADDR_TYPE:
		  bytesUsed+= 23
          break;
		case StatementGenerator.PGSQL_IDENTIFIER:
		  bytesUsed+= 4
          break;
		case StatementGenerator.MONGO_OBJECT_ID:
		  bytesUsed+= 12
          break;
		case StatementGenerator.MONGO_UNKNOWN_TYPE:
		  bytesUsed+= 2048
          break;
		case StatementGenerator.MONGO_REGEX_TYPE:
		  bytesUsed+= 2048
          break;	   
		case StatementGenerator.XML_TYPE:
		case StatementGenerator.JSON_TYPE:
		case StatementGenerator.CLOB_TYPE:
		case StatementGenerator.BLOB_TYPE:
		   lobList.push(idx)
		   break;
        default:
           // console.log(`Oops: ${targetDataType}`);
	  }		   
	})

    /*
	**
	** LOB SIZE CALCULATION
	**  
	** Calculate bytes used by Non LOB columns
	** Calculate Number of Lob Colums
	** Calculate Max LOb Size (Bytes Remaining/LOB Columns)
	** Check for LOB columns less than Max LOB SIZE  
	** Reduce Bytes Avaialle by Size of each Small Lob Columns
	** Split remaining Bytes between Large LOB Columns
	**
	*/
		
  	this.TABLE_UNUSED_BYTES = StatementGenerator.LARGEST_ROW_SIZE - bytesUsed	
    this.TABLE_LOB_COUNT = lobList.length
	
	if (lobList.length > 0) {
	  // Resize LONG columns based on available remainingSpace.
	  lobList = lobList.flatMap((idx) => {
		// Filter small LONG fields 
		const lobSize = parseInt(sizeConstraints[idx])
	    if ((lobSize > 0) && (lobSize < this.TABLE_LOB_SIZE)) {
		  bytesUsed+=lobSize
		  return []
	    }
		return [idx]
	  })

      this.TABLE_UNUSED_BYTES = StatementGenerator.LARGEST_ROW_SIZE - bytesUsed	
      this.TABLE_LOB_COUNT = lobList.length
        
      if (this.TABLE_LOB_SIZE < StatementGenerator.LARGEST_LOB_SIZE) {
  	    this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR,tableMetadata.tableName],`LONG VARCHAR and LONG VARBINARY columns restricted to ${this.TABLE_LOB_SIZE} bytes`);
	  }
	  
	  lobList.forEach((idx) => {
        if (columnClauses[idx].indexOf(`" ${StatementGenerator.CLOB_TYPE}`) > 0) {
  		  columnClauses[idx] = `"${columnNames[idx]}" ${this.CLOB_TYPE}`;
   	      sizeConstraints[idx] = `${this.TABLE_LOB_SIZE}`
	    }
        if (columnClauses[idx].indexOf(`" ${StatementGenerator.BLOB_TYPE}`) > 0) {
   	      const column_suffix = String(idx+1).padStart(3,"0");
		  // Vertica 10.x Raises out of Memory if copy buffer is > 32M
		  copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${this.TABLE_LOB_SIZE}), "${columnNames[idx]}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`	
		  // copyColumnList[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${this.TABLE_LOB_SIZE > 16000000 ? StatementGenerator.LARGEST_LOB_SIZE : (this.TABLE_LOB_SIZE * 2)}), "${columnNames[idx]}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`	
  		  columnClauses[idx] = `"${columnNames[idx]}" ${this.BLOB_TYPE}`;
   	      sizeConstraints[idx] = `${this.TABLE_LOB_SIZE}` 
	    }		
		if (YadamuLibrary.isJSON(tableMetadata.dataTypes[idx])) {
		  columnClauses[idx] = `${tableMetadata.columnNames[idx]} ${this.CLOB_TYPE} check(YADAMU.IS_JSON("${tableMetadata.columnNames[idx]}"))`
		}
		if (YadamuLibrary.isXML(tableMetadata.dataTypes[idx])) {
		 
		 columnClauses[idx] = `${tableMetadata.columnNames[idx]} ${this.CLOB_TYPE} check(YADAMU.IS_XML("${tableMetadata.columnNames[idx]}"))`
		}
	  })
	}

	const stagingFileName =  `YST-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
	const stagingFilePath =  path.join(this.dbi.LOCAL_STAGING_AREA,stagingFileName)
	const localPath       =  path.resolve(stagingFilePath); 
	let remotePath        =  tableMetadata.dataFile || path.join(this.dbi.REMOTE_STAGING_AREA,stagingFileName)
	// remotePath            =  Array.isArray(remotePath) ? remotePath.map((p) => { return p.split(path.sep).join(path.posix.sep)}).join("','") : remotePath.split(path.sep).join(path.posix.sep); 
	// remotePath         =   =  Array.isArray(remotePath) ? remotePath.map((p) => { return p.split(path.sep).join(path.posix.sep)}).join("','") : remotePath.split(path.sep).join(path.posix.sep); 
	
    const createStatement = `create table if not exists "${this.targetSchema}"."${tableMetadata.tableName}"(\n  ${columnClauses.join(',')})`;
    const insertStatement = `insert into "${this.targetSchema}"."${tableMetadata.tableName}" ("${columnNames.join('","')}") values `;
	const mergeoutStatement = `select do_tm_task('mergeout','${this.targetSchema}.${tableMetadata.tableName}')`

    let copy
	if (Array.isArray(tableMetadata.dataFile)) {
      copy = tableMetadata.dataFile.map((remotePath,idx) => {
	    return  {
	      dml             : `copy "${this.targetSchema}"."${tableMetadata.tableName}" (${copyColumnList.join(',')}) from '${remotePath.split(path.sep).join(path.posix.sep)}' PARSER fcsvparser(type='rfc4180', header=false, trim=${this.dbi.COPY_TRIM_WHITEPSPACE===true}) NULL ''`
		, partitionCount  : tableMetadata.partitionCount
		, partitionID     : idx+1
	    }
	  })
	}
    else {
	  copy = {
	   dml         : `copy "${this.targetSchema}"."${tableMetadata.tableName}" (${copyColumnList.join(',')}) from '${remotePath.split(path.sep).join(path.posix.sep)}' PARSER fcsvparser(type='rfc4180', header=false, trim=${this.dbi.COPY_TRIM_WHITEPSPACE===true}) NULL ''`
	  }
    }
	
    return { 
       ddl             : createStatement, 
       dml             : insertStatement, 
	   copy            : copy,
	   mergeout        : mergeoutStatement,
	   stagingFileName : stagingFileName,
	   localPath       : localPath,
	   columnNames     : columnNames,
       targetDataTypes : targetDataTypes, 
	   sizeConstraints : sizeConstraints,
	   insertOperators : insertOperators,
       insertMode      : insertMode,
       _BATCH_SIZE     : this.dbi.BATCH_SIZE,
       _COMMIT_COUNT   : this.dbi.COMMIT_COUNT,
       _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT,
	   _SCHEMA_NAME    : this.targetSchema,
	   _TABLE_NAME     : tableMetadata.tableName
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

module.exports = StatementGenerator