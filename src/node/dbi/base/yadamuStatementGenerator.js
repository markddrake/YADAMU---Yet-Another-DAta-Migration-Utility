
import YadamuLibrary          from '../../lib/yadamuLibrary.js'
import YadamuDataTypes        from './yadamuDataTypes.js'

class YadamuStatementGenerator {
	  
  get SOURCE_VENDOR()        { return this._TARGET_VENDOR }
  set SOURCE_VENDOR(v)       { this._TARGET_VENDOR = v }
  
  get TYPE_MAPPINGS()        { return YadamuDataTypes.DATA_TYPE_MAPPINGS[this.SOURCE_VENDOR].mappings }
	  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {

	this.dbi = dbi;
    this.SOURCE_VENDOR = vendor
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
 
  isJSON(dataType) {
	return YadamuDataTypes.isJSON(dataType)
  }
 
  isXML(dataType) {
	return YadamuDataTypes.isXML(dataType)
  }

  mapDataType(dataType, length) {
	  
     const dataTypeDefinition = YadamuLibrary.decomposeDataType(dataType)
     const key = Object.keys(this.TYPE_MAPPINGS).find(key => this.TYPE_MAPPINGS[key] === dataTypeDefinition.type)
	 let mappedDataType = this.dbi.DATA_TYPES[key]
	 switch (mappedDataType) {
	   case this.dbi.DATA_TYPES.CHAR_TYPE:
	     switch (true) {
		   case (length === undefined) :
		   case (length < 0) :
		   case (length > this.dbi.DATA_TYPES.CHAR_LENGTH) :
	         mappedDataType = this.dbi.DATA_TYPES.CLOB_TYPE
		     break; 
           default:			 
	     }
		 break
	   case this.dbi.DATA_TYPES.VARCHAR_TYPE:
	     switch (true) {
		   case (length === undefined) :
		   case (length < 0) :
		   case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :
	         mappedDataType = this.dbi.DATA_TYPES.CLOB_TYPE
		     break; 
           default:			 
	     }
		 break
	   default:
	 }
     // console.log(key,':',dataType,length,'==>',mappedDataType)
	 return mappedDataType
	 
  }

  getMappedDataType(dataType,sizeConstraint) {
	  
	/*
	**
    **  Map the source datatype to the target.
	**
	** If the target table already exists then the data type is defined by the target table. 
	** If the target table does not exist use that mapping mechansim to determine the target type
	** First objtain the name of the key that describes the source data type in the source table mapping object
	** USe tje key to obtain the corresponding data type from the target environemnt's type mappings
	**
	*/

    const dataTypeDefinition = YadamuLibrary.composeDataType(dataType,sizeConstraint)
    if ((!this.dbi.DATATYPE_IDENTITY_MAPPING) || (this.dbi.DATABASE_VENDOR !== this.SOURCE_VENDOR)) {

	  const mappedDataType = this.mapDataType(dataTypeDefinition.type,dataTypeDefinition.length);
   
      if (mappedDataType === undefined) {
        this.yadamuLogger.logInternalError([this.dbi.DATABASE_VENDOR,`MAPPING NOT FOUND`],`Missing Mapping for "${dataType}" in mappings for "${this.SOURCE_VENDOR}".`)
	  }

	  // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.SOURCE_VENDOR,dataTypeDefinition.type,dataTypeDefinition.length,dataTypeDefinition.scale],`Mapped to "${mappedDataType}".`)
      return mappedDataType
	}
	else {
	  return this.generateStorageClause(dataType,sizeConstraint)
	}
  }
  
  generateStorageClause(mappedDataType,sizeConstraint) {

    if (sizeConstraint) {

      if (RegExp(/\(.*\)/).test(mappedDataType)) {
         /* Already has a Size specified */
         return mappedDataType
      }
	  
      if (this.dbi.DATA_TYPES.UNBOUNDED_TYPES.includes(mappedDataType)) {
        return mappedDataType
      }
      
      const sizeComponents = sizeConstraint.split(',')
      const length = sizeComponents[0] === 'max' ? -1 :  parseInt(sizeComponents[0])
      const scale = (sizeComponents.length > 1) ? parseInt(sizeComponents[1]) : undefined
      if (scale && (scale > 0)) {
        return `${mappedDataType}(${length},${scale})`
      }                                                   
      
      if (length && (length > 0)) {    
        return (mappedDataType.includes(' ') && !mappedDataType.startsWith('long')  &&!mappedDataType.startsWith('bit')) ? mappedDataType.replace(' ',`(${length}) `) : `${mappedDataType}(${length})`
      }
	}
	
	return mappedDataType
  }

  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
	  
    return `create table "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
	
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}") values (${insertOperators.join(',')})`;
  }

  getInsertOperator(mappedDataType) {
    return '?'
  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';

    const columnNames = tableMetadata.columnNames
    
    const mappedDataTypes = [];
	const insertOperators = []
	
    const columnDefinitions = columnNames.map((columnName,idx) => {
				
      const mappedDataType = tableMetadata.source ? tableMetadata.dataTypes[idx] : this.getMappedDataType(tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx])
      mappedDataTypes.push(mappedDataType)
      insertOperators.push(this.getInsertOperator(mappedDataType))
	  return `"${columnName}" ${this.generateStorageClause(mappedDataType,tableMetadata.sizeConstraints[idx])}`	   
    })
	
    const tableInfo =  { 
      ddl             : this.tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml             : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
    , columnNames     : tableMetadata.columnNames
    , sourceDataTypes : tableMetadata.source ? tableMetadata.source.dataTypes : dataTypes
    , targetDataTypes : mappedDataTypes
    , insertMode      : insertMode
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
	
	return tableInfo
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

export { YadamuStatementGenerator as default }

/*
**
     switch (vendor) {
       case 'Oracle':
         switch (dataType) {

           case 'CHAR':                                                            return this.dbi.DATA_TYPES.CHAR_TYPE
           case 'NCHAR':                                                           return this.dbi.DATA_TYPES.NVARCHAR_TYPE
           case 'VARCHAR2':                                                        return this.dbi.DATA_TYPES.VARCHAR_TYPE
           case 'NVARCHAR2':                                                       return this.dbi.DATA_TYPES.NVARCHAR_TYPE

           case 'NUMBER':                                                          return this.dbi.DATA_TYPES.NUMERIC_TYPE
           case 'BINARY_FLOAT':                                                    return this.dbi.DATA_TYPES.FLOAT_TYPE
           case 'BINARY_DOUBLE':                                                   return this.dbi.DATA_TYPES.DOUBLE_TYPE

           case 'CLOB':                                                            return this.dbi.DATA_TYPES.CLOB_TYPE
           case 'NCLOB':                                                           return this.dbi.DATA_TYPES.NCLOB_TYPE

           case 'BOOLEAN':                                                         return this.dbi.DATA_TYPES.BOOLEAN_TYPE
           case 'RAW':                                                             return this.dbi.DATA_TYPES.VARBINARY_TYPE
           case 'BLOB':                                                            return this.dbi.DATA_TYPES.BLOB_TYPE

           case 'DATE':                                                            return this.dbi.DATA_TYPES.DATE_TYPE
           case 'TIMESTAMP':
             switch (true) {
               default:                                                            return this.dbi.DATA_TYPES.TIMESTAMP_TYPE
             }

           case 'BFILE':                                                           return this.dbi.DATA_TYPES.ORACLE_BFILE_TYPE
           case 'ROWID':                                                           return this.dbi.DATA_TYPES.ORACLE_ROWID_TYPE

           case 'JSON':                                                            return this.dbi.DATA_TYPES.JSON_TYPE
           case 'XMLTYPE':                                                         return this.dbi.DATA_TYPES.XML_TYPE
           case '"MDSYS"."SDO_GEOMETRY"':                                          return this.dbi.DATA_TYPES.SPATIAL_TYPE
           case 'ANYDATA':                                                         return this.dbi.DATA_TYPES.CLOB_TYPE

           default :
             if (dataType.indexOf('LOCAL TIME ZONE') > -1) {
               return this.dbi.DATA_TYPES.TIMESTAMP_LTZ_TYPE
             }
             if (dataType.indexOf('TIME ZONE') > -1) {
               return this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE
             }
             if (dataType.indexOf('INTERVAL') === 0) {
               return 'VARCHAR(16)';
             }
             if (dataType.indexOf('XMLTYPE') > -1) {
               return this.dbi.DATA_TYPES.XML_TYPE
             }
             if (dataType.indexOf('.') > -1) {
               return this.dbi.DATA_TYPES.CLOB_TYPE
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toUpperCase();
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType) {
           case 'char':
             switch (true) {
               case (length === -1):                                               return this.dbi.DATA_TYPES.CLOB_TYPE
               case (length > this.dbi.DATA_TYPES.CHAR_LENGTH) :                        return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.CHAR_TYPE
             }
           case 'nchar':
             switch (true) {
               case (length === -1):                                               return this.dbi.DATA_TYPES.NCLOB_TYPE
               case (length > this.dbi.DATA_TYPES.NCHAR_LENGTH) :                       return this.dbi.DATA_TYPES.NCLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.NCHAR_TYPE
             }
           case 'varchar':
             switch (true) {
               case (length === -1):                                               return this.dbi.DATA_TYPES.CLOB_TYPE
               case (length > this.VARCHAR_LENGTH) :                               return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARCHAR_TYPE
             }
           case 'nvarchar':
             switch (true) {
               case (length === -1):                                               return this.dbi.DATA_TYPES.NCLOB_TYPE
               case (length > this.NVARCHAR_LENGTH) :                              return this.dbi.DATA_TYPES.NCLOB_TYPE
             default:                                                              return this.dbi.DATA_TYPES.NVARCHAR_TYPE
             }
           case 'text':                                                            return this.dbi.DATA_TYPES.CLOB_TYPE
           case 'ntext':                                                           return this.dbi.DATA_TYPES.NCLOB_TYPE


           case 'tinyint':                                                         return this.dbi.DATA_TYPES.TINYINT_TYPE
           case 'smallint':                                                        return this.dbi.DATA_TYPES.SMALLINT_TYPE
           case 'mediumint':                                                       return this.dbi.DATA_TYPES.MEDIUMINT_TYPE
           case 'int':
           case 'integer':                                                         return this.dbi.DATA_TYPES.INTEGER_TYPE
           case 'bigint':                                                          return this.dbi.DATA_TYPES.BIGINT_TYPE
           case 'smallmoney':                                                      return this.dbi.DATA_TYPES.MSSQL_SMALL_MONEY_TYPE
           case 'money':                                                           return this.dbi.DATA_TYPES.MSSQL_MONEY_TYPE
           case 'real':                                                            return this.dbi.DATA_TYPES.FLOAT_TYPE
           case 'float':                                                           return this.dbi.DATA_TYPES.DOUBLE_TYPE
           case 'numeric':                                                         return this.dbi.DATA_TYPES.NUMERIC_TYPE

           case 'date':                                                            return this.dbi.DATA_TYPES.DATE_TYPE
           case 'time':                                                            return this.dbi.DATA_TYPES.TIME_TYPE
           case 'smalldate':                                                       return this.dbi.DATA_TYPES.DATETIME_TYPE
           case 'datetime':                                                        return this.dbi.DATA_TYPES.DATETIME_TYPE
           case 'datetime2':
             switch (true) {
                // case (length > 6):                                              return 'datetime(6)';
                default:                                                           return this.dbi.DATA_TYPES.DATETIME_TYPE
             }
           case 'datetimeoffset':                                                  return this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE

           case 'bit':                                                             return this.dbi.DATA_TYPES.BOOLEAN_TYPE
           case 'binary':
             switch (true) {
               case (length === -1):                                               return this.dbi.DATA_TYPES.BLOB_TYPE
               case (length > this.dbi.DATA_TYPES.BINARY_LENGTH):                       return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.BINARY_TYPE
             }
           case 'varbinary':
             switch (true) {
               case (length === -1):                                               return this.dbi.DATA_TYPES.BLOB_TYPE
               case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH):                    return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARBINARY_TYPE
             }
           case 'image':                                                           return this.dbi.DATA_TYPES.BLOB_TYPE

           case 'rowversion':                                                      return this.dbi.DATA_TYPES.MSSQL_ROWVERSION_TYPE
           case 'hierarchyid':                                                     return this.dbi.DATA_TYPES.MSSQL_HEIRARCHY_TYPE
           case 'uniqueidentifier':                                                return this.dbi.DATA_TYPES.UUID_TYPE

           case 'xml':                                                             return this.dbi.DATA_TYPES.XML_TYPE
           case 'JSON':                                                            return this.dbi.DATA_TYPES.JSON_TYPE
           case 'geometry':                                                        return this.dbi.DATA_TYPES.GEOMETRY_TYPE
           case 'geography':                                                       return this.dbi.DATA_TYPES.GEOGRAPHY_TYPE


           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toUpperCase();
         }
         break;
       case 'Postgres':
         switch (dataType.toLowerCase()) {
           case 'bpchar':
           case 'character':
             switch (true) {
               case (length === undefined):                                        return this.dbi.DATA_TYPES.CLOB_TYPE
               case (length > this.dbi.DATA_TYPES.CHAR_LENGTH) :                        return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.CHAR_TYPE
             }
           case 'character varying':
             switch (true) {
               case (length === undefined):                                        return this.dbi.DATA_TYPES.CLOB_TYPE
               case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :                     return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARCHAR_TYPE
             }
           case 'name':                                                            return this.dbi.DATA_TYPES.PGSQL_NAME_TYPE

           case 'bytea':
             switch (true) {
               case (length === undefined):                                        return this.dbi.DATA_TYPES.BLOB_TYPE
               case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH):                    return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARBINARY_TYPE
             }

           case 'smallint':                                                        return this.dbi.DATA_TYPES.SMALLINT_TYPE
           case 'integer':                                                         return this.dbi.DATA_TYPES.INTEGER_TYPE
           case 'bigint':                                                          return this.dbi.DATA_TYPES.BIGINT_TYPE
           case 'real':                                                            return this.dbi.DATA_TYPES.FLOAT_TYPE
           case 'double precision':                                                return this.dbi.DATA_TYPES.DOUBLE_TYPE
           case 'numeric':                                                         return this.dbi.DATA_TYPES.NUMERIC_TYPE
           case 'decimal':                                                         return this.dbi.DATA_TYPES.DECIMAL_TYPE
           case 'money':                                                           return this.dbi.DATA_TYPES.PGSQL_MONEY_TYPE

           case 'date':                                                            return this.dbi.DATA_TYPES.DATE_TYPE
           case 'timestamp':
           case 'timestamp without time zone':                                     return this.dbi.DATA_TYPES.TIMESTAMP_TYPE
           case 'timestamp with time zone':                                        return this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE
           case 'time without time zone':                                          return this.dbi.DATA_TYPES.TIME_TYPE
           case 'time with time zone':                                             return this.dbi.DATA_TYPES.TIME_TZ_TYPE

           case 'boolean':                                                         return this.dbi.DATA_TYPES.BOOLEAN_TYPE
		   
           case 'bit':                                                             return this.dbi.DATA_TYPES.BIT_STRING_TYPE
           
		   case 'bit varying':
             switch (true) {
               case (length === undefined):                                        return this.dbi.DATA_TYPES.CLOB_TYPE
               case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH):                      return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARBIT_STRING_TYPE
             }

           case 'uuid':                                                            return this.dbi.DATA_TYPES.UUID_TYPE
           case 'text':                                                            return this.dbi.DATA_TYPES.CLOB_TYPE
           case 'xml':                                                             return this.dbi.DATA_TYPES.XML_TYPE
           case 'json':                                                            return this.dbi.DATA_TYPES.JSON_TYPE
           case 'jsonb':                                                           return this.dbi.DATA_TYPES.JSON_TYPE

           case 'point':                                                           return this.dbi.DATA_TYPES.POINT_TYPE 
           case 'lseg':                                                            return this.dbi.DATA_TYPES.LINE_TYPE 
           case 'path':                                                            return this.dbi.DATA_TYPES.PATH_TYPE  
           case 'box':                                                             return this.dbi.DATA_TYPES.BOX_TYPE 
           case 'polygon':                                                         return this.dbi.DATA_TYPES.POLYGON_TYPE 
           case 'geography':                                                       return this.dbi.DATA_TYPES.SPATIAL_TYPE
           case 'line':                                                            return this.dbi.DATA_TYPES.PGSQL_LINE_TYPE
           case 'circle':                                                          return this.dbi.DATA_TYPES.CIRCLE_TYPE

           case 'cidr':
           case 'inet':                                                            return this.dbi.DATA_TYPES.PGSQL_INET_ADDR_TYPE
           case 'macaddr':
           case 'macaddr8':                                                        return this.dbi.DATA_TYPES.PGSQL_MAC_ADDR_TYPE


           case 'int4range':
           case 'int8range':
           case 'numrange':
           case 'tsrange':
           case 'tstzrange':
           case 'daterange':                                                       return this.dbi.DATA_TYPES.JSON_TYPE
           case 'tsvector':
           case 'gtsvector':                                                       return this.dbi.DATA_TYPES.JSON_TYPE

           case 'tsquery':                                                         return this.dbi.DATA_TYPES.MAX_VARCHAR_TYPE

           case 'oid':                                                             return this.dbi.DATA_TYPES.PGSQL_IDENTIFIER

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
           case 'regtype':                                                         return this.dbi.DATA_TYPES.PGSQL_IDENTIFIER

           case 'tid':
           case 'xid':
           case 'cid':
           case 'txid_snapshot':                                                   return this.dbi.DATA_TYPES.PGSQL_IDENTIFIER

           case 'aclitem':
           case 'refcursor':                                                       return this.dbi.DATA_TYPES.JSON_TYPE

          default:
             if (dataType.indexOf('interval') === 0) {
               return 'varchar(16)';
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toUpperCase();
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
           case 'char':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.CHAR_LENGTH) :                        return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.CHAR_TYPE
             }

           case 'varchar':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :                     return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARCHAR_TYPE
             }

           case 'longtext':                                                        return this.dbi.DATA_TYPES.CLOB_TYPE
           case 'mediumtext':
             switch (true) {
               case (16777215 > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :                   return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return `${this.dbi.DATA_TYPES.VARCHAR_TYPE}(16777215)`
             }
           case 'text':
             switch (true) {
               case (65535 > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :                      return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return `${this.dbi.DATA_TYPES.VARCHAR_TYPE}(65535)`
             }

           case 'tinytext':                                                        return `${this.dbi.DATA_TYPES.VARCHAR_TYPE}(256)`

           case 'tinyint':                                                         return this.dbi.DATA_TYPES.TINYINT_TYPE
           case 'smallint':                                                        return this.dbi.DATA_TYPES.SMALLINT_TYPE
           case 'mediumint':                                                       return this.dbi.DATA_TYPES.MEDIUMINT_TYPE
           case 'int':                                                             return this.dbi.DATA_TYPES.INTEGER_TYPE
           case 'bigint':                                                          return this.dbi.DATA_TYPES.BIGINT_TYPE

           case 'decimal':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.NUMERIC_PRECISION):                   return this.dbi.DATA_TYPES.MAX_NUMERIC_TYPE
               default:                                                            return this.dbi.DATA_TYPES.DECIMAL_TYPE
             }

           case 'float':                                                           return this.dbi.DATA_TYPES.FLOAT_TYPE
           case 'double':                                                          return this.dbi.DATA_TYPES.DOUBLE_TYPE

           case 'boolean':                                                         return this.dbi.DATA_TYPES.BOOLEAN_TYPE

           case 'date':                                                            return this.dbi.DATA_TYPES.DATE_TYPE
           case 'time':                                                            return this.dbi.DATA_TYPES.TIME_TYPE
           case 'datetime':                                                        return this.dbi.DATA_TYPES.DATETIME_TYPE
           case 'year':                                                            return this.dbi.DATA_TYPES.MYSQL_YEAR_TYPE || this.YEAR_TYPE

           case 'binary':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.BINARY_LENGTH) :                      return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.BINARY_TYPE
             }

           case 'varbinary':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH) :                   return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARBINARY_TYPE
             }

           case 'longblob':
           case 'mediumblob':                                                      return this.dbi.DATA_TYPES.BLOB_TYPE
             switch (true) {
               case (65535 > this.dbi.DATA_TYPES.BINARY_LENGTH) :                       return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return `${DataTypes.VARCHAR_TYPE}(65535)`
             }
           case 'blob':                                                            return `${DataTypes.VARBINARY_TYPE}(65535)`
           case 'tinyblob':                                                        return `${DataTypes.VARBINARY_TYPE}(256)`

           case 'bit':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.BIT_LENGTH) :                         return this.dbi.DATA_TYPES.CHAR_TYPE
               default:                                                            return this.dbi.DATA_TYPES.BIT_STRING_TYPE
             }

           case 'json':                                                            return this.dbi.DATA_TYPES.JSON_TYPE
           case 'xml':                                                             return this.dbi.DATA_TYPES.XML_TYPE

           case 'point':                                                           return this.dbi.DATA_TYPES.POINT_TYPE
           case 'linestring':                                                      return this.dbi.DATA_TYPES.LINE_TYPE  
           case 'polygon':                                                         return this.dbi.DATA_TYPES.POLYGON_TYPE
           case 'geometry':                                                        return this.dbi.DATA_TYPES.SPATIAL_TYPE
           case 'multipoint':                                                      return this.dbi.DATA_TYPES.MULTI_POINT_TYPE
           case 'multilinestring':                                                 return this.dbi.DATA_TYPES.MULTI_LINE_TYPE
           case 'multipolygon':                                                    return this.dbi.DATA_TYPES.MULTI_POLYGON_TYPE
           case 'geometrycollection':                                              return this.dbi.DATA_TYPES.GEOMETRY_COLLECTION_TYPE
           case 'geomcollection':                                                  return this.dbi.DATA_TYPES.GEOMETRY_COLLECTION_TYPE
           case 'geometry':                                                        return this.dbi.DATA_TYPES.GEOMETRY_TYPE

           case 'set':                                                             return this.dbi.DATA_TYPES.MYSQL_SET_TYPE || this.JSON_TYPE
           case 'enum':                                                            return this.dbi.DATA_TYPES.MYSQL_ENUM_TYPE || `${DataTypes.VARCHAR_TYPE}(512)`

           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toUpperCase();
         }
         break;
       case 'SNOWFLAKE':
         switch (dataType.toUpperCase()) {
           case 'TEXT':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :                     return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.CHAR_TYPE
             }

           case 'JSON':                                                            return this.dbi.DATA_TYPES.JSON_TYPE
           case 'SET':                                                             return this.dbi.DATA_TYPES.JSON_TYPE
           case 'XML':                                                             return this.dbi.DATA_TYPES.XML_TYPE
           case 'XMLTYPE':                                                         return this.dbi.DATA_TYPES.XML_TYPE
           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toUpperCase();
         }
         break;
       case 'MongoDB':
         switch (dataType.toLowerCase()) {
           case 'string':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :                     return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARCHAR_TYPE
             }

           case 'int':                                                             return this.dbi.DATA_TYPES.INTEGER_TYPE
           case 'long':                                                            return this.dbi.DATA_TYPES.BIGINT_TYPE
           case 'double':                                                          return this.dbi.DATA_TYPES.DOUBLE_TYPE
           case 'bindata':                                                         return this.dbi.DATA_TYPES.VARBINARY_TYPE
           case 'bool':                                                            return this.dbi.DATA_TYPES.BOOLEAN_TYPE
           case 'date':                                                            return this.dbi.DATA_TYPES.DATE_TYPE
           case 'timestamp':                                                       return this.dbi.DATA_TYPES.TIMESTAMP_TYPE
           case 'objectid':                                                        return this.dbi.DATA_TYPES.MONGO_OBJECT_ID;
           case 'decimal':                                                         return this.dbi.DATA_TYPES.MONGO_DECIMAL128_TYPE
           case 'array':                                                           return this.dbi.DATA_TYPES.MONGO_ARRAY_TYPE
           case 'object':                                                          return this.dbi.DATA_TYPES.MONGO_OBJECT_TYPE
           case 'null':                                                            return this.dbi.DATA_TYPES.MONGO_NULL_TYPE
           case 'regex':                                                           return this.dbi.DATA_TYPES.MONGO_REGEX_TYPE
           case 'javascript':                                                      return this.dbi.DATA_TYPES.MONGO_JS_TYPE
           case 'javascriptWithScope':                                             return this.dbi.DATA_TYPES.MONGO_SCOPED_JS_TYPE
           case 'minkey':                                                          return this.dbi.DATA_TYPES.MONGO_MINKEY_TYPE
           case 'maxKey':                                                          return this.dbi.DATA_TYPES.MONGO_MAXKEY_TYPE
           case 'undefined':                                                       return this.dbi.DATA_TYPES.MONGO_UNDEFINED_TYPE
           case 'dbPointer':                                                       return this.dbi.DATA_TYPES.MONGO_DBPOINTER_TYPE
           case 'function':                                                        return this.dbi.DATA_TYPES.MONGO_FUNCTION_TYPE
           case 'symbol':                                                          return this.dbi.DATA_TYPES.MONGO_SYMBOL_TYPE
           // No data in the Mongo Collection
           case 'json':                                                            return this.dbi.DATA_TYPES.JSON_TYPE
           deafault:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toLowerCase();
         }
         break;
       case 'Vertica':
         switch (dataType.toLowerCase()) {
           case 'char':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.CHAR_LENGTH) :                        return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.CHAR_TYPE
             }

           case 'varchar':
           case 'long varchar':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH):                      return this.dbi.DATA_TYPES.CLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARCHAR_TYPE
             }

           case 'binary':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.BINARY_LENGTH):                       return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.BINARY_TYPE
             }

           case 'varbinary':
           case 'long varbinary':
             switch (true) {
               case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH):                    return this.dbi.DATA_TYPES.BLOB_TYPE
               default:                                                            return this.dbi.DATA_TYPES.VARBINARY_TYPE
             }

           case 'numeric':
             switch (true) {
               default:                                                            return this.dbi.DATA_TYPES.NUMERIC_TYPE
             }

           case 'boolean':                                                         return this.dbi.DATA_TYPES.BOOLEAN_TYPE


           case 'int':                                                             return this.dbi.DATA_TYPES.BIGINT_TYPE
           case 'float':                                                           return this.dbi.DATA_TYPES.DOUBLE_TYPE

           case 'date':                                                            return this.dbi.DATA_TYPES.DATE_TYPE
           case 'time':                                                            return this.dbi.DATA_TYPES.TIME_TYPE
           case 'timetz':                                                          return this.dbi.DATA_TYPES.TIME_TZ_TYPE
           case 'timestamptz':                                                     return this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE
           case 'timestamp':                                                       return this.dbi.DATA_TYPES.TIMESTAMP_TYPE
           case 'year':                                                            return this.dbi.DATA_TYPES.VERTICA_YEAR_TYPE || this.YEAR_TYPE

           case 'xml':                                                             return this.dbi.DATA_TYPES.XML_TYPE
           case 'json':                                                            return this.dbi.DATA_TYPES.JSON_TYPE
           case 'uuid':                                                            return this.dbi.DATA_TYPES.UUID_TYPE
		   
           case 'geometry':                                                        return this.dbi.DATA_TYPES.SPATIAL_TYPE
           case 'geography':                                                       return this.dbi.DATA_TYPES.GEOGRAPHY_TYPE
		   
           default:
             if (dataType.startsWith('interval')) {
               return this.dbi.DATA_TYPES.INTERVAL_TYPE
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toLowerCase();
         }
         break;
       case 'Teradata':
         switch (dataType) {
           case '++':                                                              return this.dbi.DATA_TYPES.JSON_TYPE // ANY TY{E
           case 'A1':                                                              return this.dbi.DATA_TYPES.JSON_TYPE // SINGLE DIMENSIONAL ARRAY
           case 'AN':                                                              return this.dbi.DATA_TYPES.JSON_TYPE // MULTI DIMENSIONAL ARRAY
           case 'AT':                                                              return this.dbi.DATA_TYPES.TIME_TYPE
           case 'BO':                                                              return this.dbi.DATA_TYPES.BLOB_TYPE
           case 'BF':                                                              return this.dbi.DATA_TYPES.BINARY_TYPE
           case 'BV':                                                              return this.dbi.DATA_TYPES.VARBINARY_TYPE
           case 'CF':                                                              return this.dbi.DATA_TYPES.CHAR_TYPE
           case 'CV':                                                              return this.dbi.DATA_TYPES.VARCHAR_TYPE
           case 'CO':                                                              return this.dbi.DATA_TYPES.CLOB_TYPE
           case 'D':                                                               return this.dbi.DATA_TYPES.DECIMAL_TYPE
           case 'DA':                                                              return this.dbi.DATA_TYPES.DATE_TYPE
           case 'DY':                                                              return this.dbi.DATA_TYPES.INTERVAL_DAY_TYPE              || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL DAY
           case 'DH':                                                              return this.dbi.DATA_TYPES.INTERVAL_DAY_TO_HOUR_TYPE      || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL DAY TO HOUR
           case 'DM':                                                              return this.dbi.DATA_TYPES.INTERVAL_DAY_TO_MINUTE_TYPE    || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL DAY TO MINUTE
           case 'DS':                                                              return this.dbi.DATA_TYPES.INTERVAL_DAY_TO_SECOND_TYPE    || this.INTERVAL_TYPE                                              // INTERVAL DAY TO SECOND
           case 'HR':                                                              return this.dbi.DATA_TYPES.INTERVAL_HOUR_TYPE             || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL HOUR
           case 'HM':                                                              return this.dbi.DATA_TYPES.INTERVAL_HOUR_TO_MINUTE_TYPE   || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL HOUR TO MINUTE
           case 'HS':                                                              return this.dbi.DATA_TYPES.INTERVAL_HOUR_TO_SECOND_TYPE   || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL HOUR TO SECOND
           case 'F':                                                               return this.dbi.DATA_TYPES.DOUBLE_TYPE
           case 'I':                                                               return this.dbi.DATA_TYPES.INTEGER_TYPE
           case 'I1':                                                              return this.dbi.DATA_TYPES.TINYINY_TYPE
           case 'I2':                                                              return this.dbi.DATA_TYPES.SMALLINT_TYPE
           case 'I4':                                                              return this.dbi.DATA_TYPES.INTEGER_TYPE
           case 'I8':                                                              return this.dbi.DATA_TYPES.BIGINT_TYPE
           case 'JN':                                                              return this.dbi.DATA_TYPES.JSON_TYPE
           case 'MI':                                                              return this.dbi.DATA_TYPES.INTERVAL_MINUTE_TYPE           || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL MINUTE
           case 'MS':                                                              return this.dbi.DATA_TYPES.INTERVAL_MINUTE_TO_SECOND_TYPE || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL MINUTE TO SECOND
           case 'N':                                                               return this.dbi.DATA_TYPES.NUMERIC_TYPE
           case 'PD':                                                              return this.dbi.DATA_TYPES.PERIOD_DAY_TYPE
           case 'PT':                                                              return this.dbi.DATA_TYPES.PERIOD_TIME_TYPE
           case 'PZ':                                                              return this.dbi.DATA_TYPES.PERIOD_TIME_TZ_TYPE
           case 'PS':                                                              return this.dbi.DATA_TYPES.PERIOD_TIMESTAMP_TYPE
           case 'PM':                                                              return this.dbi.DATA_TYPES.PERIOD_TIMESTAMP_TZ_TYPE
           case 'N':                                                               return this.dbi.DATA_TYPES.NUMERIC_TYPE
           case 'SC':                                                              return this.dbi.DATA_TYPES.INTERVAL_SECOND_TYPE           || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.INTERVAL_TYPE      // INTERVAL SECOND
           case 'SZ':                                                              return this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE
           case 'TS':                                                              return this.dbi.DATA_TYPES.TIMESTAMP_TYPE
           case 'TZ':                                                              return this.dbi.DATA_TYPES.TIME_TZ_TYPE || this.TIME_TYPE
           case 'UT':                                                              return this.dbi.DATA_TYPES.JSON_TYPE // USER DEFINED TYPE
           case 'XM':                                                              return this.dbi.DATA_TYPES.XML_TYPE
           case 'YR':                                                              return this.dbi.DATA_TYPES.YEAR_TYPE                      || DataTyes.INTERVAL_YEAR_TO_MONTH      || this.INTERVAL_TYPE      // INTERVAL YEAR
           case 'YM':                                                              return this.dbi.DATA_TYPES.INTERVAL_YEAR_TO_MONTH         || this.INTERVAL_TYPE                                              // INTERVAL YEAR TO MONTH
           default:                                                                return dataType.toUpperCase()
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toUpperCase();
         }
         break
       default:
         this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
         return dataType.toUpperCase();
    }

**
*/