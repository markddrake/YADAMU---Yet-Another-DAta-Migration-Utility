
import fs                     from 'fs'

import {
  dirname,
  join
}                             from 'path'

import { 
  fileURLToPath 
}                             from 'url'

import {
  YadamuError
}                                            from '../../core/yadamuException.js'

const  __filename             = fileURLToPath(import.meta.url)
const __dirname               = dirname(__filename)
const DataTypeConfiguration   = JSON.parse(fs.readFileSync(join(__dirname,'../cfg/dataTypeConfiguration.json'),'utf-8'))
const DataTypeClassification  = JSON.parse(fs.readFileSync(join(__dirname,'../cfg/dataTypeClassification.json'),'utf-8'))

class StorageOptions {

  static get SPATIAL_FORMAT()                      { return 'WKB' }
  
  static set SPATIAL_FORMAT(v)                     { Object.defineProperty(this,'SPATIAL_FORMAT',   { get() { return(v) }, configurable: true })}

  static get CIRCLE_FORMAT()                       { return 'JSON' }

  static set CIRCLE_FORMAT(v)                      { Object.defineProperty(this,'CIRCLE_FORMAT',   { get() { return(v) }, configurable: true })}

}

class YadamuDataTypes {

  static storageOptions = StorageOptions
    
  static get DATA_TYPE_CONFIGURATION() {
    return DataTypeConfiguration
  }
  
  static get CHAR_TYPE()                           {
    throw new YadamuError(`Must supply explicit data type mapping for 'CHAR_TYPE'`)
  }
  
  static set CHAR_TYPE(v)                          { Object.defineProperty(this,'CHAR_TYPE',   { get() { return(v) }, configurable: true })}
  
  static get BINARY_TYPE()                         {
    throw new YadamuError(`Must supply explicit data type mapping for 'BINARY_TYPE'`)
  }
  
  static set BINARY_TYPE(v)                        { Object.defineProperty(this,'BINARY_TYPE', { get() { return(v) }, configurable: true })}
  
  static get FLOAT_TYPE()                          {
    throw new YadamuError(`Must supply explicit data type mapping for 'FLOAT_TYPE'`)
  }
  
  static set FLOAT_TYPE(v)                         { Object.defineProperty(this,'FLOAT_TYPE', { get() { return(v) }, configurable: true })}
  
  static get NUMERIC_TYPE()                        {
    throw new YadamuError(`Must supply explicit data type mapping for 'NUMERIC_TYPE'`)
  }
  
  static set NUMERIC_TYPE(v)                       { Object.defineProperty(this,'NUMERIC_TYPE', { get() { return(v) }, configurable: true })}
  
  static get UNBOUNDED_NUMERIC_TYPE()              {
	  
    throw new YadamuError(`Must supply explicit data type mapping for 'UNBOUNDED_NUMERIC_TYPE'`)
  }
  
  static set UNBOUNDED_NUMERIC_TYPE(v)             { Object.defineProperty(this,'UNBOUNDED_NUMERIC_TYPE', { get() { return(v) }, configurable: true })}
  
  static get TIMESTAMP_TYPE()                      {
    throw new YadamuError(`Must supply explicit data type mapping for 'TIMESTAMP_TYPE'`)
  }
   
  static set TIMESTAMP_TYPE(v)                     { Object.defineProperty(this,'TIMESTAMP_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // Characeter Types                              
                                                   
  static get NCHAR_TYPE()                          { return this.CHAR_TYPE }
                                                   
  static set NCHAR_TYPE(v)                         { Object.defineProperty(this,'NCHAR_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get VARCHAR_TYPE()                        { return this.CHAR_TYPE }
                                                   
  static set VARCHAR_TYPE(v)                       { Object.defineProperty(this,'VARCHAR_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get NVARCHAR_TYPE()                       { return this.VARCHAR_TYPE }
                                                   
  static set NVARCHAR_TYPE(v)                      { Object.defineProperty(this,'NVARCHAR_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MAX_VARCHAR_TYPE()                    { return `${this.VARCHAR_TYPE}(${this.VARCHAR_LENGTH})` }
                                                   
  // Boolean Types                                 
                                                   
  static get BOOLEAN_TYPE()                        { return 'BOOLEAN' }
                                                   
  static set BOOLEAN_TYPE(v)                       { Object.defineProperty(this,'BOOLEAN_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // Bit Types                                     
                                                   
  static get SINGLE_BIT_TYPE()                     { return `${this.BINARY_TYPE}(1)` }
                                                   
  static set SINGLE_BIT_TYPE(v)                    { Object.defineProperty(this,'SINGLE_BIT_TYPE', { get() { return(v) }, configurable: true })}
  
  static get BIT_STRING_TYPE()                     { return this.CHAR_TYPE }
                                                   
  static set BIT_STRING_TYPE(v)                    { Object.defineProperty(this,'BIT_STRING_TYPE', { get() { return(v) }, configurable: true })}
  
  static get VARBIT_STRING_TYPE()                  { return this.VARCHAR_TYPE }
                                                   
  static set VARBIT_STRING_TYPE(v)                 { Object.defineProperty(this,'VARBIT_STRING_TYPE', { get() { return(v) }, configurable: true })}
  
  // Binary Types                                  
                                                   
  static get VARBINARY_TYPE()                      { return this.BINARY_TYPE }
                                                   
  static set VARBINARY_TYPE(v)                     { Object.defineProperty(this,'VARBINARY_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MAX_VARBINARY_TYPE()                  { return `${this.VARBINARY_TYPE}(${this.VARBINARY_LENGTH})` }
                                                   
  // LOB Types                                     
                                                   
  static get CLOB_TYPE()                           { return this.MAX_VARCHAR_TYPE }
                                                   
  static set CLOB_TYPE(v)                          { Object.defineProperty(this,'CLOB_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MAX_CLOB_TYPE()                       { return `${this.CLOB_TYPE}(${this.CLOB_LENGTH})` }
                                                   
  static get NCLOB_TYPE()                          { return this.CLOB_TYPE }
                                                   
  static set NCLOB_TYPE(v)                         { Object.defineProperty(this,'NCLOB_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get BLOB_TYPE()                           { return this.MAX_VARBINARY_TYPE }
                                                   
  static set BLOB_TYPE(v)                          { Object.defineProperty(this,'BLOB_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MAX_BLOB_TYPE()                       { return `${this.BLOB_TYPE}(${this.BLOB_LENGTH})` }
                                                   
  // Integer Types                                   
                                                   
  static get INTEGER_TYPE()                        { return `${this.NUMERIC_TYPE}(19)` }
                                                   
  static set INTEGER_TYPE(v)                       { Object.defineProperty(this,'INTEGER_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get TINYINT_TYPE()                        { return this.SMALLINT_TYPE }
                                                   
  static set TINYINT_TYPE(v)                       { Object.defineProperty(this,'TINYINT_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get SMALLINT_TYPE()                       { return this.MEDIUMINT_TYPE }
                                                   
  static set SMALLINT_TYPE(v)                      { Object.defineProperty(this,'SMALLINT_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MEDIUMINT_TYPE()                      { return this.INTEGER_TYPE }
                                                   
  static set MEDIUMINT_TYPE(v)                     { Object.defineProperty(this,'MEDIUMINT_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get BIGINT_TYPE()                         { return this.INTEGER_TYPE }
                                                   
  static set BIGINT_TYPE(v)                        { Object.defineProperty(this,'BIGINT_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // Float Types                                   
                                                   
  static get REAL_TYPE()                           { return this.FLOAT_TYPE }
                                                   
  static get DOUBLE_TYPE()                         { return this.FLOAT_TYPE }
                                                   
  static set DOUBLE_TYPE(v)                        { Object.defineProperty(this,'DOUBLE_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // Decimal Types                                   
                                                   
  static get NUMBER_TYPE()                         { return this.NUMERIC_TYPE }
                                                   
  static set NUMBER_TYPE(v)                        { Object.defineProperty(this,'NUMBER_TYPE', { get() { return(v) }, configurable: true })}
  
  static get DECIMAL_TYPE()                        { return this.NUMERIC_TYPE }
                                                   
  static set DECIMAL_TYPE(v)                       { Object.defineProperty(this,'DECIMAL_TYPE', { get() { return(v) }, configurable: true })}
  
  // Date, Time, Timestamp, Interval and Period Types
                                                   
  static get DATE_TYPE()                           { return this.TIMESTAMP_TYPE }
                                                   
  static set DATE_TYPE(v)                          { Object.defineProperty(this,'DATE_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get TIME_TYPE()                           { return this.TIMESTAMP_TYPE }
                                                   
  static set TIME_TYPE(v)                          { Object.defineProperty(this,'TIME_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get TIME_TZ_TYPE()                        { return this.TIMESTAMP_TZ_TYPE }
                                                   
  static set TIME_TZ_TYPE(v)                       { Object.defineProperty(this,'TIME_TZ_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get DATETIME_TYPE()                       { return this.TIMESTAMP_TYPE }
                                                   
  static set DATETIME_TYPE(v)                      { Object.defineProperty(this,'DATETIME_TYPE', { get() { return(v) }, configurable: true })}
  
  static get TIMESTAMP_TZ_TYPE()                   { return this.TIMESTAMP_TYPE }
                                                   
  static set TIMESTAMP_TZ_TYPE(v)                  { Object.defineProperty(this,'TIMESTAMP_TZ_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get TIMESTAMP_LTZ_TYPE()                  { return this.TIMESTAMP_TZ_TYPE }
                                                   
  static set TIMESTAMP_LTZ_TYPE(v)                 { Object.defineProperty(this,'TIMESTAMP_LTZ_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get INTERVAL_TYPE()                       { return `${this.VARCHAR_TYPE}(16)` } 
                                                   
  static set INTERVAL_TYPE(v)                      { Object.defineProperty(this,'INTERVAL_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get INTERVAL_DAY_TO_SECOND_TYPE()         { return this.INTERVAL_TYPE }
                                                   
  static set INTERVAL_DAY_TO_SECOND_TYPE(v)        { Object.defineProperty(this,'INTERVAL_DAY_TO_SECOND_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get INTERVAL_YEAR_TO_MONTH_TYPE()         { return this.INTERVAL_TYPE }
                                                   
  static set INTERVAL_YEAR_TO_MONTH_TYPE(v)        { Object.defineProperty(this,'INTERVAL_YEAR_TO_MONTH_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // JSON and XML                                  
                                                   
  static get JSON_TYPE()                           { return this.CLOB_TYPE }
                                                   
  static set JSON_TYPE(v)                          { Object.defineProperty(this,'JSON_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get XML_TYPE()                            { return this.CLOB_TYPE }
                                                   
  static set XML_TYPE(v)                           { Object.defineProperty(this,'XML_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // Spatial                                       
                                                   
  static get SPATIAL_TYPE()                        { return this.JSON_TYPE }
                                                   
  static set SPATIAL_TYPE(v)                       { Object.defineProperty(this,'SPATIAL_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get GEOMETRY_TYPE()                       { return this.SPATIAL_TYPE }
                                                   
  static set GEOMETRY_TYPE(v)                      { Object.defineProperty(this,'GEOMETRY_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get GEOGRAPHY_TYPE()                      { return this.SPATIAL_TYPE }
                                                   
  static set GEOGRAPHY_TYPE(v)                     { Object.defineProperty(this,'GEOGRAPHY_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get POINT_TYPE()                          { return this.SPATIAL_TYPE }
                                                   
  static set POINT_TYPE(v)                         { Object.defineProperty(this,'POINT_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get LINE_TYPE()                           { return this.SPATIAL_TYPE }
                                                    
  static set LINE_TYPE(v)                          { Object.defineProperty(this,'LINE_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get POLYGON_TYPE()                        { return this.SPATIAL_TYPE }
                                                   
  static set POLYGON_TYPE(v)                       { Object.defineProperty(this,'POLYGON_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MULTI_POINT_TYPE()                    { return this.SPATIAL_TYPE }
                                                   
  static set MULTI_POINT_TYPE(v)                   { Object.defineProperty(this,'MULTI_POINT_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MULTI_LINE_TYPE()                     { return this.SPATIAL_TYPE }
                                                   
  static set MULTI_LINE_TYPE(v)                    { Object.defineProperty(this,'MULTI_LINE_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MULTI_POLYGON_TYPE()                  { return this.SPATIAL_TYPE }
                                                   
  static set MULTI_POLYGON_TYPE(v)                 { Object.defineProperty(this,'MULTI_POLYGON_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get GEOMETRY_COLLECTION_TYPE()            { return this.SPATIAL_TYPE }
                                                   
  static set GEOMETRY_COLLECTION_TYPE(v)           { Object.defineProperty(this,'GEOMETRY_COLLECTION_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get BOX_TYPE()                            { return this.SPATIAL_TYPE }
                                                   
  static set BOX_TYPE(v)                           { Object.defineProperty(this,'BOX_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get PATH_TYPE()                           { return this.SPATIAL_TYPE }
                                                   
  static set PATH_TYPE(v)                          { Object.defineProperty(this,'PATH_TYPE', { get() { return(v) }, configurable: true })}
  
  // Other Common Types                            
                                                   
  static get UUID_TYPE()                           { return `${this.VARCHAR_TYPE}(36)` }
                                                   
  static set UUID_TYPE(v)                          { Object.defineProperty(this,'UUID_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // User Defined Types                            
                                                   
  static get USER_DEFINED_TYPE()                   { return this.CLOB_TYPE }
                                                   
  static set USER_DEFINED_TYPE(v)                  { Object.defineProperty(this,'USER_DEFINED_TYPE', { get() { return(v) }, configurable: true })}

  // Oracle Specific Types                         
                                                   
  static get ORACLE_ROWID_TYPE()                   { return `${this.VARCHAR_TYPE}(32)` }
                                                   
  static set ORACLE_ROWID_TYPE(v)                  { Object.defineProperty(this,'ORACLE_ROWID_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get ORACLE_BFILE_TYPE()                   { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  static set ORACLE_BFILE_TYPE(v)                  { Object.defineProperty(this,'ORACLE_BFILE_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get ORACLE_ANYDATA_TYPE()                 { return this.CLOB_TYPE  }
                                                   
  static set ORACLE_ANYDATA_TYPE(v)                { Object.defineProperty(this,'ORACLE_ANYDATA_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get ORACLE_OBJECT_TYPE()                  { return this.CLOB_TYPE }
                                                   
  static set ORACLE_OBJECT_TYPE(v)                 { Object.defineProperty(this,'ORACLE_OBJECT_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get ORACLE_UNBOUNDED_NUMBER_TYPE()        { return this.MAX_NUMERIC_TYPE }
                                                   
  static set ORACLE_UNBOUNDED_NUMBER_TYPE(v)       { Object.defineProperty(this,'ORACLE_UNBOUNDED_NUMBER_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  // SQL Server Specific Types                     
                                                   
  static get MSSQL_MONEY_TYPE()                    { return `${this.NUMERIC_TYPE}(19,4)` }
                                                   
  static set MSSQL_MONEY_TYPE(v)                   { Object.defineProperty(this,'MSSQL_MONEY_TYPE', { get() { return(v) }, configurable: true })}

  static get MSSQL_SMALLMONEY_TYPE()               { return `${this.NUMERIC_TYPE}(10,4)` }
                                                    
  static set MSSQL_SMALLMONEY_TYPE(v)              { Object.defineProperty(this,'MSSQL_SMALLMONEY_TYPE', { get() { return(v) }, configurable: true })}

  static get MSSQL_SMALLDATETIME_TYPE()            { return this.DATETIME_TYPE }
                                                   
  static set MSSQL_SMALLDATETIME_TYPE(v)           { Object.defineProperty(this,'MSSQL_SMALLDATETIME_TYPE', { get() { return(v) }, configurable: true })}

  static get MSSQL_DATETIME2_TYPE()                { return this.DATETIME_TYPE }
                                                   
  static set MSSQL_DATETIME2_TYPE(v)               { Object.defineProperty(this,'MSSQL_DATETIME2_TYPE', { get() { return(v) }, configurable: true })}

  static get MSSQL_ROWVERSION_TYPE()               { return `${this.BINARY_TYPE}(8)` }
                                                   
  static set MSSQL_ROWVERSION_TYPE(v)              { Object.defineProperty(this,'MSSQL_ROWVERSION_TYPE', { get() { return(v) }, configurable: true })}

  static get MSSQL_HIERARCHY_ID_TYPE()             { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  static set MSSQL_HIERARCHY_ID_TYPE(v)            { Object.defineProperty(this,'MSSQL_HIERARCHY_ID_TYPE', { get() { return(v) }, configurable: true })}

  // Postgres Specific Types                       
                                                   
  static get PGSQL_SINGLE_CHAR_TYPE()              { return `${this.CHAR_TYPE}(1)` }
                                                   
  static set PGSQL_SINGLE_CHAR_TYPE(v)             { Object.defineProperty(this,'PGSQL_SINGLE_CHAR_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_BPCHAR_TYPE()                   { return this.CHAR_TYPE }
                                                   
  static set PGSQL_BPCHAR_TYPE(v)                  { Object.defineProperty(this,'PGSQL_BPCHAR_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_NAME_TYPE()                     { return `${this.VARCHAR_TYPE}(64)` }
                                                   
  static set PGSQL_NAME_TYPE(v)                    { Object.defineProperty(this,'PGSQL_NAME_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_MONEY_TYPE()                    { return `${this.NUMERIC_TYPE}(21,2)` }
  
  static set PGSQL_MONEY_TYPE(v)                   { Object.defineProperty(this,'PGSQL_MONEY_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_BINARY_JSON_TYPE()              { return this.JSON_TYPE }                                 
  
  static set PGSQL_BINARY_JSON_TYPE(v)             { Object.defineProperty(this,'PGSQL_BINARY_JSON_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_LINE_EQ_TYPE()                  { return this.JSON_TYPE }
                                                   
  static set PGSQL_LINE_EQ_TYPE(v)                 { Object.defineProperty(this,'PGSQL_LINE_EQ_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_CIRCLE_TYPE()                   { return this.JSON_TYPE }
                                                   
  static set PGSQL_CIRCLE_TYPE(v)                  { Object.defineProperty(this,'PGSQL_CIRCLE_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_CIDR_ADDR_TYPE()                { return `${this.VARCHAR_TYPE}(39)` }
  
  static set PGSQL_CIDR_ADDR_TYPE(v)               { Object.defineProperty(this,'PGSQL_CIDR_ADDR_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_INET_ADDR_TYPE()                { return `${this.VARCHAR_TYPE}(39)` }
                                                   
  static set PGSQL_INET_ADDR_TYPE(v)               { Object.defineProperty(this,'PGSQL_INET_ADDR_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_MAC_ADDR_TYPE()                 { return `${this.VARCHAR_TYPE}(23)` }
  
  static set PGSQL_MAC_ADDR_TYPE(v)                { Object.defineProperty(this,'PGSQL_MAC_ADDR_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_MAC_ADDR8_TYPE()                { return `${this.VARCHAR_TYPE}(23)` }
  
  static set PGSQL_MAC_ADDR8_TYPE(v)               { Object.defineProperty(this,'PGSQL_MAC_ADDR8_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_RANGE_INT4_TYPE()               { return this.JSON_TYPE } // "int4range"                                        
  
  static set PGSQL_RANGE_INT4_TYPE(v)              { Object.defineProperty(this,'PGSQL_RANGE_INT4_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_RANGE_INT8_TYPE()               { return this.JSON_TYPE } //  "int8range"                                        
  
  static set PGSQL_RANGE_INT8_TYPE(v)              { Object.defineProperty(this,'PGSQL_RANGE_INT8_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_RANGE_NUM_TYPE()                { return this.JSON_TYPE } //  "numrange"                                         
  
  static set PGSQL_RANGE_NUM_TYPE(v)               { Object.defineProperty(this,'PGSQL_RANGE_NUM_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_RANGE_TIMESTAMP_TYPE()          { return this.JSON_TYPE } //  "tsrange"                                          
  
  static set PGSQL_RANGE_TIMESTAMP_TYPE(v)         { Object.defineProperty(this,'PGSQL_RANGE_TIMESTAMP_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_RANGE_TIMESTAMP_TZ_TYPE()       { return this.JSON_TYPE } //  "tstzrange"                                        
  
  static set PGSQL_RANGE_TIMESTAMP_TZ_TYPE(v)      { Object.defineProperty(this,'PGSQL_RANGE_TIMESTAMP_TZ_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_RANGE_DATE_TYPE()               { return this.JSON_TYPE } //  "daterange"                                        
  
  static set PGSQL_RANGE_DATE_TYPE(v)              { Object.defineProperty(this,'PGSQL_RANGE_DATE_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_TIMESTAMP_VECTOR()              { return this.JSON_TYPE } //  "tsvector"                                         
  
  static set PGSQL_TIMESTAMP_VECTOR(v)             { Object.defineProperty(this,'PGSQL_TIMESTAMP_VECTOR', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_TEXTSEACH_VECTOR_TYPE()         { return this.JSON_TYPE } //  "gtsvector"                                        
  
  static set PGSQL_TEXTSEACH_VECTOR_TYPE(v)        { Object.defineProperty(this,'PGSQL_TEXTSEACH_VECTOR_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_TEXTSEACH_QUERY_TYPE()          { return this.MAX_VARCHAR_TYPE} // "tsquery"                                                       
  
  static set PGSQL_TEXTSEACH_QUERY_TYPE(v)         { Object.defineProperty(this,'PGSQL_TEXTSEACH_QUERY_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_IDENTIFIER_TYPE()               { return `${this.NUMERIC_TYPE}(10,0)` } // Unsigned 4 Byte (32 bit Integer). Alternates include BIGINT, Unsigned Int. Byte[4]
                                                   
  static set PGSQL_IDENTIFIER_TYPE(v)              { Object.defineProperty(this,'PGSQL_IDENTIFIER_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_OID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // "oid"                                              
  
  static set PGSQL_OID_TYPE(v)                     { Object.defineProperty(this,'PGSQL_OID_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_CLASS_TYPE()                { return this.PGSQL_IDENTIFIER_TYPE } // "regclass"                                         
  
  static set PGSQL_REG_CLASS_TYPE(v)               { Object.defineProperty(this,'PGSQL_REG_CLASS_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_COLLATION_TYPE()            { return this.PGSQL_IDENTIFIER_TYPE } // "regcollation"                                     
  
  static set PGSQL_REG_COLLATION_TYPE(v)           { Object.defineProperty(this,'PGSQL_REG_COLLATION_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_TEXTSEARCH_CONFIG_TYPE()    { return this.PGSQL_IDENTIFIER_TYPE } // "regconfig"                                        
  
  static set PGSQL_REG_TEXTSEARCH_CONFIG_TYPE(v)   { Object.defineProperty(this,'PGSQL_REG_TEXTSEARCH_CONFIG_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_TEXTSEARCH_DICT_TYPE()      { return this.PGSQL_IDENTIFIER_TYPE } // "regdictionary"                                    
  
  static set PGSQL_REG_TEXTSEARCH_DICT_TYPE(v)     { Object.defineProperty(this,'PGSQL_REG_TEXTSEARCH_DICT_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_NAMESPACE_TYPE()            { return this.PGSQL_IDENTIFIER_TYPE } // "regnamespace"                                     
  
  static set PGSQL_REG_NAMESPACE_TYPE(v)           { Object.defineProperty(this,'PGSQL_REG_NAMESPACE_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_OPERATOR_NAME_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regoper"                                          
  
  static set PGSQL_REG_OPERATOR_NAME_TYPE(v)       { Object.defineProperty(this,'PGSQL_REG_OPERATOR_NAME_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_OPERATOR_ARGS_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regoperator"                                      
  
  static set PGSQL_REG_OPERATOR_ARGS_TYPE(v)       { Object.defineProperty(this,'PGSQL_REG_OPERATOR_ARGS_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_FUNCTION_NAME_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regproc"                                          
  
  static set PGSQL_REG_FUNCTION_NAME_TYPE(v)       { Object.defineProperty(this,'PGSQL_REG_FUNCTION_NAME_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_FUNCTION_ARGS_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regprocedure"                                     
  
  static set PGSQL_REG_FUNCTION_ARGS_TYPE(v)       { Object.defineProperty(this,'PGSQL_REG_FUNCTION_ARGS_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_ROLE_TYPE()                 { return this.PGSQL_IDENTIFIER_TYPE } // "regrole"                                          
  
  static set PGSQL_REG_ROLE_TYPE(v)                { Object.defineProperty(this,'PGSQL_REG_ROLE_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REG_TYPE_TYPE()                 { return this.PGSQL_IDENTIFIER_TYPE } // "regtype"                                          0
  
  static set PGSQL_REG_TYPE_TYPE(v)                { Object.defineProperty(this,'PGSQL_REG_TYPE_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_TID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // tid"                                              
  
  static set PGSQL_TID_TYPE(v)                     { Object.defineProperty(this,'PGSQL_TID_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_XID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // "xid"                                              
  
  static set PGSQL_XID_TYPE(v)                     { Object.defineProperty(this,'PGSQL_XID_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_CID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // "cid"                                              
  
  static set PGSQL_CID_TYPE(v)                     { Object.defineProperty(this,'PGSQL_CID_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_TXID_SNAPSHOT_TYPE()            { return this.PGSQL_IDENTIFIER_TYPE } // "txid_snapshot"                                    
  
  static set PGSQL_TXID_SNAPSHOT_TYPE(v)           { Object.defineProperty(this,'PGSQL_TXID_SNAPSHOT_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_GTSVECTOR_TYPE()                { return this.JSON_TYPE } // "gtsvector"                                    
  
  static set PGSQL_GTSVECTOR_TYPE(v)               { Object.defineProperty(this,'PGSQL_GTSVECTOR_TYPE', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_ACLITEM()                       { return this.JSON_TYPE } // "aclitem"                                          
  
  static set PGSQL_ACLITEM(v)                      { Object.defineProperty(this,'PGSQL_ACLITEM', { get() { return(v) }, configurable: true })}
  
  static get PGSQL_REFCURSOR()                     { return this.JSON_TYPE } // "refcursor"       
  
  static set PGSQL_REFCURSOR(v)                    { Object.defineProperty(this,'PGSQL_REFCURSOR', { get() { return(v) }, configurable: true })}
  
  // MySQL Specific Types
  
  static get MYSQL_SET_TYPE()                      { return this.JSON_TYPE }
                                                   
  static set MYSQL_SET_TYPE(v)                     { Object.defineProperty(this,'MYSQL_SET_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_ENUM_TYPE()                     { return `${this.VARCHAR_TYPE}(512)` }

  static set MYSQL_ENUM_TYPE(v)                    { Object.defineProperty(this,'MYSQL_ENUM_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MYSQL_LONGTEXT_TYPE()                 { return this.CLOB_TYPE }
                                                   
  static set MYSQL_LONGTEXT_TYPE(v)                { Object.defineProperty(this,'MYSQL_LONGTEXT_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_MEDIUMTEXT_TYPE()               { return this.CLOB_TYPE }
                                                   
  static set MYSQL_MEDIUMTEXT_TYPE(v)              { Object.defineProperty(this,'MYSQL_MEDIUMTEXT_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_TEXT_TYPE()                     { return this.CLOB_TYPE }
                                                   
  static set MYSQL_TEXT_TYPE(v)                    { Object.defineProperty(this,'MYSQL_TEXT_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_TINYTEXT_TYPE()                 { return `${this.VARCHAR_TYPE}(255)` }
                                                   
  static set MYSQL_TINYTEXT_TYPE(v)                { Object.defineProperty(this,'MYSQL_TINYTEXT_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_LONGBLOB_TYPE()                 { return this.BLOB_TYPE }
                                                   
  static set MYSQL_LONGBLOB_TYPE(v)                { Object.defineProperty(this,'MYSQL_LONGBLOB_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_MEDIUMBLOB_TYPE()               { return this.BLOB_TYPE }
                                                   
  static set MYSQL_MEDIUMBLOB_TYPE(v)              { Object.defineProperty(this,'MYSQL_MEDIUMBLOB_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_BLOB_TYPE()                     { return this.BLOB_TYPE }
                                                   
  static set MYSQL_BLOB_TYPE(v)                    { Object.defineProperty(this,'MYSQL_BLOB_TYPE', { get() { return(v) }, configurable: true })}

  static get MYSQL_TINYBLOB_TYPE()                 { return `${this.VARBINARY_TYPE}(255)` }
                                                   
  static set MYSQL_TINYBLOB_TYPE(v)                { Object.defineProperty(this,'MYSQL_TINYBLOB_TYPE', { get() { return(v) }, configurable: true })}

  // Mongo Specific Types                          
                                                   
  static get MONGO_OBJECTID_TYPE()                 { return `${this.BINARY_TYPE}(12)` }
                                                   
  static set MONGO_OBJECTID_TYPE(v)                { Object.defineProperty(this,'MONGO_OBJECTID_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_DECIMAL128_TYPE()               { return this.MAX_NUMERIC_TYPE }
                                                   
  static set MONGO_DECIMAL128_TYPE(v)              { Object.defineProperty(this,'MONGO_DECIMAL128_TYPE', { get() { return(v) }, configurable: true })}
                                                   
  static get MONGO_ARRAY_TYPE()                    { return this.JSON_TYPE }
                                                   
  static set MONGO_ARRAY_TYPE(v)                   { Object.defineProperty(this,'MONGO_ARRAY_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_OBJECT_TYPE()                   { return this.JSON_TYPE }
                                                   
  static set MONGO_OBJECT_TYPE(v)                  { Object.defineProperty(this,'MONGO_OBJECT_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_NULL_TYPE()                     { return `${this.VARCHAR_TYPE}(4)` }
                                                   
  static set MONGO_NULL_TYPE(v)                    { Object.defineProperty(this,'MONGO_NULL_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_REGEX_TYPE()                    { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  static set MONGO_REGEX_TYPE(v)                   { Object.defineProperty(this,'MONGO_REGEX_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_JS_TYPE()                       { return this.CLOB_TYPE }
                                                   
  static set MONGO_JS_TYPE(v)                      { Object.defineProperty(this,'MONGO_JS_TYPE', { get() { return(v) }, configurable: true })}
   
  static get MONGO_SCOPED_JS_TYPE()                { return this.CLOB_TYPE }
                                                   
  static set MONGO_SCOPED_JS_TYPE(v)               { Object.defineProperty(this,'MONGO_SCOPED_JS_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_MINKEY_TYPE()                   { return this.JSON_TYPE }
                                                   
  static set MONGO_MINKEY_TYPE(v)                  { Object.defineProperty(this,'MONGO_MINKEY_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_MAXKEY_TYPE()                   { return this.JSON_TYPE }
                                                   
  static set MONGO_MAXKEY_TYPE(v)                  { Object.defineProperty(this,'MONGO_MAXKEY_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_UNDEFINED_TYPE()                { return `${this.VARCHAR_TYPE}(8)` }
                                                   
  static set MONGO_UNDEFINED_TYPE(v)               { Object.defineProperty(this,'MONGO_UNDEFINED_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_REGEX_TYPE()                    { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  static set MONGO_REGEX_TYPE(v)                   { Object.defineProperty(this,'MONGO_REGEX_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_DBPOINTER_TYPE()                { return `${this.VARCHAR_TYPE}(36)` }
                                                   
  static set MONGO_DBPOINTER_TYPE(v)               { Object.defineProperty(this,'MONGO_DBPOINTER_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_FUNCTION_TYPE()                 { return this.CLOB_TYPE }
                                                   
  static set MONGO_FUNCTION_TYPE(v)                { Object.defineProperty(this,'MONGO_FUNCTION_TYPE', { get() { return(v) }, configurable: true })}

  static get MONGO_SYMBOL_TYPE()                   { return `${this.VARCHAR_TYPE}(36)` }
  
  static set MONGO_SYMBOL_TYPE(v)                  { Object.defineProperty(this,'MONGO_SYMBOL_TYPE', { get() { return(v) }, configurable: true })}

  // Default Maxiumum Lengths
  
  static get SCALAR_LENGTH()                       {
    throw new YadamuError(`Must supply explicit data type mapping for 'SCALAR_LENGTH'`)
  }
  
  static set SCALAR_LENGTH(v)                      { Object.defineProperty(this,'SCALAR_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get CHAR_LENGTH()                         { return this.SCALAR_LENGTH }      
                                                   
  static set CHAR_LENGTH(v)                        { Object.defineProperty(this,'CHAR_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get VARCHAR_LENGTH()                      { return this.CHAR_LENGTH }    
                                                   
  static set VARCHAR_LENGTH(v)                     { Object.defineProperty(this,'VARCHAR_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get NCHAR_LENGTH()                        { return this.CHAR_LENGTH }    
                                                   
  static set NCHAR_LENGTH(v)                       { Object.defineProperty(this,'NCHAR_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get NVACHAR_LENGTH()                      { return this.CHAR_LENGTH }    
                                                   
  static set NVACHAR_LENGTH(v)                     { Object.defineProperty(this,'NVACHAR_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get BINARY_LENGTH()                       { return this.CHAR_LENGTH }    
                                                   
  static set BINARY_LENGTH(v)                      { Object.defineProperty(this,'BINARY_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get VARBINARY_LENGTH()                    { this.BINARY_LENGTH }     
                                                   
  static set VARBINARY_LENGTH(v)                   { Object.defineProperty(this,'VARBINARY_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get NUMERIC_PRECISION()                   { return 38 }
                                                   
  static set NUMERIC_PRECISION(v)                  { Object.defineProperty(this,'NUMERIC_PRECISION',   { get() { return(v) }, configurable: true })}
                                                   
  static get NUMERIC_SCALE()                       { return 38 }
                                                   
  static set NUMERIC_SCALE(v)                      { Object.defineProperty(this,'NUMERIC_SCALE',   { get() { return(v) }, configurable: true })}
                                                   
  static get TIMESTAMP_PRECISION()                 { return 6 }

  static set TIMESTAMP_PRECISION(v)                { Object.defineProperty(this,'TIMESTAMP_PRECISION',   { get() { return(v) }, configurable: true })}
                                                   
  static get LOB_LENGTH()                          { return -1 }      
                                                   
  static set LOB_LENGTH(v)                         { Object.defineProperty(this,'LOB_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get CLOB_LENGTH()                         { return this.LOB_LENGTH }     
                                                   
  static set CLOB_LENGTH(v)                        { Object.defineProperty(this,'CLOB_LENGTH',   { get() { return(v) }, configurable: true })}
                                                   
  static get BLOB_LENGTH()                         { return this.LOB_LENGTH }     
                                                   
  static set BLOB_LENGTH(v)                        { Object.defineProperty(this,'BLOB_LENGTH',   { get() { return(v) }, configurable: true })}

  static get BOOLEAN_TYPES() {
    return this._BOOLEAN_TYPES || (() => {
	  this._BOOLEAN_TYPES = Object.freeze([this.BOOLEAN_TYPE])
	  return this._BOOLEAN_TYPES
	})()
  }

  static get INTEGER_TYPES() { 
    return this._INTEGER_TYPES || (() => {
	  this._INTEGER_TYPES = Object.freeze([this.INTEGER_TYPE,this.TINYINT_TYPE,this.SMALLINT_TYPE,this.MEDIUMINT_TYPE,this.BIGINT_TYPE])
	  return this._INTEGER_TYPES
	})()
  }

  static get FLOAT_TYPES() { 
    return this._INTEGER_TYPES || (() => {
	  this._INTEGER_TYPES = Object.freeze([this.FLOAT_TYPE,this.REAL_TYPE,this.DOUBLE_TYPE])
	  return this._INTEGER_TYPES
	})()
  }

  static get SPATIAL_TYPES() { 
    return this._SPATIAL_TYPES || (() => {
	  this._SPATIAL_TYPES = Object.freeze([this.SPATIAL_TYPE,this.GEOMETRY_TYPE,this.GEOGRAPHY_TYPE,this.POINT_TYPE,this.LINE_TYPE,this.POLYGON_TYPE,this.MULTI_POINT_TYPE,this.MULTI_LINE_TYPE,this.MULTI_POLYGON_TYPE,this.GEOMETRY_COLLECTION_TYPE,this.BOX_TYPE,this.PATH_TYPE])
	  return this._SPATIAL_TYPES
	})()
  }

  static get WELLKNOWN_BOOLEAN_TYPES() {
    return this._WELLKNOWN_BOOLEAN_TYPES || (() => {
	  this._WELLKNOWN_BOOLEAN_TYPES = Object.freeze([...DataTypeClassification.boolean])
	  return this._WELLKNOWN_BOOLEAN_TYPES
	})()
  }

  static isBoolean(dataType,length,vendor) {
	//BIT is boolean in SQL Server, bit BIT STRING in Postgres
    if ((vendor === 'Postgres') && (dataType.toUpperCase() === 'BIT')) return false
    return (this.WELLKNOWN_BOOLEAN_TYPES.includes(dataType.toUpperCase()) && ((length === 1) || (isNaN(length))));
  }

  static get WELLKNOWN_BINARY_TYPES() {
    return this._WELLKNOWN_BINARY_TYPES || (() => {
	  this._WELLKNOWN_BINARY_TYPES = Object.freeze([...DataTypeClassification.binary])
	  return this._WELLKNOWN_BINARY_TYPES
	})()
  }

  static isBinary(dataType) {
    return this.WELLKNOWN_BINARY_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_DATE_TYPES() {
    return this._WELLKNOWN_DATE_TYPES || (() => {
	  this._WELLKNOWN_DATE_TYPES = Object.freeze([...DataTypeClassification.date])
	  return this._WELLKNOWN_DATE_TYPES
	})()
  }
   
  static isDate(dataType) {
	return this.WELLKNOWN_DATE_TYPES.includes(dataType.toUpperCase());
  }
   
  static get WELLKNOWN_TIME_TYPES() {
    return this._WELLKNOWN_TIME_TYPES || (() => {
	  this._WELLKNOWN_TIME_TYPES = Object.freeze([...DataTypeClassification.time])
	  return this._WELLKNOWN_TIME_TYPES
	})()
  }
   
  static isTime(dataType) {
    return this.WELLKNOWN_TIME_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_DATETIME_TYPES() {
    return this._WELLKNOWN_DATETIME_TYPES || (() => {
	  this._WELLKNOWN_DATETIME_TYPES = Object.freeze([...DataTypeClassification.datetime])
	  return this._WELLKNOWN_DATETIME_TYPES
	})()
  }
   
  static isDateTime(dataType) {
    return this.WELLKNOWN_DATETIME_TYPES.includes(dataType.toUpperCase());
  }
      
  static get WELLKNOWN_TIMESTAMP_TYPES() {
	return this._WELLKNOWN_TIMESTAMP_TYPES || (() => {
	  this._WELLKNOWN_TIMESTAMP_TYPES = Object.freeze([...DataTypeClassification.timestamp])
	  return this._WELLKNOWN_TIMESTAMP_TYPES
	})()
  }
   
  static isTimestamp(dataType) {
    return this.WELLKNOWN_TIMESTAMP_TYPES.includes(dataType.toUpperCase());
  }
      
  static get WELLKNOWN_TEMPORAL_TYPES() {
    return this._WELLKNOWN_TEMPORAL_TYPES || (() => {
	  this._WELLKNOWN_TEMPORAL_TYPES = Object.freeze([...this.WELLKNOWN_DATE_TYPES,...this.WELLKNOWN_TIME_TYPES,...this.WELLKNOWN_DATETIME_TYPES,...this.WELLKNOWN_TIMESTAMP_TYPES])
	  return this._WELLKNOWN_TEMPORAL_TYPES
	})()
  }
      
  static isTemporal(dataType) {
    return this.WELLKNOWN_TEMPORAL_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_INTERVAL_TYPES() {
    return this._WELLKNOWN_INTERVAL_TYPES || (() => {
	  this._WELLKNOWN_INTERVAL_TYPES = Object.freeze([...DataTypeClassification.interval])
	  return this._WELLKNOWN_INTERVAL_TYPES
	})()
  }
   
  static isInterval(dataType) {
    return this.WELLKNOWN_INTERVAL_TYPES.includes(dataType.toUpperCase());
  }
   

  static get WELLKNOWN_INTEGER_TYPES() {
    return this._WELLKNOWN_INTEGER_TYPES || (() => {
	  this._WELLKNOWN_INTEGER_TYPES = Object.freeze([...DataTypeClassification.integer])
	  return this._WELLKNOWN_INTEGER_TYPES
	})()
  }

  static isInteger(dataType) {
    return this.WELLKNOWN_INTEGER_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_FLOATING_POINT_TYPES() {
    return this._WELLKNOWN_FLOATING_POINT_TYPES || (() => {
	  this._WELLKNOWN_FLOATING_POINT_TYPES = Object.freeze([...DataTypeClassification.floatingPoint])
	  return this._WELLKNOWN_FLOATING_POINT_TYPES
	})()
  }

  static isFloatingPoint(dataType) {
    return this.WELLKNOWN_FLOATING_POINT_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_BCD_TYPES() {
    return this._WELLKNOWN_BCD_TYPES || (() => {
	  this._WELLKNOWN_BCD_TYPES = Object.freeze([...DataTypeClassification.bcd])
	  return this._WELLKNOWN_BCD_TYPES
	})()
  }

  static isBCD(dataType) {
    return this.WELLKNOWN_BCD_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_NUMERIC_TYPES() {
    return this._WELLKNOWN_NUMERIC_TYPES || (() => {
	  this._WELLKNOWN_NUMERIC_TYPES = Object.freeze([...this.WELLKNOWN_INTEGER_TYPES,...this.WELLKNOWN_FLOATING_POINT_TYPES,...this.WELLKNOWN_BCD_TYPES])
	  return this._WELLKNOWN_NUMERIC_TYPES
	})()
  }

  static isNumeric(dataType) {
    return this.WELLKNOWN_NUMERIC_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_XML_TYPES() {
    return this._WELLKNOWN_XML_TYPES || (() => {
	  this._WELLKNOWN_XML_TYPES = Object.freeze([...DataTypeClassification.xml])
	  return this._WELLKNOWN_XML_TYPES
	})()
  }

  static isXML(dataType) {
    return this.WELLKNOWN_XML_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_JSON_TYPES() {
    return this._WELLKNOWN_JSON_TYPES || (() => {
	  this._WELLKNOWN_JSON_TYPES = Object.freeze([...DataTypeClassification.json])
	  return this._WELLKNOWN_JSON_TYPES
	})()
  }

  static isJSON(dataType) {
    return this.WELLKNOWN_JSON_TYPES.includes(dataType.toUpperCase());
  }
  
  static get WELLKNOWN_CLOB_TYPES() {
    return this._WELLKNOWN_CLOB_TYPES || (() => {
	  this._WELLKNOWN_CLOB_TYPES = Object.freeze([...DataTypeClassification.clob])
	  return this._WELLKNOWN_CLOB_TYPES
	})()
  }

  static isCLOB(dataType) {
    return this.WELLKNOWN_CLOB_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_BLOB_TYPES() {
    return this._WELLKNOWN_BLOB_TYPES || (() => {
	  this._WELLKNOWN_BLOB_TYPES = Object.freeze([...DataTypeClassification.blob])
	  return this._WELLKNOWN_BLOB_TYPES
	})()
  }

  static isBLOB(dataType) {
    return this.WELLKNOWN_BLOB_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_LOB_TYPES() {
    return this._WELLKNOWN_LOB_TYPES || (() => {
	  this._WELLKNOWN_LOB_TYPES = Object.freeze([...this.WELLKNOWN_CLOB_TYPES,this.WELLKNOWN_BLOB_TYPES])
	  return this._WELLKNOWN_LOB_TYPES
	})()
  }

  static isLOB(dataType) {
    return this.WELLKNOWN_BLOB_TYPES.includes(dataType.toUpperCase()) || this.WELLKNOWN_CLOB_TYPES.includes(dataType.toUpperCase());
  }

  static get WELLKNOWN_SPATIAL_TYPES() {
    return this._WELLKNOWN_SPATIAL_TYPES || (() => {
	  this._WELLKNOWN_SPATIAL_TYPES = Object.freeze([...DataTypeClassification.spatial])
	  return this._WELLKNOWN_SPATIAL_TYPES
	})()
  }

  static isSpatial(dataType) {
    return this.WELLKNOWN_SPATIAL_TYPES.includes(dataType.toUpperCase());
  }

  static composeDataType(dataType, sizeConstraint) {
    
    const dataTypDefiniton = {
      type : dataType
    }    

    if ((sizeConstraint !== null) && (sizeConstraint.length > 0)) {
      const components = sizeConstraint.split(',');
      dataTypDefiniton.length = parseInt(components[0])
      if (components.length > 1) {
        dataTypDefiniton.scale = parseInt(components[1])
      }
    }
    
    return dataTypDefiniton
  }
  
  static decomposeDataType(dataType) {
	  
	// NUMBER(n,m) => {type:"NUMBER", length:n, scale:m}
	// VARCHAR(n)  => {type:"VARCHAR", length:n}
	// LONG VARCHAR(n) => {type:"LONG VARCHAR", length:n}
	// TIMETSTAMP(n) WITH TIME ZONE => {type:"TIMESTAMP WITH TIME ZONE": length:n}
	
	// ### Need to test with "interval year(4) to month (2)"

    const typeDefinition = {};
    let components = dataType.split('(');
	switch (components.length) {
	  case 1:
	    typeDefinition.type = dataType.trim().replace(/  +/g, ' ')
	    return typeDefinition
    }	    
    
    typeDefinition.type = components[0]
    let sizeComponents = components[1].split(')')
    typeDefinition.type = (sizeComponents.length > 1 ) ? `${typeDefinition.type} ${sizeComponents[1]}` : typeDefinition.type.trim()
	typeDefinition.type = typeDefinition.type.trim().replace(/  +/g, ' ')

    sizeComponents = sizeComponents[0].split(',')
    typeDefinition.length = sizeComponents[0] === 'max' ? -1 :  parseInt(sizeComponents[0])
	if (sizeComponents.length > 1) {
      typeDefinition.scale = parseInt(sizeComponents[1])
    }	 	
	
    return typeDefinition;      
    
  } 
  
  static decomposeDataTypes(dataTypes) {
     return dataTypes.map((dataType) => {
       return this.decomposeDataType(dataType)
     })
  }

  static coalesceTypeMappings(typeList) {
	
	switch (true) {
	  case typeList.includes(this.SPATIAL_TYPE):
	    return this.SPATIAL_TYPE
	  case typeList.includes(this.GEOMETRY_TYPE):
	    return this.GEOMETRY_TYPE
      case typeList.includes(this.GEOGRAPHY_TYPE):
	    return this.GEOGRAPHY_TYPE
      case typeList.includes(this.JSON_TYPE):
	    return this.JSON_TYPE
      case typeList.includes(this.CLOB_TYPE):
	    return this.CLOB_TYPE
      case typeList.includes(this.VARCHAR_TYPE):
	    return this.VARCHAR_TYPE
      case typeList.includes(this.CHAR_TYPE):
	    return this.CHAR_TYPE
      case typeList.includes(this.BLOB_TYPE):
	    return this.BLOB_TYPE
      case typeList.includes(this.VARBINARY_TYPE):
	    return this.VARBINARY_TYPE
      case typeList.includes(this.BINARY_TYPE):
	    return this.BINARY_TYPE
	  case typeList.includes(this.BIGINT_TYPE):
	    return this.BIGINT_TYPE		
	  case typeList.includes(this.INTEGER_TYPE):
	    return this.INTEGER_TYPE		
	  case typeList.includes(this.MEDIUMINT_TYPE):
	    return this.MEDIUMINT_TYPE		
	  case typeList.includes(this.SMALLINT_TYPE):
	    return this.SMALLINT_TYPE		
      case typeList.includes(this.DOUBLE_TYPE):
	    return this.DOUBLE_TYPE
      case typeList.includes(this.NUMERIC_TYPE):
	    return this.NUMERIC_TYPE
      case typeList.includes(this.TIMESTAMP_TZ_TYPE):
	    return this.TIMESTAMP_TZ_TYPE
      case typeList.includes(this.TIMESTAMP_TYPE):
	    return this.TIMESTAMP_TYPE
      case typeList.includes(this.TIME_TZ_TYPE):
	    return this.TIME_TZ_TYPE
	  default:
        console.log(this.name,'Type List Reduction failed for ',typeList)
	}
  }
  
}

export { YadamuDataTypes as default }