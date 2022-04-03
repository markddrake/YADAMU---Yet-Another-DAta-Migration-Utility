
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

import DBIConstants                          from './dbiConstants.js';

const  __filename             = fileURLToPath(import.meta.url)
const __dirname               = dirname(__filename)
const DataTypeConfiguration   = JSON.parse(fs.readFileSync(join(__dirname,'../cfg/dataTypeConfiguration.json'),'utf-8'))
const DataTypeClassification  = JSON.parse(fs.readFileSync(join(__dirname,'../cfg/dataTypeClassification.json'),'utf-8'))

class YadamuStorageOptions {
	
  get SPATIAL_FORMAT()                      { return DBIConstants.SPATIAL_FORMAT }
  
  set SPATIAL_FORMAT(v)                     { YadamuDataTypes.redefineProperty(this,'SPATIAL_FORMAT',v) }

  get CIRCLE_FORMAT()                       { return 'CIRCLE' }  // VALID values are POLYGON (which returns a spatial POLYGON instance approximating the circle) and CIRCLE (which returns a GeoJSON representation of the equation that defines the circle).

  set CIRCLE_FORMAT(v)                      { YadamuDataTypes.redefineProperty(this,'CIRCLE_FORMAT',v) }

}

class YadamuDataTypes {

  static get DATA_TYPE_CONFIGURATION() {
    return DataTypeConfiguration
  }
  
  static redefineProperty(instance,name,v) {
	// Override Method in Type Hierachy 
	let obj = instance
	while (!obj.hasOwnProperty(name)) {obj = Object.getPrototypeOf(obj)}
	Object.defineProperty(instance,name, { get: () => {return v}, set: Object.getOwnPropertyDescriptor(obj,name).set, configurable: true })
  }
	   
  get TYPE_CONFIGURATION()        { 
    this._TYPE_CONFIGURATION = this._TYPE_CONFIGURATION || (() => {
      this._TYPE_CONFIGURATION = JSON.parse(fs.readFileSync(DataTypeConfiguration[this.DATABASE_VENDOR].file))
	  return this._TYPE_CONFIGURATION
    })()
    return this._TYPE_CONFIGURATION
  }

  constructor() {
	Object.assign(this,this.TYPE_CONFIGURATION.mappings)
	Object.assign(this,this.TYPE_CONFIGURATION.limits);
	this.storageOptions = this.STORAGE_OPTIONS 
	Object.assign(this.storageOptions,this.TYPE_CONFIGURATION.storageOptions || {});	  
  }

  get CHAR_TYPE()                           {
    throw new YadamuError(`Must supply explicit data type mapping for 'CHAR_TYPE'`)
  }
  
  set CHAR_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'CHAR_TYPE',v) }
  
  get BINARY_TYPE()                         {
    throw new YadamuError(`Must supply explicit data type mapping for 'BINARY_TYPE'`)
  }
  
  set BINARY_TYPE(v)                        { YadamuDataTypes.redefineProperty(this,'BINARY_TYPE',v) }
  
  get FLOAT_TYPE()                          {
    throw new YadamuError(`Must supply explicit data type mapping for 'FLOAT_TYPE'`)
  }
  
  set FLOAT_TYPE(v)                         { YadamuDataTypes.redefineProperty(this,'FLOAT_TYPE',v) }
  
  get NUMERIC_TYPE()                        {
    throw new YadamuError(`Must supply explicit data type mapping for 'NUMERIC_TYPE'`)
  }
  
  set NUMERIC_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'NUMERIC_TYPE',v) }
  
  get UNBOUNDED_NUMERIC_TYPE()              {
	  
    throw new YadamuError(`Must supply explicit data type mapping for 'UNBOUNDED_NUMERIC_TYPE'`)
  }
  
  set UNBOUNDED_NUMERIC_TYPE(v)             { YadamuDataTypes.redefineProperty(this,'UNBOUNDED_NUMERIC_TYPE',v) }
  
  get TIMESTAMP_TYPE()                      {
    throw new YadamuError(`Must supply explicit data type mapping for 'TIMESTAMP_TYPE'`)
  }
   
  set TIMESTAMP_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'TIMESTAMP_TYPE',v) }
                                                   
  // Characeter Types                              
                                                   
  get NCHAR_TYPE()                          { return this.CHAR_TYPE }
                                                   
  set NCHAR_TYPE(v)                         { YadamuDataTypes.redefineProperty(this,'NCHAR_TYPE',v) }
                                                   
  get VARCHAR_TYPE()                        { return this.CHAR_TYPE }
                                                   
  set VARCHAR_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'VARCHAR_TYPE',v) }
                                                   
  get NVARCHAR_TYPE()                       { return this.VARCHAR_TYPE }
                                                   
  set NVARCHAR_TYPE(v)                      { YadamuDataTypes.redefineProperty(this,'NVARCHAR_TYPE',v) }
                                                   
  get MAX_VARCHAR_TYPE()                    { return `${this.VARCHAR_TYPE}(${this.VARCHAR_LENGTH})` }
                                                   
  // Boolean Types                                 
                                                   
  get BOOLEAN_TYPE()                        { return 'BOOLEAN' }
                                                   
  set BOOLEAN_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'BOOLEAN_TYPE',v) }
                                                   
  // Bit Types                                     
                                                   
  get SINGLE_BIT_TYPE()                     { return `${this.BINARY_TYPE}(1)` }
                                                   
  set SINGLE_BIT_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'SINGLE_BIT_TYPE',v) }
  
  get BIT_STRING_TYPE()                     { return this.CHAR_TYPE }
                                                   
  set BIT_STRING_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'BIT_STRING_TYPE',v) }
  
  get VARBIT_STRING_TYPE()                  { return this.VARCHAR_TYPE }
                                                   
  set VARBIT_STRING_TYPE(v)                 { YadamuDataTypes.redefineProperty(this,'VARBIT_STRING_TYPE',v) }
  
  // Binary Types                                  
                                                   
  get VARBINARY_TYPE()                      { return this.BINARY_TYPE }
                                                   
  set VARBINARY_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'VARBINARY_TYPE',v) }
                                                   
  get MAX_VARBINARY_TYPE()                  { return `${this.VARBINARY_TYPE}(${this.VARBINARY_LENGTH})` }
                                                   
  // LOB Types                                     
                                                   
  get CLOB_TYPE()                           { return this.MAX_VARCHAR_TYPE }
                                                   
  set CLOB_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'CLOB_TYPE',v) }
                                                   
  get MAX_CLOB_TYPE()                       { return `${this.CLOB_TYPE}(${this.CLOB_LENGTH})` }
                                                   
  get NCLOB_TYPE()                          { return this.CLOB_TYPE }
                                                   
  set NCLOB_TYPE(v)                         { YadamuDataTypes.redefineProperty(this,'NCLOB_TYPE',v) }
                                                   
  get BLOB_TYPE()                           { return this.MAX_VARBINARY_TYPE }
                                                   
  set BLOB_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'BLOB_TYPE',v) }
                                                   
  get MAX_BLOB_TYPE()                       { return `${this.BLOB_TYPE}(${this.BLOB_LENGTH})` }
                                                   
  // Integer Types                                   
                                                   
  get INTEGER_TYPE()                        { return `${this.NUMERIC_TYPE}(19)` }
                                                   
  set INTEGER_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'INTEGER_TYPE',v) }
                                                   
  get TINYINT_TYPE()                        { return this.SMALLINT_TYPE }
                                                   
  set TINYINT_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'TINYINT_TYPE',v) }
                                                   
  get SMALLINT_TYPE()                       { return this.MEDIUMINT_TYPE }
                                                   
  set SMALLINT_TYPE(v)                      { YadamuDataTypes.redefineProperty(this,'SMALLINT_TYPE',v) }
                                                   
  get MEDIUMINT_TYPE()                      { return this.INTEGER_TYPE }
                                                   
  set MEDIUMINT_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'MEDIUMINT_TYPE',v) }
                                                   
  get BIGINT_TYPE()                         { return this.INTEGER_TYPE }
                                                   
  set BIGINT_TYPE(v)                        { YadamuDataTypes.redefineProperty(this,'BIGINT_TYPE',v) }
                                                   
  // Float Types                                   
                                                   
  get REAL_TYPE()                           { return this.FLOAT_TYPE }
                                                   
  get DOUBLE_TYPE()                         { return this.FLOAT_TYPE }
                                                   
  set DOUBLE_TYPE(v)                        { YadamuDataTypes.redefineProperty(this,'DOUBLE_TYPE',v) }
                                                   
  // Decimal Types                                   
                                                   
  get NUMBER_TYPE()                         { return this.NUMERIC_TYPE }
                                                   
  set NUMBER_TYPE(v)                        { YadamuDataTypes.redefineProperty(this,'NUMBER_TYPE',v) }
  
  get DECIMAL_TYPE()                        { return this.NUMERIC_TYPE }
                                                   
  set DECIMAL_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'DECIMAL_TYPE',v) }
  
  // Date, Time, Timestamp, Interval and Period Types
                                                   
  get DATE_TYPE()                           { return this.TIMESTAMP_TYPE }
                                                   
  set DATE_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'DATE_TYPE',v) }
                                                   
  get TIME_TYPE()                           { return this.TIMESTAMP_TYPE }
                                                   
  set TIME_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'TIME_TYPE',v) }
                                                   
  get TIME_TZ_TYPE()                        { return this.TIMESTAMP_TZ_TYPE }
                                                   
  set TIME_TZ_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'TIME_TZ_TYPE',v) }
                                                   
  get DATETIME_TYPE()                       { return this.TIMESTAMP_TYPE }
                                                   
  set DATETIME_TYPE(v)                      { YadamuDataTypes.redefineProperty(this,'DATETIME_TYPE',v) }
  
  get TIMESTAMP_TZ_TYPE()                   { return this.TIMESTAMP_TYPE }
                                                   
  set TIMESTAMP_TZ_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'TIMESTAMP_TZ_TYPE',v) }
                                                   
  get TIMESTAMP_LTZ_TYPE()                  { return this.TIMESTAMP_TZ_TYPE }
                                                   
  set TIMESTAMP_LTZ_TYPE(v)                 { YadamuDataTypes.redefineProperty(this,'TIMESTAMP_LTZ_TYPE',v) }
                                                   
  get INTERVAL_TYPE()                       { return `${this.VARCHAR_TYPE}(16)` } 
                                                   
  set INTERVAL_TYPE(v)                      { YadamuDataTypes.redefineProperty(this,'INTERVAL_TYPE',v) }
                                                   
  get INTERVAL_DAY_TO_SECOND_TYPE()         { return this.INTERVAL_TYPE }
                                                   
  set INTERVAL_DAY_TO_SECOND_TYPE(v)        { YadamuDataTypes.redefineProperty(this,'INTERVAL_DAY_TO_SECOND_TYPE',v) }
                                                   
  get INTERVAL_YEAR_TO_MONTH_TYPE()         { return this.INTERVAL_TYPE }
                                                   
  set INTERVAL_YEAR_TO_MONTH_TYPE(v)        { YadamuDataTypes.redefineProperty(this,'INTERVAL_YEAR_TO_MONTH_TYPE',v) }
                                                   
  get YEAR_TYPE()                            { return `${this.NUMERIC_TYPE}(4)` }
                                                   
  set YEAR_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'YEAR_TYPE',v) }

  // JSON and XML                                  
                                                   
  get JSON_TYPE()                           { return this.CLOB_TYPE }
                                                   
  set JSON_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'JSON_TYPE',v) }
                                                   
  get XML_TYPE()                            { return this.CLOB_TYPE }
                                                   
  set XML_TYPE(v)                           { YadamuDataTypes.redefineProperty(this,'XML_TYPE',v) }
                                                   
  // Spatial                                       
                                                   
  get SPATIAL_TYPE()                        { return this.JSON_TYPE }
                                                   
  set SPATIAL_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'SPATIAL_TYPE',v) }
                                                   
  get GEOMETRY_TYPE()                       { return this.SPATIAL_TYPE }
                                                   
  set GEOMETRY_TYPE(v)                      { YadamuDataTypes.redefineProperty(this,'GEOMETRY_TYPE',v) }
                                                   
  get GEOGRAPHY_TYPE()                      { return this.SPATIAL_TYPE }
                                                   
  set GEOGRAPHY_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'GEOGRAPHY_TYPE',v) }
                                                   
  get POINT_TYPE()                          { return this.SPATIAL_TYPE }
                                                   
  set POINT_TYPE(v)                         { YadamuDataTypes.redefineProperty(this,'POINT_TYPE',v) }
                                                   
  get LINE_TYPE()                           { return this.SPATIAL_TYPE }
                                                    
  set LINE_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'LINE_TYPE',v) }
                                                   
  get POLYGON_TYPE()                        { return this.SPATIAL_TYPE }
                                                   
  set POLYGON_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'POLYGON_TYPE',v) }
                                                   
  get MULTI_POINT_TYPE()                    { return this.SPATIAL_TYPE }
                                                   
  set MULTI_POINT_TYPE(v)                   { YadamuDataTypes.redefineProperty(this,'MULTI_POINT_TYPE',v) }
                                                   
  get MULTI_LINE_TYPE()                     { return this.SPATIAL_TYPE }
                                                   
  set MULTI_LINE_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'MULTI_LINE_TYPE',v) }
                                                   
  get MULTI_POLYGON_TYPE()                  { return this.SPATIAL_TYPE }
                                                   
  set MULTI_POLYGON_TYPE(v)                 { YadamuDataTypes.redefineProperty(this,'MULTI_POLYGON_TYPE',v) }
                                                   
  get GEOMETRY_COLLECTION_TYPE()            { return this.SPATIAL_TYPE }
                                                   
  set GEOMETRY_COLLECTION_TYPE(v)           { YadamuDataTypes.redefineProperty(this,'GEOMETRY_COLLECTION_TYPE',v) }
                                                   
  get BOX_TYPE()                            { return this.SPATIAL_TYPE }
                                                   
  set BOX_TYPE(v)                           { YadamuDataTypes.redefineProperty(this,'BOX_TYPE',v) }
                                                   
  get PATH_TYPE()                           { return this.SPATIAL_TYPE }
                                                   
  set PATH_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'PATH_TYPE',v) }
  
  // Other Common Types                            
                                                   
  get UUID_TYPE()                           { return `${this.VARCHAR_TYPE}(36)` }
                                                   
  set UUID_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'UUID_TYPE',v) }
                                                   
  // User Defined Types                            
                                                   
  get USER_DEFINED_TYPE()                   { return this.CLOB_TYPE }
                                                   
  set USER_DEFINED_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'USER_DEFINED_TYPE',v) }

  // Oracle Specific Types                         
                                                   
  get ORACLE_ROWID_TYPE()                   { return `${this.VARCHAR_TYPE}(32)` }
                                                   
  set ORACLE_ROWID_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'ORACLE_ROWID_TYPE',v) }
                                                   
  get ORACLE_BFILE_TYPE()                   { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  set ORACLE_BFILE_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'ORACLE_BFILE_TYPE',v) }
                                                   
  get ORACLE_ANYDATA_TYPE()                 { return this.CLOB_TYPE  }
                                                   
  set ORACLE_ANYDATA_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'ORACLE_ANYDATA_TYPE',v) }
                                                   
  get ORACLE_OBJECT_TYPE()                  { return this.CLOB_TYPE }
                                                   
  set ORACLE_OBJECT_TYPE(v)                 { YadamuDataTypes.redefineProperty(this,'ORACLE_OBJECT_TYPE',v) }
                                                   
  get ORACLE_UNBOUNDED_NUMBER_TYPE()        { return this.MAX_NUMERIC_TYPE }
                                                   
  set ORACLE_UNBOUNDED_NUMBER_TYPE(v)       { YadamuDataTypes.redefineProperty(this,'ORACLE_UNBOUNDED_NUMBER_TYPE',v) }
                                                   
  // SQL Server Specific Types                     
                                                   
  get MSSQL_MONEY_TYPE()                    { return `${this.NUMERIC_TYPE}(19,4)` }
                                                   
  set MSSQL_MONEY_TYPE(v)                   { YadamuDataTypes.redefineProperty(this,'MSSQL_MONEY_TYPE',v) }

  get MSSQL_SMALLMONEY_TYPE()               { return `${this.NUMERIC_TYPE}(10,4)` }
                                                    
  set MSSQL_SMALLMONEY_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'MSSQL_SMALLMONEY_TYPE',v) }

  get MSSQL_SMALLDATETIME_TYPE()            { return this.DATETIME_TYPE }
                                                   
  set MSSQL_SMALLDATETIME_TYPE(v)           { YadamuDataTypes.redefineProperty(this,'MSSQL_SMALLDATETIME_TYPE',v) }

  get MSSQL_DATETIME2_TYPE()                { return this.DATETIME_TYPE }
                                                   
  set MSSQL_DATETIME2_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'MSSQL_DATETIME2_TYPE',v) }

  get MSSQL_ROWVERSION_TYPE()               { return `${this.BINARY_TYPE}(8)` }
                                                   
  set MSSQL_ROWVERSION_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'MSSQL_ROWVERSION_TYPE',v) }

  get MSSQL_HIERARCHY_ID_TYPE()             { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  set MSSQL_HIERARCHY_ID_TYPE(v)            { YadamuDataTypes.redefineProperty(this,'MSSQL_HIERARCHY_ID_TYPE',v) }

  // Postgres Specific Types                       
                                                   
  get PGSQL_SINGLE_CHAR_TYPE()              { return `${this.CHAR_TYPE}(1)` }
                                                   
  set PGSQL_SINGLE_CHAR_TYPE(v)             { YadamuDataTypes.redefineProperty(this,'PGSQL_SINGLE_CHAR_TYPE',v) }
  
  get PGSQL_BPCHAR_TYPE()                   { return this.CHAR_TYPE }
                                                   
  set PGSQL_BPCHAR_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'PGSQL_BPCHAR_TYPE',v) }
  
  get PGSQL_NAME_TYPE()                     { return `${this.VARCHAR_TYPE}(64)` }
                                                   
  set PGSQL_NAME_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'PGSQL_NAME_TYPE',v) }
  
  get PGSQL_MONEY_TYPE()                    { return `${this.NUMERIC_TYPE}(21,2)` }
  
  set PGSQL_MONEY_TYPE(v)                   { YadamuDataTypes.redefineProperty(this,'PGSQL_MONEY_TYPE',v) }
  
  get PGSQL_BINARY_JSON_TYPE()              { return this.JSON_TYPE }                                 
  
  set PGSQL_BINARY_JSON_TYPE(v)             { YadamuDataTypes.redefineProperty(this,'PGSQL_BINARY_JSON_TYPE',v) }
  
  get PGSQL_LINE_EQ_TYPE()                  { return this.JSON_TYPE }
                                                   
  set PGSQL_LINE_EQ_TYPE(v)                 { YadamuDataTypes.redefineProperty(this,'PGSQL_LINE_EQ_TYPE',v) }
  
  get PGSQL_CIRCLE_TYPE()                   { return this.JSON_TYPE }
                                                   
  set PGSQL_CIRCLE_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'PGSQL_CIRCLE_TYPE',v) }
  
  get PGSQL_CIDR_ADDR_TYPE()                { return `${this.VARCHAR_TYPE}(39)` }
  
  set PGSQL_CIDR_ADDR_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'PGSQL_CIDR_ADDR_TYPE',v) }
  
  get PGSQL_INET_ADDR_TYPE()                { return `${this.VARCHAR_TYPE}(39)` }
                                                   
  set PGSQL_INET_ADDR_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'PGSQL_INET_ADDR_TYPE',v) }
  
  get PGSQL_MAC_ADDR_TYPE()                 { return `${this.VARCHAR_TYPE}(23)` }
  
  set PGSQL_MAC_ADDR_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'PGSQL_MAC_ADDR_TYPE',v) }
  
  get PGSQL_MAC_ADDR8_TYPE()                { return `${this.VARCHAR_TYPE}(23)` }
  
  set PGSQL_MAC_ADDR8_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'PGSQL_MAC_ADDR8_TYPE',v) }
  
  get PGSQL_RANGE_INT4_TYPE()               { return this.JSON_TYPE } // "int4range"                                        
  
  set PGSQL_RANGE_INT4_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'PGSQL_RANGE_INT4_TYPE',v) }
  
  get PGSQL_RANGE_INT8_TYPE()               { return this.JSON_TYPE } //  "int8range"                                        
  
  set PGSQL_RANGE_INT8_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'PGSQL_RANGE_INT8_TYPE',v) }
  
  get PGSQL_RANGE_NUM_TYPE()                { return this.JSON_TYPE } //  "numrange"                                         
  
  set PGSQL_RANGE_NUM_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'PGSQL_RANGE_NUM_TYPE',v) }
  
  get PGSQL_RANGE_TIMESTAMP_TYPE()          { return this.JSON_TYPE } //  "tsrange"                                          
  
  set PGSQL_RANGE_TIMESTAMP_TYPE(v)         { YadamuDataTypes.redefineProperty(this,'PGSQL_RANGE_TIMESTAMP_TYPE',v) }
  
  get PGSQL_RANGE_TIMESTAMP_TZ_TYPE()       { return this.JSON_TYPE } //  "tstzrange"                                        
  
  set PGSQL_RANGE_TIMESTAMP_TZ_TYPE(v)      { YadamuDataTypes.redefineProperty(this,'PGSQL_RANGE_TIMESTAMP_TZ_TYPE',v) }
  
  get PGSQL_RANGE_DATE_TYPE()               { return this.JSON_TYPE } //  "daterange"                                        
  
  set PGSQL_RANGE_DATE_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'PGSQL_RANGE_DATE_TYPE',v) }
  
  get PGSQL_TIMESTAMP_VECTOR()              { return this.JSON_TYPE } //  "tsvector"                                         
  
  set PGSQL_TIMESTAMP_VECTOR(v)             { YadamuDataTypes.redefineProperty(this,'PGSQL_TIMESTAMP_VECTOR',v) }
  
  get PGSQL_TEXTSEACH_VECTOR_TYPE()         { return this.JSON_TYPE } //  "gtsvector"                                        
  
  set PGSQL_TEXTSEACH_VECTOR_TYPE(v)        { YadamuDataTypes.redefineProperty(this,'PGSQL_TEXTSEACH_VECTOR_TYPE',v) }
  
  get PGSQL_TEXTSEACH_QUERY_TYPE()          { return this.MAX_VARCHAR_TYPE} // "tsquery"                                                       
  
  set PGSQL_TEXTSEACH_QUERY_TYPE(v)         { YadamuDataTypes.redefineProperty(this,'PGSQL_TEXTSEACH_QUERY_TYPE',v) }
  
  get PGSQL_IDENTIFIER_TYPE()               { return `${this.NUMERIC_TYPE}(10,0)` } // Unsigned 4 Byte (32 bit Integer). Alternates include BIGINT, Unsigned Int. Byte[4]
                                                   
  set PGSQL_IDENTIFIER_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'PGSQL_IDENTIFIER_TYPE',v) }
  
  get PGSQL_OID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // "oid"                                              
  
  set PGSQL_OID_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'PGSQL_OID_TYPE',v) }
  
  get PGSQL_REG_CLASS_TYPE()                { return this.PGSQL_IDENTIFIER_TYPE } // "regclass"                                         
  
  set PGSQL_REG_CLASS_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_CLASS_TYPE',v) }
  
  get PGSQL_REG_COLLATION_TYPE()            { return this.PGSQL_IDENTIFIER_TYPE } // "regcollation"                                     
  
  set PGSQL_REG_COLLATION_TYPE(v)           { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_COLLATION_TYPE',v) }
  
  get PGSQL_REG_TEXTSEARCH_CONFIG_TYPE()    { return this.PGSQL_IDENTIFIER_TYPE } // "regconfig"                                        
  
  set PGSQL_REG_TEXTSEARCH_CONFIG_TYPE(v)   { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_TEXTSEARCH_CONFIG_TYPE',v) }
  
  get PGSQL_REG_TEXTSEARCH_DICT_TYPE()      { return this.PGSQL_IDENTIFIER_TYPE } // "regdictionary"                                    
  
  set PGSQL_REG_TEXTSEARCH_DICT_TYPE(v)     { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_TEXTSEARCH_DICT_TYPE',v) }
  
  get PGSQL_REG_NAMESPACE_TYPE()            { return this.PGSQL_IDENTIFIER_TYPE } // "regnamespace"                                     
  
  set PGSQL_REG_NAMESPACE_TYPE(v)           { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_NAMESPACE_TYPE',v) }
  
  get PGSQL_REG_OPERATOR_NAME_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regoper"                                          
  
  set PGSQL_REG_OPERATOR_NAME_TYPE(v)       { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_OPERATOR_NAME_TYPE',v) }
  
  get PGSQL_REG_OPERATOR_ARGS_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regoperator"                                      
  
  set PGSQL_REG_OPERATOR_ARGS_TYPE(v)       { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_OPERATOR_ARGS_TYPE',v) }
  
  get PGSQL_REG_FUNCTION_NAME_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regproc"                                          
  
  set PGSQL_REG_FUNCTION_NAME_TYPE(v)       { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_FUNCTION_NAME_TYPE',v) }
  
  get PGSQL_REG_FUNCTION_ARGS_TYPE()        { return this.PGSQL_IDENTIFIER_TYPE } // "regprocedure"                                     
  
  set PGSQL_REG_FUNCTION_ARGS_TYPE(v)       { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_FUNCTION_ARGS_TYPE',v) }
  
  get PGSQL_REG_ROLE_TYPE()                 { return this.PGSQL_IDENTIFIER_TYPE } // "regrole"                                          
  
  set PGSQL_REG_ROLE_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_ROLE_TYPE',v) }
  
  get PGSQL_REG_TYPE_TYPE()                 { return this.PGSQL_IDENTIFIER_TYPE } // "regtype"                                          0
  
  set PGSQL_REG_TYPE_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'PGSQL_REG_TYPE_TYPE',v) }
  
  get PGSQL_TID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // tid"                                              
  
  set PGSQL_TID_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'PGSQL_TID_TYPE',v) }
  
  get PGSQL_XID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // "xid"                                              
  
  set PGSQL_XID_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'PGSQL_XID_TYPE',v) }
  
  get PGSQL_CID_TYPE()                      { return this.PGSQL_IDENTIFIER_TYPE } // "cid"                                              
  
  set PGSQL_CID_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'PGSQL_CID_TYPE',v) }
  
  get PGSQL_TXID_SNAPSHOT_TYPE()            { return this.PGSQL_IDENTIFIER_TYPE } // "txid_snapshot"                                    
  
  set PGSQL_TXID_SNAPSHOT_TYPE(v)           { YadamuDataTypes.redefineProperty(this,'PGSQL_TXID_SNAPSHOT_TYPE',v) }
  
  get PGSQL_GTSVECTOR_TYPE()                { return this.JSON_TYPE } // "gtsvector"                                    
  
  set PGSQL_GTSVECTOR_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'PGSQL_GTSVECTOR_TYPE',v) }
  
  get PGSQL_ACLITEM()                       { return this.JSON_TYPE } // "aclitem"                                          
  
  set PGSQL_ACLITEM(v)                      { YadamuDataTypes.redefineProperty(this,'PGSQL_ACLITEM',v) }
  
  get PGSQL_REFCURSOR()                     { return this.JSON_TYPE } // "refcursor"       
  
  set PGSQL_REFCURSOR(v)                    { YadamuDataTypes.redefineProperty(this,'PGSQL_REFCURSOR',v) }
  
  // MySQL Specific Types
  
  get MYSQL_SET_TYPE()                      { return this.JSON_TYPE }
                                                   
  set MYSQL_SET_TYPE(v)                     { YadamuDataTypes.redefineProperty(this,'MYSQL_SET_TYPE',v) }

  get MYSQL_ENUM_TYPE()                     { return `${this.VARCHAR_TYPE}(512)` }

  set MYSQL_ENUM_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'MYSQL_ENUM_TYPE',v) }
                                                   
  get MYSQL_LONGTEXT_TYPE()                 { return this.CLOB_TYPE }
                                                   
  set MYSQL_LONGTEXT_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'MYSQL_LONGTEXT_TYPE',v) }

  get MYSQL_MEDIUMTEXT_TYPE()               { return this.CLOB_TYPE }
                                                   
  set MYSQL_MEDIUMTEXT_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'MYSQL_MEDIUMTEXT_TYPE',v) }

  get MYSQL_TEXT_TYPE()                     { return this.CLOB_TYPE }
                                                   
  set MYSQL_TEXT_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'MYSQL_TEXT_TYPE',v) }

  get MYSQL_TINYTEXT_TYPE()                 { return `${this.VARCHAR_TYPE}(255)` }
                                                   
  set MYSQL_TINYTEXT_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'MYSQL_TINYTEXT_TYPE',v) }

  get MYSQL_LONGBLOB_TYPE()                 { return this.BLOB_TYPE }
                                                   
  set MYSQL_LONGBLOB_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'MYSQL_LONGBLOB_TYPE',v) }

  get MYSQL_MEDIUMBLOB_TYPE()               { return this.BLOB_TYPE }
                                                   
  set MYSQL_MEDIUMBLOB_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'MYSQL_MEDIUMBLOB_TYPE',v) }

  get MYSQL_BLOB_TYPE()                     { return this.BLOB_TYPE }
                                                   
  set MYSQL_BLOB_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'MYSQL_BLOB_TYPE',v) }

  get MYSQL_TINYBLOB_TYPE()                 { return `${this.VARBINARY_TYPE}(255)` }
                                                   
  set MYSQL_TINYBLOB_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'MYSQL_TINYBLOB_TYPE',v) }

  // Mongo Specific Types                          
                                                   
  get MONGO_OBJECTID_TYPE()                 { return `${this.BINARY_TYPE}(12)` }
                                                   
  set MONGO_OBJECTID_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'MONGO_OBJECTID_TYPE',v) }

  get MONGO_DECIMAL128_TYPE()               { return this.MAX_NUMERIC_TYPE }
                                                   
  set MONGO_DECIMAL128_TYPE(v)              { YadamuDataTypes.redefineProperty(this,'MONGO_DECIMAL128_TYPE',v) }
                                                   
  get MONGO_ARRAY_TYPE()                    { return this.JSON_TYPE }
                                                   
  set MONGO_ARRAY_TYPE(v)                   { YadamuDataTypes.redefineProperty(this,'MONGO_ARRAY_TYPE',v) }

  get MONGO_OBJECT_TYPE()                   { return this.JSON_TYPE }
                                                   
  set MONGO_OBJECT_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'MONGO_OBJECT_TYPE',v) }

  get MONGO_NULL_TYPE()                     { return `${this.VARCHAR_TYPE}(4)` }
                                                   
  set MONGO_NULL_TYPE(v)                    { YadamuDataTypes.redefineProperty(this,'MONGO_NULL_TYPE',v) }

  get MONGO_REGEX_TYPE()                    { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  set MONGO_REGEX_TYPE(v)                   { YadamuDataTypes.redefineProperty(this,'MONGO_REGEX_TYPE',v) }

  get MONGO_JS_TYPE()                       { return this.CLOB_TYPE }
                                                   
  set MONGO_JS_TYPE(v)                      { YadamuDataTypes.redefineProperty(this,'MONGO_JS_TYPE',v) }
   
  get MONGO_SCOPED_JS_TYPE()                { return this.CLOB_TYPE }
                                                   
  set MONGO_SCOPED_JS_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'MONGO_SCOPED_JS_TYPE',v) }

  get MONGO_MINKEY_TYPE()                   { return this.JSON_TYPE }
                                                   
  set MONGO_MINKEY_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'MONGO_MINKEY_TYPE',v) }

  get MONGO_MAXKEY_TYPE()                   { return this.JSON_TYPE }
                                                   
  set MONGO_MAXKEY_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'MONGO_MAXKEY_TYPE',v) }

  get MONGO_UNDEFINED_TYPE()                { return `${this.VARCHAR_TYPE}(8)` }
                                                   
  set MONGO_UNDEFINED_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'MONGO_UNDEFINED_TYPE',v) }

  get MONGO_REGEX_TYPE()                    { return `${this.VARCHAR_TYPE}(2048)` }
                                                   
  set MONGO_REGEX_TYPE(v)                   { YadamuDataTypes.redefineProperty(this,'MONGO_REGEX_TYPE',v) }

  get MONGO_DBPOINTER_TYPE()                { return `${this.VARCHAR_TYPE}(36)` }
                                                   
  set MONGO_DBPOINTER_TYPE(v)               { YadamuDataTypes.redefineProperty(this,'MONGO_DBPOINTER_TYPE',v) }

  get MONGO_FUNCTION_TYPE()                 { return this.CLOB_TYPE }
                                                   
  set MONGO_FUNCTION_TYPE(v)                { YadamuDataTypes.redefineProperty(this,'MONGO_FUNCTION_TYPE',v) }

  get MONGO_SYMBOL_TYPE()                   { return `${this.VARCHAR_TYPE}(36)` }
  
  set MONGO_SYMBOL_TYPE(v)                  { YadamuDataTypes.redefineProperty(this,'MONGO_SYMBOL_TYPE',v) }

  // Default Maxiumum Lengths
  
  get SCALAR_LENGTH()                       {
    throw new YadamuError(`Must supply explicit data type mapping for 'SCALAR_LENGTH'`)
  }
  
  set SCALAR_LENGTH(v)                      { YadamuDataTypes.redefineProperty(this,'SCALAR_LENGTH',v) }
                                                   
  get CHAR_LENGTH()                         { return this.SCALAR_LENGTH }      
                                                   
  set CHAR_LENGTH(v)                        { YadamuDataTypes.redefineProperty(this,'CHAR_LENGTH',v) }
                                                   
  get VARCHAR_LENGTH()                      { return this.CHAR_LENGTH }    
                                                   
  set VARCHAR_LENGTH(v)                     { YadamuDataTypes.redefineProperty(this,'VARCHAR_LENGTH',v) }
                                                   
  get NCHAR_LENGTH()                        { return this.CHAR_LENGTH }    
                                                   
  set NCHAR_LENGTH(v)                       { YadamuDataTypes.redefineProperty(this,'NCHAR_LENGTH',v) }
                                                   
  get NVACHAR_LENGTH()                      { return this.CHAR_LENGTH }    
                                                   
  set NVACHAR_LENGTH(v)                     { YadamuDataTypes.redefineProperty(this,'NVACHAR_LENGTH',v) }
                                                   
  get BINARY_LENGTH()                       { return this.CHAR_LENGTH }    
                                                   
  set BINARY_LENGTH(v)                      { YadamuDataTypes.redefineProperty(this,'BINARY_LENGTH',v) }
                                                   
  get VARBINARY_LENGTH()                    { this.BINARY_LENGTH }     
                                                   
  set VARBINARY_LENGTH(v)                   { YadamuDataTypes.redefineProperty(this,'VARBINARY_LENGTH',v) }
                                                   
  get NUMERIC_PRECISION()                   { return 38 }
                                                   
  set NUMERIC_PRECISION(v)                  { YadamuDataTypes.redefineProperty(this,'NUMERIC_PRECISION',v) }
                                                   
  get NUMERIC_SCALE()                       { return 38 }
                                                   
  set NUMERIC_SCALE(v)                      { YadamuDataTypes.redefineProperty(this,'NUMERIC_SCALE',v) }
                                                   
  get TIMESTAMP_PRECISION()                 { return 6 }

  set TIMESTAMP_PRECISION(v)                { YadamuDataTypes.redefineProperty(this,'TIMESTAMP_PRECISION',v) }
                                                   
  get LOB_LENGTH()                          { return -1 }      
                                                   
  set LOB_LENGTH(v)                         { YadamuDataTypes.redefineProperty(this,'LOB_LENGTH',v) }
                                                   
  get CLOB_LENGTH()                         { return this.LOB_LENGTH }     
                                                   
  set CLOB_LENGTH(v)                        { YadamuDataTypes.redefineProperty(this,'CLOB_LENGTH',v) }
                                                   
  get BLOB_LENGTH()                         { return this.LOB_LENGTH }     
                                                   
  set BLOB_LENGTH(v)                        { YadamuDataTypes.redefineProperty(this,'BLOB_LENGTH',v) }

  get BOOLEAN_TYPES() {
    return this._BOOLEAN_TYPES || (() => {
	  this._BOOLEAN_TYPES = Object.freeze([this.BOOLEAN_TYPE])
	  return this._BOOLEAN_TYPES
	})()
  }

  get INTEGER_TYPES() { 
    return this._INTEGER_TYPES || (() => {
	  this._INTEGER_TYPES = Object.freeze([this.INTEGER_TYPE,this.TINYINT_TYPE,this.SMALLINT_TYPE,this.MEDIUMINT_TYPE,this.BIGINT_TYPE])
	  return this._INTEGER_TYPES
	})()
  }

  get FLOAT_TYPES() { 
    return this._INTEGER_TYPES || (() => {
	  this._INTEGER_TYPES = Object.freeze([this.FLOAT_TYPE,this.REAL_TYPE,this.DOUBLE_TYPE])
	  return this._INTEGER_TYPES
	})()
  }

  get SPATIAL_TYPES() { 
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

export { YadamuDataTypes as default, YadamuDataTypes, YadamuStorageOptions }