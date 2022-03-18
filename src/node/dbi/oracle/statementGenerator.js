"use strict";

import path from 'path';
import crypto from 'crypto';
import { performance } from 'perf_hooks';

import oracledb from 'oracledb';
oracledb.fetchAsString = [ oracledb.DATE, oracledb.NUMBER ]

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
     
class StatementGenerator {

  static get LOB_TYPES()   { 
    StatementGenerator._LOB_TYPES = StatementGenerator._LOB_TYPES || Object.freeze([oracledb.CLOB,oracledb.BLOB])
    return StatementGenerator._LOB_TYPES
  }

  get BIND_LENGTH() {     
    StatementGenerator._BIND_LENGTH = StatementGenerator._BIND_LENGTH || Object.freeze({
      BLOB          : this.dbi.LOB_MAX_SIZE
    , CLOB          : this.dbi.LOB_MAX_SIZE
    , JSON          : this.dbi.LOB_MAX_SIZE
    , NCLOB         : this.dbi.LOB_MAX_SIZE
    , OBJECT        : this.dbi.LOB_MAX_SIZE
    , XMLTYPE       : this.dbi.LOB_MAX_SIZE
    , ANYDATA       : this.dbi.LOB_MAX_SIZE
    , GEOMETRY      : this.dbi.LOB_MAX_SIZE
	, NUMBER        : 41 // 38 + Sign and Point
	, BOOLEAN       : 1  // Mappped to RAW(1)
    , BFILE         : 4096
    , DATE          : 24
    , TIMESTAMP     : 35
    , INTERVAL      : 12
    })
    return StatementGenerator._BIND_LENGTH
  }
  
  static get BOUNDED_TYPES() { 
    StatementGenerator._BOUNDED_TYPES = StatementGenerator._BOUNDED_TYPES || Object.freeze( ['CHAR','NCHAR','VARCHAR2','NVARCHAR2','RAW'])
    return StatementGenerator._BOUNDED_TYPES;
  }


  // This is the spatial format of the incoming data, not the format used by this driver
  get SPATIAL_FORMAT()               { return this.dbi.INBOUND_SPATIAL_FORMAT } 
  get OBJECTS_AS_JSON()              { return this.dbi.systemInformation.objectFormat === 'JSON'}

  get GEOJSON_FUNCTION()             { return 'DESERIALIZE_GEOJSON' }
  get RANDOM_OBJECT_LENGTH()         { return 16 }
  get ORACLE_CSV_SPECIFICATION()     { return 'CSV WITH EMBEDDED' }
  get ORACLE_NEWLINE_SPECIFICATION() { return 'RECORDS DELIMITED BY NEWLINE'}
  
  get SQL_DIRECTORY_NAME()           { return this._SQL_DIRECTORY_NAME }
  set SQL_DIRECTORY_NAME(v)          { this._SQL_DIRECTORY_NAME = v }
  get SQL_DIRECTORY_PATH()           { return this._SQL_DIRECTORY_PATH }
  set SQL_DIRECTORY_PATH(v)          { this._SQL_DIRECTORY_PATH = v }
  get LOADER_CLOB_SIZE()             { return 67108864 }
  get LOADER_CLOB_TYPE()             { return `CHAR(${this.LOADER_CLOB_SIZE})`}
    
  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger
  }
   
  generateBinds(dataTypes, tableInfo, metadata) {
      
     // Binds describe the format that will be used to supply the data. Eg with SQLServer BIGINT values will be presented as String
	 tableInfo.lobColumns = false;
     return dataTypes.map((dataType,idx) => {
       if (!dataType.length) {
          dataType.length = parseInt(metadata.sizeConstraints[idx]);
       }
       switch (dataType.type) {
         case 'NUMBER':
           return { type: oracledb.DB_TYPE_NUMBER }
		   // return { type: oracledb.STRING, maxSize : this.BIND_LENGTH.NUMBER}
         case 'FLOAT':
         case 'BINARY_FLOAT':
           return { type: oracledb.DB_TYPE_BINARY_FLOAT }
         case 'BINARY_DOUBLE':
           /*
           // Peek numeric binds to check for strings when writing
           if ((metadata.source.vendor === 'SNOWFLAKE') && ['NUMBER','DECIMAL','NUMERIC','FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE','DOUBLE PRECISION', 'REAL'].includes(metadata.source.dataTypes[idx])) {
             return { type: oracledb.STRING, maxSize : dataType.length + 3}
           }
           */
           return { type: oracledb.DB_TYPE_BINARY_DOUBLE }
         case 'RAW':
           return { type: oracledb.DB_TYPE_RAW, maxSize : dataType.length}
         case 'CHAR':
           return { type: oracledb.DB_TYPE_CHAR, maxSize : dataType.length * 2}
         case 'VARCHAR':
         case 'VARCHAR2':
           return { type: oracledb.DB_TYPE_VARCHAR, maxSize : dataType.length * 2}
         case 'NCHAR':
           return { type: oracledb.DB_TYPE_NCHAR, maxSize : dataType.length * 2}
         case 'NVARCHAR2':
           return { type: oracledb.DB_TYPE_VARCHAR, maxSize : dataType.length * 2}
         case 'DATE':
         case 'TIMESTAMP':
           return { type: oracledb.STRING, maxSize : this.BIND_LENGTH.TIMESTAMP}
         case 'INTERVAL':
            return { type: oracledb.STRING, maxSize : this.BIND_LENGTH.INTERVAL}
         case 'CLOB':
           tableInfo.lobColumns = true;
           return {type : oracledb.DB_TYPE_CLOB, maxSize : this.BIND_LENGTH.CLOB }
         case 'NCLOB':
           tableInfo.lobColumns = true;
           // return {type : oracledb.DB_TYPE_CLOB, maxSize : this.BIND_LENGTH.NCLOB }
           return {type : oracledb.DB_TYPE_NCLOB, maxSize : this.BIND_LENGTH.NCLOB }
         case 'ANYDATA':
           tableInfo.lobColumns = true;
           return {type : oracledb.DB_TYPE_CLOB, maxSize : this.BIND_LENGTH.ANYDATA }
         case 'XMLTYPE':
           // Cannot Bind XMLTYPE > 32K as String: ORA-01461: can bind a LONG value only for insert into a LONG column when constructing XMLTYPE
           tableInfo.lobColumns = true;
           // return {type : oracledb.CLOB}
           return {type : oracledb.DB_TYPE_CLOB, maxSize : this.BIND_LENGTH.CLOB}
         case 'JSON':
           // Defalt JSON Storeage model: JSON store as CLOB
           // JSON store as BLOB can lead to Error: ORA-40479: internal JSON serializer error during export operations.
           // return {type : oracledb.CLOB}
		   switch (this.dbi.JSON_DATA_TYPE) {
			  case 'JSON':
			  case 'BLOB':
                tableInfo.lobColumns = true;
                return {type : oracledb.DB_TYPE_BLOB, maxSize : this.BIND_LENGTH.JSON}
			  case 'CLOB':
                tableInfo.lobColumns = true;
                return {type : oracledb.DB_TYPE_CLOB, maxSize : this.BIND_LENGTH.JSON}
			  default:
			    return {type : oracledb.DB_TYPE_VARCHAR, maxSize : 32767 }
		   }
         case 'BLOB':
           // return {type : oracledb.BUFFER}
           // return {type : oracledb.BUFFER, maxSize : BIND_LENGTH.BLOB }
           tableInfo.lobColumns = true;
           return {type : oracledb.DB_TYPE_BLOB, maxSize : this.BIND_LENGTH.BLOB}
         case 'BFILE':
           return { type :oracledb.DB_TYPE_VARCHAR, maxSize : this.BIND_LENGTH.BFILE }
         case 'BOOLEAN':
            return { type: oracledb.BUFFER, maxSize :  this.BIND_LENGTH.BOOLEAN }         
         case 'GEOMETRY':
         case 'GEOGRAPHY':
         case "\"MDSYS\".\"SDO_GEOMETRY\"":
           tableInfo.lobColumns = true;
           // return {type : oracledb.CLOB}
           switch (this.SPATIAL_FORMAT) { 
             case "WKB":
             case "EWKB":
               return {type : oracledb.DB_TYPE_BLOB, maxSize : this.BIND_LENGTH.GEOMETRY}
               break;
             case "WKT":
             case "EWKT":
             case "GeoJSON":
               return {type : oracledb.DB_TYPE_CLOB, maxSize : this.BIND_LENGTH.JSON}
               break;
             default:
           }
           break;   
         default:
           if (dataType.type.indexOf('.') > -1) {
             // return {type : oracledb.CLOB}
             tableInfo.lobColumns = true
			 return {type : oracledb.DB_TYPE_CLOB, maxSize : this.BIND_LENGTH.OBJECT}
           }
           return {type : oracledb.DB_TYPE_VARCHAR, maxSize :  dataType.length}
       }
     })
  
  }
  
  async getMetadataLob() {

    return await this.dbi.jsonToBlob({metadata: this.metadata});  
      
  }
 
  getPLSQL(dml) {    
	const withOffset = dml.indexOf('\nWITH\n')
    return withOffset > -1 ? dml.substring(withOffset+5,dml.indexOf('\nselect')) : null
  }
 
  generatePLSQL(targetSchema,tableName,dml,columns,declarations,assignments,variables) {

   const plsqlFunctions = this.getPLSQL(dml);
   const dmlBlock = `declare\n  ${declarations.join(';\n  ')};\n\n${plsqlFunctions}\nbegin\n  ${assignments.join(';\n  ')};\n  insert into "${targetSchema}"."${tableName}" (${columns}) values (${variables.join(',')});\nend;`;      
   return dmlBlock;
     
  }

  orderColumnsByBindType(tableInfo) {
	 
    /*
    **
    ** Avoid Error: ORA-24816: Expanded non LONG bind data supplied after actual LONG or LOB column 
    ** 
    ** When optimzing performance by binding Strings and Buffers to LOBS it is necessary to order the column list so that CLOB and BLOB columns are at the end of the list.
    **
    ** Set up an array that maps columns by binding data type. Scalar Columns, Spatial Columns
    **
    */

    const bindOrdering = [];
	
    for (const colIdx in tableInfo.lobBinds) {
	  switch (tableInfo.targetDataTypes[colIdx]) {
		// GEOMETRY is inserted by Stored Procedure.. Do not move to end of List.
        case "GEOMETRY":
        case "\"MDSYS\".\"SDO_GEOMETRY\"":
		  bindOrdering.push(parseInt(colIdx))
		  break
        case "XMLTYPE":
		  bindOrdering.push(parseInt(colIdx))
		  break
		default:
          if (!StatementGenerator.LOB_TYPES.includes(tableInfo.lobBinds[colIdx].type)) {
            bindOrdering.push(parseInt(colIdx))
		  }
	  }
    }
	
    for (const colIdx in tableInfo.lobBinds) {
      if (!bindOrdering.includes(parseInt(colIdx))) {
        bindOrdering.push(parseInt(colIdx))
	  }
    }
   
    // Column Mappings contains the indices of the non LOB binds, following by the indices of the the LOB binds	
	
    // Reorder binds, dataTypes based on mapping
	
	const columnNames = []
	const targetDataTypes = []
	const sizeConstraints = []
	const dataTypes = []
	const binds = []
	const lobBinds = []
	
	for (const idx in bindOrdering) {
	  columnNames.push(tableInfo.columnNames[bindOrdering[idx]])
	  targetDataTypes.push(tableInfo.targetDataTypes[bindOrdering[idx]])
	  binds.push(tableInfo.binds[bindOrdering[idx]])
	  lobBinds.push(tableInfo.lobBinds[bindOrdering[idx]])
	}
	
    tableInfo.columnNames = columnNames;
	tableInfo.targetDataTypes = targetDataTypes;
	tableInfo.sizeConstraints = sizeConstraints;
    tableInfo.binds = binds;
    tableInfo.lobBinds = lobBinds
    tableInfo.bindOrdering = bindOrdering
	
    return tableInfo
  }

  getTypeMappings() {

    const typeMappings = {
	  spatialFormat    : this.SPATIAL_FORMAT
	, raw1AsBoolean    : new Boolean(this.dbi.TREAT_RAW1_AS_BOOLEAN).toString().toUpperCase()
	, jsonDataType     : this.dbi.JSON_DATA_TYPE
	, xmlStorageModel  : this.dbi.XML_STORAGE_CLAUSE
	, circleFormat     : this.dbi.INBOUND_CIRCLE_FORMAT
	}
	
	return JSON.stringify(typeMappings); 
  }
  
  generateExternalTableDefinition(tableMetadata,externalTableName,externalColumnDefinitions,copyColumnDefinitions) {
	 return `
CREATE TABLE ${externalTableName} (
  ${externalColumnDefinitions.join(',')}
) 
ORGANIZATION EXTERNAL ( 
  default directory ${this.SQL_DIRECTORY_NAME} 
  ACCESS PARAMETERS (
    ${this.ORACLE_NEWLINE_SPECIFICATION}
	READSIZE 67108864 
	CHARACTERSET AL32UTF8 
	${this.dbi.COPY_BADFILE_DIRNAME ? `BADFILE ${this.dbi.COPY_BADFILE_DIRNAME}:` : 'NOBADFILE'}
	${this.dbi.COPY_LOGFILE_DIRNAME ? `LOGFILE ${this.dbi.COPY_LOGFILE_DIRNAME}:` : 'NOLOGFILE'}
	FIELDS ${this.ORACLE_CSV_SPECIFICATION}
		   MISSING FIELD VALUES ARE NULL 
		   (
		     ${copyColumnDefinitions.join(",")}
		   )
  ) 
  LOCATION (
	'${tableMetadata.partitionCount ? `${tableMetadata.dataFile.map((filename) => { return path.basename(filename).split(path.sep).join(path.posix.sep)}).join("','")}` : path.basename(tableMetadata.dataFile).split(path.sep).join(path.posix.sep)}'
  )
) 
${tableMetadata.partitionCount ? `PARALLEL ${(tableMetadata.partitionCount > this.dbi.PARALLEL) ? this.dbi.PARALLEL : tableMetadata.partitionCount}` : ''}
` 
  }
  
  generateCopyStatement(targetSchema,tableName,externalTableName,externalColumnNames,externalSelectList,plsql) {
	return `insert ${plsql ? `/*+ WITH_PLSQL */` : '/*+ APPEND */'} into "${targetSchema}"."${tableName}" (${externalColumnNames.join(",")})\n${plsql ? `WITH\n${plsql}\n` : ''}select ${externalSelectList.join(",")} from ${externalTableName}`
  }

  generateCopyOperation(tableMetadata,tableInfo,externalColumnNames,externalColumnDefinitions,externalSelectList,copyColumnDefinitions) {
		  
   	this.dbi.SQL_DIRECTORY_NAME = this.SQL_DIRECTORY_NAME
    const externalTableName = `"${this.targetSchema}"."YXT-${crypto.randomBytes(this.RANDOM_OBJECT_LENGTH).toString("hex").toUpperCase()}"`;

    tableInfo.copy = {
      ddl          : this.generateExternalTableDefinition(tableMetadata,externalTableName,externalColumnDefinitions,copyColumnDefinitions)
    , dml          : this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,externalTableName,externalColumnNames,externalSelectList,this.getPLSQL(tableInfo.dml)) 
	, drop         : `drop table ${externalTableName}`
	}
  }

  async generateStatementCache(vendor) {
	  
     /*
     **
     ** Turn the generated DDL Statements into an array and execute them as single batch via YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS()
     **
     */
	 
	this.SQL_DIRECTORY_NAME = `"YDIR-${crypto.randomBytes(this.RANDOM_OBJECT_LENGTH).toString("hex").toUpperCase()}"`
	
    const sourceDateFormatMask = this.dbi.getDateFormatMask(vendor);
    const sourceTimeStampFormatMask = this.dbi.getTimeStampFormatMask(vendor);
    const oracleDateFormatMask = this.dbi.getDateFormatMask('Oracle');
    const oracleTimeStampFormatMask = this.dbi.getTimeStampFormatMask('Oracle');
    
    let setOracleDateMask = '';
    let setSourceDateMask = '';
    
    if (sourceDateFormatMask !== oracleDateFormatMask) {
      setOracleDateMask = `execute immediate 'ALTER SESSION SET NLS_DATE_FORMAT = ''${oracleDateFormatMask}''';\n  `; 
      setSourceDateMask = `;\n  execute immediate 'ALTER SESSION SET NLS_DATE_FORMAT = ''${sourceDateFormatMask}'''`; 
    }
    
    let setOracleTimeStampMask = ''
    let setSourceTimeStampMask = ''
    
    if (sourceTimeStampFormatMask !== oracleTimeStampFormatMask) {
      setOracleTimeStampMask = `execute immediate 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''${oracleTimeStampFormatMask}''';\n  `; 
      setSourceTimeStampMask = `;\n  execute immediate 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''${sourceTimeStampFormatMask}'''`; 
    }

    const typeMappings = this.getTypeMappings();
	const sqlStatement = `begin :sql := YADAMU_IMPORT.GENERATE_STATEMENTS(:metadata, :schema, :typeMappings);end;`;

    // console.log(JSON.stringify(this.metadata," ",2));
    const metadataLob = await this.getMetadataLob()
	const startTime = performance.now()
    const results = await this.dbi.executeSQL(sqlStatement,{sql:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , metadata:metadataLob, schema:this.targetSchema, typeMappings:typeMappings});
	// this.dbi.yadamuLogger.trace([this.constructor.name],`${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
    await metadataLob.close();
    const statementCache = JSON.parse(results.outBinds.sql);
    
    const tables = Object.keys(this.metadata); 
    tables.forEach((table,idx) => {
      
	  const tableMetadata = this.metadata[table];
      const tableName = tableMetadata.tableName;
	  const tableInfo = statementCache[tableName];
	  
      tableInfo.columnNames     = tableMetadata.columnNames
      tableInfo.sizeConstraints = tableMetadata.sizeConstraints
      tableInfo.insertMode      = 'Batch';      
	  tableInfo.dataFile        = tableMetadata.dataFile
      tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE
      tableInfo._SPATIAL_FORMAT = this.dbi.INBOUND_SPATIAL_FORMAT
      
	  const dataTypes           = YadamuLibrary.decomposeDataTypes(tableInfo.targetDataTypes)
      tableInfo.binds           = this.generateBinds(dataTypes,tableInfo,tableMetadata);

	  if (tableInfo.lobColumns) {
		// Do not 'copy' binds to lobBinds. binds is a collection of objects and we do not want to change properties of the objects in binds when we modify corresponding properties in lobBinds.
		tableInfo.lobBinds = this.generateBinds(dataTypes, tableInfo,tableMetadata);

        // Reorder select list to enable LOB optimization.
        // Columns with LOB data types must come last in the insert column list
		
		tableInfo.binds = tableInfo.lobBinds.map((bind) => {
		  switch (bind.type) {
		    case oracledb.CLOB:
		      return { type: oracledb.STRING, maxSize : this.dbi.CACHELOB_MAX_SIZE}
			  break;
		    case oracledb.BLOB:
		      return { type: oracledb.BUFFER, maxSize : this.dbi.CACHELOB_MAX_SIZE}
			  break;
		    default:
		      return bind;	
		  }			 
	    });

		this.orderColumnsByBindType(tableInfo);	    
	  }
	  else {
		// Fill bindOrdering with values 1..n
		tableInfo.bindOrdering = [...Array(tableInfo.columnNames.length).keys()]
	  }
	
	  // Some Drivers will return (some) numeric values as strings. The Writer will need to check for this and adjust the binds accordingly before performing an insert.
	  // Create a list of the binds that need checking (NUMBER). Do this after LOB reording  
	
	  tableInfo.numericBindPositions = []
	  tableInfo.binds.forEach((bind,idx) => {
		 if ((bind.type === oracledb.NUMBER) || (bind.type === oracledb.DB_TYPE_NUMBER) || (bind.type === oracledb.DB_TYPE_BINARY_FLOAT) || (bind.type === oracledb.DB_TYPE_BINARY_DOUBLE)) {
		   tableInfo.numericBindPositions.push(idx)
		 }
	  })
	 
      let includesObjectTypes = false;        
      
	  // const nullSettings =  ' NULLIF ${copyColumnDefinition}=BLANKS'
	  const nullSettings = ''
	  
      const assignments = [];
      const operators = [];
      const variables = []
      const values = []
	  const externalSelectList = []
	  const copyColumnDefinitions = []
      const externalColumnDefinitions = []
	  const externalColumnNames = []
	  
	  const declarations = tableInfo.columnNames.map((column,idx) => {
        variables.push(`"V_${column}"`);
        let targetDataType = tableInfo.targetDataTypes[idx];
        let externalDataType = undefined
		let copyColumnDefinition = `"${column}"`
		let value = `:${(idx+1)}`
	    let externalSelect = copyColumnDefinition
        switch (targetDataType) {
          case "GEOMETRY":
          case "GEOGRAPHY":
          case "\"MDSYS\".\"SDO_GEOMETRY\"":
		     externalDataType = "CLOB"
			 copyColumnDefinition = `${copyColumnDefinition} ${this.LOADER_CLOB_TYPE}${nullSettings}` 
		     switch (this.SPATIAL_FORMAT) {       
               case "WKB":
               case "EWKB":
                 value = `OBJECT_SERIALIZATION.DESERIALIZE_WKBGEOMETRY(:${(idx+1)})`;
		         externalSelect = `case when LENGTH("${column}") > 0 then  OBJECT_SERIALIZATION.DESERIALIZE_WKBGEOMETRY(OBJECT_SERIALIZATION.DESERIALIZE_HEX_BLOB("${column}")) else NULL end`
                 break;
               case "WKT":
               case "EWKT":
                 value = `OBJECT_SERIALIZATION.DESERIALIZE_WKTGEOMETRY(:${(idx+1)})`;
		         externalSelect = `OBJECT_SERIALIZATION.DESERIALIZE_WKTGEOMETRY("${column}")`
                 break;
               case "GeoJSON":
                 value = `OBJECT_SERIALIZATION.${this.GEOJSON_FUNCTION}(:${(idx+1)})`;
		         externalSelect = `OBJECT_SERIALIZATION.${this.GEOJSON_FUNCTION}("${column}")`
                 break;
               default:
            }
            break
          case "RAW":
		    const length = tableMetadata.sizeConstraints[tableInfo.bindOrdering[idx]]*2
			switch (true) {
			  case (length > 32767):
		        externalDataType =  'CLOB'
		        copyColumnDefinition = `${copyColumnDefinition}  ${this.LOADER_CLOB_TYPE}`
	            externalSelect = `OBJECT_SERIALIZATION.DESERIALIZE_HEX_BLOB("${column}")`
				break;
			  default:
		        externalDataType = `VARCHAR2(${length})`			    
		        copyColumnDefinition = `${copyColumnDefinition}  CHAR(${length})`
	            externalSelect = `HEXTORAW(TRIM("${column}"))`
		    }
			break;
          case "XMLTYPE":
		    externalDataType = "CLOB"
            copyColumnDefinition = `${copyColumnDefinition} ${this.LOADER_CLOB_TYPE}${nullSettings}` 
		    value = `OBJECT_SERIALIZATION.DESERIALIZE_XML(:${(idx+1)})`;
	        externalSelect = `case when LENGTH("${column}") > 0 then OBJECT_SERIALIZATION.DESERIALIZE_XML("${column}") else NULL end`
            break
          case "BFILE":
		    if (this.OBJECTS_AS_JSON) {
              value = `OBJECT_TO_JSON.DESERIALIZE_BFILE(:${(idx+1)})`;
			}
			else {
			  value = `OBJECT_SERIALIZATION.DESERIALIZE_BFILE(:${(idx+1)})`;
            }
  	        externalDataType = 'VARCHAR2(2048)'
  		    copyColumnDefinition = `"${column}" CHAR(2048)`
            externalSelect = value.replace(`:${idx+1}`,`"${column}"`)
            break;
          case "JSON":
            // value = this.dbi.DB_VERSION > 19  ? `JSON(:${(idx+1)})` : value
  	        externalDataType = 'BLOB'
            copyColumnDefinition = `${copyColumnDefinition} ${this.LOADER_CLOB_TYPE}${nullSettings}` 
            externalSelect = `case when LENGTH("${column}") > 0 then "${column}" else NULL end`
			break
          case "ANYDATA":
  	        externalDataType = 'CLOB'
  		    copyColumnDefinition = `${copyColumnDefinition} ${this.LOADER_CLOB_TYPE}`
            value = `ANYDATA.convertVARCHAR2(:${(idx+1)})`;
  		    externalSelect = value.replace(`:${idx+1}`,`"${column}"`)
            break;
		  case 'NCHAR':
          case 'NVARCHAR2':
		  case 'CHAR':
		  case 'VARCHAR2':
		    copyColumnDefinition = `${copyColumnDefinition} CHAR(${tableMetadata.sizeConstraints[tableInfo.bindOrdering[idx]]*this.dbi.BYTE_TO_CHAR_RATIO})${nullSettings}`
		    break; 
		  case 'BLOB':
  	        externalDataType = 'CLOB'
  		    copyColumnDefinition = `${copyColumnDefinition} ${this.LOADER_CLOB_TYPE}${nullSettings}`
            externalSelect = `case when LENGTH("${column}") > 0 then OBJECT_SERIALIZATION.DESERIALIZE_HEX_BLOB("${column}") else NULL end`
		    break; 		  
		  case 'NCLOB':
		  case 'CLOB':
            copyColumnDefinition = `${copyColumnDefinition} ${this.LOADER_CLOB_TYPE}${nullSettings}`
            externalSelect = `case when LENGTH("${column}") > 0 then "${column}" else NULL end`
            break;
          case "DATE":
  	        externalDataType = 'CHAR(32)'
  		    copyColumnDefinition = `"${column}" ${externalDataType}`
            externalSelect = `to_date(substr("${column}",1,19),'YYYY-MM-DD"T"HH24:MI:SS')` 
            // copyColumnDefinition = `"${column}" CHAR(36) DATE_FORMAT DATE 'YYYY-MM_DD"T"HH24:MI:SS#########"Z"'`
		    break;
          case "BOOLEAN":
  	        externalDataType = 'CHAR(5)'
  		    copyColumnDefinition = `"${column}" ${externalDataType}`
            externalSelect = `case when "${column}" is NULL then NULL when LENGTH("${column}") = 0 then NULL when "${column}" = 'true' then HEXTORAW('01') else HEXTORAW('00') end` 
		    break;
          default:
		    if ((targetDataType.indexOf('TIMESTAMP') === 0) ) {
		      externalDataType = 'CHAR(36)'
			  copyColumnDefinition = `"${column}" ${externalDataType}`
		      externalSelect = targetDataType.indexOf('ZONE') > 0 ? `to_timestamp_tz("${column}",'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')` : `to_timestamp("${column}",'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"')` 
			  /*
			  switch (true) {
				case (targetDataType.indexOf('LOCAL') > 0):
				   copyColumnDefinition = `"${column}" CHAR(36) DATE_FORMAT TIMESTAMP WITH LOCAL TIME ZONE 'YYYY-MM_DD"T"HH24:MI:SS.FF9"Z"'`
    		       break;
				case (targetDataType.indexOf('ZONE') > 0):
				   copyColumnDefinition = `"${column}" CHAR(36) DATE_FORMAT TIMESTAMP WITH TIME ZONE 'YYYY-MM_DD"T"HH24:MI:SS.FF9"Z"'`
    		       break;
				default:
				   copyColumnDefinition = `"${column}" CHAR(36) DATE_FORMAT TIMESTAMP 'YYYY-MM_DD"T"HH24:MI:SS.FF9'`
    		       break;
			  }
			  */
		    }
		    if ((targetDataType.indexOf('INTERVAL') === 0) && (targetDataType.indexOf('DAY') > 0) && (targetDataType.indexOf('SECOND') > 0)) {
              value = `OBJECT_SERIALIZATION.DESERIALIZE_ISO8601_DSINTERVAL(:${(idx+1)})`;
		      break;
		    }
		    if (targetDataType.indexOf('.') > -1) {
		      includesObjectTypes = true;
 		      externalDataType = "CLOB"
			  copyColumnDefinition = `${copyColumnDefinition} ${this.LOADER_CLOB_TYPE}`
              value = `"#${targetDataType.slice(targetDataType.indexOf(".")+2,-1)}"(:${(idx+1)})`;
			  externalSelect = value.replace(`:${idx+1}`,`"${column}"`)
			  break;
		    }
        } 
        // Append length to bounded datatypes if necessary
        targetDataType = (StatementGenerator.BOUNDED_TYPES.includes(targetDataType) && targetDataType.indexOf('(') === -1)  ? `${targetDataType}(${tableMetadata.sizeConstraints[tableInfo.bindOrdering[idx]]})` : targetDataType;
		values.push(value)
		copyColumnDefinitions[tableInfo.bindOrdering[idx]]     = copyColumnDefinition
		externalColumnDefinitions[tableInfo.bindOrdering[idx]] = `"${column}" ${externalDataType || targetDataType}`
		externalSelectList[tableInfo.bindOrdering[idx]]        = externalSelect
	    externalColumnNames[tableInfo.bindOrdering[idx]]       = `"${column}"`
        return `${variables[idx]} ${targetDataType}`;
      })
	  
	  if (tableMetadata.dataFile) {
		this.generateCopyOperation(tableMetadata,tableInfo,externalColumnNames,externalColumnDefinitions,externalSelectList,copyColumnDefinitions) 

      } 
	  
      if (includesObjectTypes === true) {
        const assignments = values.map((value,idx) => {
          if (value[1] === '#') {
            return `${setOracleDateMask}${setOracleTimeStampMask}${variables[idx]} := ${value}${setSourceDateMask}${setSourceTimeStampMask}`;
          }
          else {
            return `${variables[idx]} := ${value}`;
          }
        })
        tableInfo.dml = this.generatePLSQL(this.targetSchema,tableMetadata.tableName,tableInfo.dml,tableInfo.columnNames,declarations,assignments,variables);
      }
      else  {
        tableInfo.dml = `insert /*+ APPEND */ into "${this.targetSchema}"."${tableMetadata.tableName}" (${tableInfo.columnNames.map((col) => {return `"${col}"`}).join(',')}) values (${values.join(',')})`;
      }	  

	});
	return statementCache
  }  
}

export {StatementGenerator as default }