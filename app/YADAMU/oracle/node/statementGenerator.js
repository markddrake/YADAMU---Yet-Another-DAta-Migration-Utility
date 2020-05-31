"use strict";

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamu.js');

const LOB_TYPES = [oracledb.CLOB,oracledb.BLOB]
     
class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
	this.objectsAsJSON = this.dbi.systemInformation.objectFormat === 'JSON';
    this.batchSize = batchSize
    this.commitSize = commitSize;
	
	this.BIND_LENGTH = {
      BLOB          : this.dbi.parameters.LOB_MAX_SIZE
    , CLOB          : this.dbi.parameters.LOB_MAX_SIZE
    , JSON          : this.dbi.parameters.LOB_MAX_SIZE
    , NCLOB         : this.dbi.parameters.LOB_MAX_SIZE
    , OBJECT        : this.dbi.parameters.LOB_MAX_SIZE
    , XMLTYPE       : this.dbi.parameters.LOB_MAX_SIZE
    , ANYDATA       : this.dbi.parameters.LOB_MAX_SIZE
    , GEOMETRY      : this.dbi.parameters.LOB_MAX_SIZE
	, NUMBER        : 19
	, BOOLEAN       : 5
    , BFILE         : 4096
    , DATE          : 24
    , TIMESTAMP     : 30
    , INTERVAL      : 16
    }  

    this.GEOJSON_FUNCTION = 'DESERIALIZE_GEOJSON'
  }
   
  generateBinds(tableInfo, metadata) {
      
     // Binds describe the format that will be used to supply the data. Eg with SQLServer BIGINT values will be presented as String

     tableInfo.lobColumns = false;
     return tableInfo.dataTypes.map(function (dataType,idx) {
       if (!dataType.length) {
          dataType.length = parseInt(metadata.sizeConstraints[idx]);
       }
       switch (dataType.type) {
         case 'NUMBER':
           if ((metadata.source.vendor === 'MSSQLSERVER') && (metadata.source.dataTypes[idx] === 'bigint')) {
             return { type: oracledb.STRING, maxSize : this.BIND_LENGTH.NUMBER}
           }
         case 'FLOAT':
         case 'BINARY_FLOAT':
         case 'BINARY_DOUBLE':
           if ((metadata.source.vendor === 'SNOWFLAKE') && ['NUMBER','DECIMAL','NUMERIC','FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE','DOUBLE PRECISION', 'REAL'].includes(metadata.source.dataTypes[idx])) {
             return { type: oracledb.STRING, maxSize : dataType.length + 3}
           }
           return { type: oracledb.NUMBER }
         case 'RAW':
           return { type: oracledb.BUFFER, maxSize : dataType.length}
         case 'CHAR':
         case 'VARCHAR':
         case 'VARCHAR2':
           return { type: oracledb.STRING, maxSize : dataType.length * 2}
         case 'NCHAR':
         case 'NVARCHAR2':
           return { type: oracledb.STRING, maxSize : dataType.length * 2}
         case 'DATE':
         case 'TIMESTAMP':
           return { type: oracledb.STRING, maxSize : 35}
         case 'INTERVAL':
            return { type: oracledb.STRING, maxSize : 12}
         case 'CLOB':
         case 'NCLOB':
         case 'ANYDATA':
           tableInfo.lobColumns = true;
           // return {type : oracledb.CLOB}
           return {type : oracledb.CLOB, maxSize : this.BIND_LENGTH.ANYDATA }
         case 'XMLTYPE':
           // Cannot Bind XMLTYPE > 32K as String: ORA-01461: can bind a LONG value only for insert into a LONG column when constructing XMLTYPE
           tableInfo.lobColumns = true;
           // return {type : oracledb.CLOB}
           return {type : oracledb.CLOB, maxSize : this.BIND_LENGTH.CLOB}
         case 'JSON':
           // Defalt JSON Storeage model: JSON store as CLOB
           // JSON store as BLOB can lead to Error: ORA-40479: internal JSON serializer error during export operations.
           // return {type : oracledb.CLOB}
		   switch (this.dbi.jsonDataType) {
			  case 'JSON':
			  case 'BLOB':
                tableInfo.lobColumns = true;
                return {type : oracledb.BLOB, maxSize : this.BIND_LENGTH.JSON}
			  case 'CLOB':
                tableInfo.lobColumns = true;
                return {type : oracledb.CLOB, maxSize : this.BIND_LENGTH.JSON}
			  default:
			    return {type : oracledb.STRING, maxSize : 32767 }
		   }
         case 'BLOB':
           // return {type : oracledb.BUFFER}
           // return {type : oracledb.BUFFER, maxSize : BIND_LENGTH.BLOB }
           tableInfo.lobColumns = true;
           return {type : oracledb.BLOB, maxSize : this.BIND_LENGTH.BLOB}
         case 'RAW':
           // return { type :oracledb.STRING, maxSize : parseInt(metadata.sizeConstraints[idx])*2}
           return { type :oracledb.BUFFER, maxSize : parseInt(metadata.sizeConstraints[idx])}
         case 'BFILE':
           return { type :oracledb.STRING, maxSize : this.BIND_LENGTH.BFILE }
         case 'BOOLEAN':
            return { type: oracledb.STRING, maxSize :  this.BIND_LENGTH.BOOLEAN }         
         case 'GEOMETRY':
         case "\"MDSYS\".\"SDO_GEOMETRY\"":
           tableInfo.lobColumns = true;
           // return {type : oracledb.CLOB}
           switch (this.spatialFormat) { 
             case "WKB":
             case "EWKB":
               return {type : oracledb.BLOB, maxSize : this.BIND_LENGTH.GEOMETRY}
               break;
             case "WKT":
             case "EWKT":
             case "GeoJSON":
               return {type : oracledb.CLOB, maxSize : this.BIND_LENGTH.JSON}
               break;
             default:
           }
           break;   
         default:
           if (dataType.type.indexOf('.') > -1) {
             // return {type : oracledb.CLOB}
             tableInfo.lobColumns = true
			 return {type : oracledb.CLOB, maxSize : this.BIND_LENGTH.OBJECT}
           }
           return {type : oracledb.STRING, maxSize :  dataType.length}
       }
     },this)
  
  }
  
  async getMetadataLob() {

    return await this.dbi.blobFromJSON({metadata: this.metadata});  
      
  }
 
  getPLSQL(dml) {
    
    return dml.substring(dml.indexOf('\nWITH\n')+5,dml.indexOf('\nselect'));
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
	  switch (tableInfo.dataTypes[colIdx].type) {
		// GEOMETRY is inserted by Stored Procedure.. Do not move to end of List.
        case "GEOMETRY":
        case "\"MDSYS\".\"SDO_GEOMETRY\"":
		  bindOrdering.push(parseInt(colIdx))
		  break
        case "XMLTYPE":
		  bindOrdering.push(parseInt(colIdx))
		  break
		default:
          if (!LOB_TYPES.includes(tableInfo.lobBinds[colIdx].type)) {
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
	
	const columns = []
	const targetDataTypes = []
	const sizeConstraints = []
	const dataTypes = []
	const binds = []
	const lobBinds = []
	
	for (const idx in bindOrdering) {
	  columns.push(tableInfo.columns[bindOrdering[idx]])
	  targetDataTypes.push(tableInfo.targetDataTypes[bindOrdering[idx]])
	  sizeConstraints.push(tableInfo.sizeConstraints[bindOrdering[idx]])
	  dataTypes.push(tableInfo.dataTypes[bindOrdering[idx]])
	  binds.push(tableInfo.binds[bindOrdering[idx]])
	  lobBinds.push(tableInfo.lobBinds[bindOrdering[idx]])
	}
	
    tableInfo.columns = columns;
	tableInfo.targetDataTypes = targetDataTypes;
	tableInfo.sizeConstraints = sizeConstraints;
	tableInfo.dataTypes = dataTypes;
    tableInfo.binds = binds;
    tableInfo.lobBinds = lobBinds
    tableInfo.bindOrdering = bindOrdering
	
    return tableInfo
  }
  
  generateWrapperNames(tableInfo) {
	// Only Required with release 11.2.
  }

  async generateStatementCache(executeDDL, vendor) {
	  
     /*
     **
     ** Turn the generated DDL Statements into an array and execute them as single batch via YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS()
     **
     */
     
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
   
    const sqlStatement = `begin :sql := YADAMU_IMPORT.GENERATE_STATEMENTS(:metadata, :schema, :spatialFormat, :jsonStorageModel, :xmlStorageModel);\nend;`;
	
    const metadataLob = await this.getMetadataLob()
   
    const results = await this.dbi.executeSQL(sqlStatement,{sql:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , metadata:metadataLob, schema:this.targetSchema, spatialFormat:this.spatialFormat, jsonStorageModel: this.dbi.jsonStorageModel, xmlStorageModel: this.dbi.xmlStorageModel});
    await metadataLob.close();
    const statementCache = JSON.parse(results.outBinds.sql);
    const boundedTypes = ['CHAR','NCHAR','VARCHAR2','NVARCHAR2','RAW']
    const ddlStatements = [JSON.stringify({jsonColumns:null})];  
    
    const tables = Object.keys(this.metadata); 
    tables.forEach(function(table,idx) {
      const tableMetadata = this.metadata[table];
	  
	  if (tableMetadata.WITH_CLAUSE) {
		generateWrapperName(tableMetadata);
	  }
	  
      const tableInfo = statementCache[tableMetadata.tableName];
	  
      tableInfo.batchSize = this.batchSize
      tableInfo.commitSize = this.commitSize;
	  tableInfo.spatialFormat = this.spatialFormat

      tableInfo.columns = JSON.parse('[' + tableMetadata.columns + ']')
	  tableInfo.sizeConstraints = tableMetadata.sizeConstraints
	  
 	  tableInfo.dataTypes = this.dbi.decomposeDataTypes(tableInfo.targetDataTypes);
      tableInfo.binds = this.generateBinds(tableInfo,this.metadata[table]);

	  if (tableInfo.lobColumns) {
		// Do not 'copy' binds to lobBinds. binds is a collection of objects and we do not want to change properties of the objects in binds when we modify corresponding properties in lobBinds.
		tableInfo.lobBinds = this.generateBinds(tableInfo,tableMetadata);

        // Reorder select list to enable LOB optimization.
        // Columns with LOB data types must come last in the insert column list
		
		tableInfo.binds = tableInfo.lobBinds.map(function(bind) {
		  switch (bind.type) {
		    case oracledb.CLOB:
		      return { type: oracledb.STRING, maxSize : this.dbi.parameters.LOB_MIN_SIZE }
			  break;
		    case oracledb.BLOB:
		      return { type: oracledb.BUFFER, maxSize : this.dbi.parameters.LOB_MIN_SIZE }
			  break;
		    default:
		      return bind;	
		  }			 
	    },this);

		this.orderColumnsByBindType(tableInfo);	    
	  }
	  else {
		// Fill bindOrdering with values 1..n
		tableInfo.bindOrdering = [...Array(tableInfo.columns.length).keys()]
	  }
	
      let plsqlRequired = false;        

      const assignments = [];
      const operators = [];
      const variables = []
      const values = []

      const declarations = tableInfo.columns.map(function(column,idx) {
        variables.push(`"V_${column}"`);
        let targetDataType =  tableInfo.targetDataTypes[idx];
        const dataType = tableInfo.dataTypes[idx];
        switch (dataType.type) {
          case "GEOMETRY":
          case "\"MDSYS\".\"SDO_GEOMETRY\"":
             switch (this.spatialFormat) {
               case "WKB":
               case "EWKB":
                 values.push(`OBJECT_SERIALIZATION.DESERIALIZE_WKBGEOMETRY(:${(idx+1)})`);
                 break;
               case "WKT":
               case "EWKT":
                 values.push(`OBJECT_SERIALIZATION.DESERIALIZE_WKTGEOMETRY(:${(idx+1)})`);
                 break;
               case "GeoJSON":
                 values.push(`OBJECT_SERIALIZATION.${this.GEOJSON_FUNCTION}(:${(idx+1)})`);
                 break;
               default:
            }
            break
          case "XMLTYPE":
             values.push(`OBJECT_SERIALIZATION.DESERIALIZE_XML(:${(idx+1)})`);
             break
           case "BFILE":
		     if (this.objectsAsJSON) {
               values.push(`OBJECT_TO_JSON.DESERIALIZE_BFILE(:${(idx+1)})`);
			 }
			 else {
			   values.push(`OBJECT_SERIALIZATION.DESERIALIZE_BFILE(:${(idx+1)})`);
             }
             break;
          case "ANYDATA":
            values.push(`ANYDATA.convertVARCHAR2(:${(idx+1)})`);
            break;
          case "BOOLEAN":
            values.push(`case when :${(idx+1)} = 'true' then HEXTORAW('01') else HEXTORAW('00') end`)
            break;
          default:
            if (targetDataType.indexOf('.') > -1) {
              plsqlRequired = true;
              values.push(`"#${targetDataType.slice(targetDataType.indexOf(".")+2,-1)}"(:${(idx+1)})`);
			}
            else {
              values.push(`:${(idx+1)}`);
            }
        } 
        // Append length to bounded datatypes if necessary
        targetDataType = (boundedTypes.includes(targetDataType) && targetDataType.indexOf('(') === -1)  ? `${targetDataType}(${tableInfo.sizeConstraints[idx]})` : targetDataType;
        return `${variables[idx]} ${targetDataType}`;
      },this)
      
      if (plsqlRequired === true) {
        const assignments = values.map(function(value,idx) {
          if (value[1] === '#') {
            return `${setOracleDateMask}${setOracleTimeStampMask}${variables[idx]} := ${value}${setSourceDateMask}${setSourceTimeStampMask}`;
          }
          else {
            return `${variables[idx]} := ${value}`;
          }
        },this)
        tableInfo.dml = this.generatePLSQL(this.targetSchema,this.metadata[table].tableName,tableInfo.dml,tableInfo.columns,declarations,assignments,variables);
      }
      else  {
        tableInfo.dml = `insert into "${this.targetSchema}"."${this.metadata[table].tableName}" (${tableInfo.columns.map(function(col){return `"${col}"`}).join(',')}) values (${values.join(',')})`;
      }
      
	  if (tableInfo.ddl !== null) {
        ddlStatements.push(tableInfo.ddl);
      }
	  
    },this);
    
    if (executeDDL === true) {
      await this.dbi.executeDDL(ddlStatements);
    }
    
	return statementCache
  }  
}

module.exports = StatementGenerator;