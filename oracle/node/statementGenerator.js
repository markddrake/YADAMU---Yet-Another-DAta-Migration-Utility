"use strict";

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamuCore.js');
const OracleCore = require('./oracleCore.js');

const LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const LOB_STRING_MAX_LENGTH    = 64 * 1024;
const BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const STRING_MAX_LENGTH        =  4 * 1024;

const DATA_TYPE_STRING_LENGTH = {
  BLOB          : LOB_STRING_MAX_LENGTH
, CLOB          : LOB_STRING_MAX_LENGTH
, JSON          : LOB_STRING_MAX_LENGTH
, NCLOB         : LOB_STRING_MAX_LENGTH
, OBJECT        : LOB_STRING_MAX_LENGTH
, XMLTYPE       : LOB_STRING_MAX_LENGTH
, ANYDATA       : LOB_STRING_MAX_LENGTH
, BFILE         : BFILE_STRING_MAX_LENGTH
, DATE          : 24
, TIMESTAMP     : 30
, INTERVAL      : 16
}  

class StatementGenerator {
  
  constructor(conn, status, logWriter) {
    
    // super();
    const statementGenerator = this;
    
    this.conn = conn;
    this.status = status;
    this.logWriter = logWriter;
    this.ddlRequired = true;
    
  }
 
  async executeDDL(schema, systemInformation, ddl) {
  
    const sqlStatement = `begin :log := JSON_EXPORT_DDL.APPLY_DDL_STATEMENTS(:ddl, :schema); end;`;
        
    try {
      const ddlLob = await OracleCore.lobFromJSON(this.conn, { systemInformation : systemInformation, ddl : ddl});  
      const results = await this.conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , ddl:ddlLob, schema:schema});
      this.ddlRequired = false;
      await ddlLob.close();
      const log = JSON.parse(results.outBinds.log);
      if (log !== null) {
        Yadamu.processLog(log, this.status, this.logWriter)
      }
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }    
  }
 
  generateBinds(tableInfo, dataTypeSizes) {
     
     tableInfo.lobCount = 0;
     return tableInfo.targetDataTypes.map(function (targetDataType,idx) {
       const dataType = Yadamu.decomposeDataType(targetDataType)
       if (!dataType.length) {
          dataType.length = parseInt(dataTypeSizes[idx]);
       }
       switch (dataType.type) {
         case 'NUMBER':
         case 'FLOAT':
         case 'BINARY_FLOAT':
         case 'BINARY_DOUBLE':
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
           return { type: oracledb.STRING, maxSize : 48}
         case 'INTERVAL':
            return { type: oracledb.STRING, maxSize : 12}
         case 'CLOB':
         case 'NCLOB':
         case 'ANYDATA':
           tableInfo.lobCount++;
           // return {type : oracledb.CLOB}
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type] }
         case 'XMLTYPE':
           // Cannot Bind XMLTYPE > 32K as String: ORA-01461: can bind a LONG value only for insert into a LONG column when constructing XMLTYPE
           tableInfo.lobCount++;
           // return {type : oracledb.CLOB}
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type]}
         case 'JSON':
           // Defalt JSON Storeage model: JSON store as CLOB
           // JSON store as BLOB can lead to Error: ORA-40479: internal JSON serializer error during export operations.
           tableInfo.lobCount++;
           // return {type : oracledb.CLOB}
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type]}
         case 'BLOB':
           tableInfo.lobCount++;
           // return {type : oracledb.BUFFER}
           return {type : oracledb.BUFFER, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type] }
         case 'RAW':
           return { type :oracledb.STRING, maxSize : parseInt(dataTypeSizes[idx])*2}
         case 'BFILE':
           return { type :oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type] }
         case 'BOOLEAN':
            return { type: oracledb.STRING, maxSize : 5}         
         case 'GEOMETRY':
           tableInfo.lobCount++;
           // return {type : oracledb.CLOB}
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type]}
         default:
           if (dataType.type.indexOf('.') > -1) {
             tableInfo.lobCount++;
             // return {type : oracledb.CLOB}
             return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH['XMLTYPE']}
           }
           return {type : oracledb.STRING, maxSize :  dataType.length}
       }
     },this)
  
  }
  
  async generateStatementCache(schema, systemInformation, metadata) {
   
    const sqlStatement = `begin :sql := JSON_IMPORT.GENERATE_STATEMENTS(:metadata, :schema);\nEND;`;
      
     /*
     **
     ** Turn the generated DDL Statements into an array and execute them as single batch via JSON_EXPORT_DDL.APPLY_DDL_STATEMENTS()
     **
     */
     
    try {
      const sourceDateFormatMask = OracleCore.getDateFormatMask(systemInformation.vendor);
      const sourceTimeStampFormatMask = OracleCore.getTimeStampFormatMask(systemInformation.vendor);
      const oracleDateFormatMask = OracleCore.getDateFormatMask('Oracle');
      const oracleTimeStampFormatMask = OracleCore.getTimeStampFormatMask('Oracle');
      
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
      
      const boundedTypes = ['CHAR','NCHAR','VARCHAR2','NVARCHAR2','RAW']
      const ddlStatements = [];  
      
      const metadataLob = await OracleCore.lobFromJSON(this.conn,{systemInformation : systemInformation, metadata: metadata});  
      const results = await this.conn.execute(sqlStatement,{sql:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , metadata:metadataLob, schema:schema});
      await metadataLob.close();

      const statementCache = JSON.parse(results.outBinds.sql);
      const tables = Object.keys(metadata); 
      tables.forEach(function(table,idx) {
                       const tableInfo = statementCache[table];
                       const tableMetadata = metadata[table];
                       const columns = JSON.parse('[' + tableMetadata.columns + ']');
                       if (tableInfo.ddl !== null) {
                         ddlStatements.push(tableInfo.ddl);
                       }
                       let plsqlRequired = false;        

                       const assignments = [];
                       const operators = [];
                       const variables = []
                       const values = []
                       
                       const declarations = columns.map(function(column,idx) {
                         variables.push(`"V_${column}"`);
                         let targetDataType =  tableInfo.targetDataTypes[idx];
                         const dataType = Yadamu.decomposeDataType(targetDataType);
                         switch (dataType.type) {
                           case "GEOMETRY":
                           case "\"MDSYS\".\"SDO_GEOMETRY\"":
                              values.push(`OBJECT_SERIALIZATION.DESERIALIZE_GEOMETRY(:${(idx+1)})`);
                              break;
                           case "XMLTYPE":
                              values.push(`OBJECT_SERIALIZATION.DESERIALIZE_XML(:${(idx+1)})`);
                              break
                            case "BFILE":
                              values.push(`OBJECT_SERIALIZATION.DESERIALIZE_BFILE(:${(idx+1)})`);
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
                         targetDataType = (boundedTypes.includes(targetDataType) && targetDataType.indexOf('(') === -1)  ? `${targetDataType}(${tableMetadata.sizeConstraints[idx]})` : targetDataType;
                         return `${variables[idx]} ${targetDataType}`;
                       })
                       if (plsqlRequired === true) {
                         const assignments = values.map(function(value,idx) {
                           if (value[1] === '#') {
                             return `${setOracleDateMask}${setOracleTimeStampMask}${variables[idx]} := ${value}${setSourceDateMask}${setSourceTimeStampMask}`;
                           }
                           else {
                             return `${variables[idx]} := ${value}`;
                           }
                         })
                         let plsqlFunctions = tableInfo.dml.substring(tableInfo.dml.indexOf('\WITH\n')+5,tableInfo.dml.indexOf('\nselect'));
                         tableInfo.dml = `declare\n  ${declarations.join(';\n  ')};\n\n${plsqlFunctions}\nbegin\n  ${assignments.join(';\n  ')};\n\n  insert into "${schema}"."${table}" (${tableMetadata.columns}) values (${variables.join(',')});\nend;`;      
                       }
                       else  {
                         tableInfo.dml = `insert into "${schema}"."${table}" (${tableMetadata.columns}) values (${values.join(',')})`;
                       }
                       tableInfo.binds = this.generateBinds(tableInfo,metadata[table].sizeConstraints);
      },this);
      
      if (this.ddlRequired) {
        await this.executeDDL(schema, systemInformation, ddlStatements);
      }
      
      return statementCache
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }
  }
  
}

module.exports = StatementGenerator;