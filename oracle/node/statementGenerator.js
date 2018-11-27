"use strict";
const oracledb = require('oracledb');

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
           return { type: oracledb.STRING, maxSize : dataType.length}
         case 'NCHAR':
         case 'NVARCHAR2':
           return { type: oracledb.STRING, maxSize : dataType.length * 2}
         case 'DATE':
         case 'TIMESTAMP':
            return { type: oracledb.DATE}
         case 'INTERVAL':
            return { type: oracledb.STRING, maxSize : 12}
         case 'CLOB':
         case 'NCLOB':
         case 'ANYDATA':
           tableInfo.lobCount++;
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type] }
         case 'BOOLEAN':
            return { type: oracledb.STRING, maxSize : 5}         
         case 'XMLTYPE':
           // Cannot Bind XMLTYPE > 32K as String: ORA-01461: can bind a LONG value only for insert into a LONG column when constructing XMLTYPE
           tableInfo.lobCount++;
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type]}
         case 'JSON':
           // Defalt JSON Storeage model: JSON store as CLOB
           // JSON store as BLOB can lead to Error: ORA-40479: internal JSON serializer error during export operations.
           tableInfo.lobCount++;
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type]}
         case 'BLOB':
           tableInfo.lobCount++;
           return {type : oracledb.BUFFER, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type] }
         case 'RAW':
           return { type :oracledb.STRING, maxSize : parseInt(dataTypeSizes[idx])*2}
         case 'RAW(1)':
           return { type :oracledb.STRING, maxSize : 5}
         case 'BFILE':
           return { type :oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType.type] }
         default:
           if (dataType.type.indexOf('.') > -1) {
             tableInfo.lobCount++;
             return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH['XMLTYPE']}
           }
           return {type : oracledb.STRING, maxSize :  dataType.length}
       }
     })
  
  }
  
  async generateStatementCache(schema, systemInformation, metadata) {
  
    const sqlStatement = `begin :sql := JSON_IMPORT.GENERATE_STATEMENTS(:metadata, :schema);\nEND;`;
      
     /*
     **
     ** Turn the generated DDL Statements into an array and execute them as single batch via JSON_EXPORT_DDL.APPLY_DDL_STATEMENTS()
     **
     */
      
    try {
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
                       ddlStatements[idx] = tableInfo.ddl;
                       let plsqlRequired = false;         
                       // /*
                       const assignments = [];
                       const operators = [];
                       const variables = []
                       const values = []
                       const declarations = columns.map(function(column,idx) {
                         variables.push(`"V_${column}"`);
                         let targetDataType =  tableInfo.targetDataTypes[idx];
                         const dataType = Yadamu.decomposeDataType(targetDataType);
                         switch (dataType.type) {
                           case "XMLTYPE":
                              values.push(`XMLTYPE.createXML(:${(idx+1)})`);
                              break
                            case "BFILE":
                              values.push(`OBJECT_SERIALIZATION.CHAR2BFILE(:${(idx+1)})`);
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
                           return  `${variables[idx]} := ${values[idx]}`
                         })
                         let plsqlFunctions = tableInfo.dml.substring(tableInfo.dml.indexOf('\WITH\n')+5,tableInfo.dml.indexOf('\nselect'));
                         tableInfo.dml = `declare\n  ${declarations.join(';\n  ')};\n\n${plsqlFunctions}\nbegin\n  ${assignments.join(';\n  ')};\n\n  insert into "${schema}"."${table}" (${tableMetadata.columns}) values (${variables.join(',')});\nend;`;      
                       }
                       else  {
                         tableInfo.dml = `insert into "${schema}"."${table}" (${tableMetadata.columns}) values (${values.join(',')})`;
                       }
                       tableInfo.binds = this.generateBinds(tableInfo,metadata[table].sizeConstraints);
                       // */
                       /*
                       tableInfo.binds = this.generateBinds(tableInfo,metadata[table].sizeConstraints);
                       const insertAsSelectList = tableInfo.targetDataTypes.map(function(targetDataType,idx) {  
                                                                            const dataType = Yadamu.decomposeDataType(targetDataType);
                                                                            switch (dataType.type) {
                                                                              case "XMLTYPE":
                                                                                // Cannot passs XMLTYPE as BUFFER
                                                                                // Reason: ORA-06553: PLS-307: too many declarations of 'XMLTYPE' match this call
                                                                                // return 'XMLTYPE(:' + (idx+1) + ",NLS_CHARSET_ID('AL32UTF8'))"
                                                                                return 'XMLPARSE(DOCUMENT :' + (idx+1) + ')';
                                                                              case "BFILE":
                                                                                return 'OBJECT_SERIALIZATION.CHAR2BFILE(:' + (idx+1) + ')'
                                                                              case "ANYDATA":
                                                                                return 'ANYDATA.convertVARCHAR2(:' + (idx+1) + ')'
                                                                              default:
                                                                                if (targetDataType.indexOf('.') > -1) {
                                                                                  plsqlRequired= true;         
                                                                                  return '"#' + targetDataType.slice(targetDataType.indexOf(".")+2,-1) + '"(:' + (idx+1) + ')'
                                                                                }
                                                                                else {
                                                                                 return ':' + (idx+1)
                                                                                }
                                                                            }
                       });                                                                                                                     
                       if (tableInfo.containsObjects) {
                         tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('\nselect'));
                         tableInfo.dml += '\nselect ' + insertAsSelectList.join(',') +' from dual;';
                       }
                       else {
                         tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(')')+2);
                         tableInfo.dml += 'values (' + insertAsSelectList.join(',') +')';
                       }
                       */
      },this);
      
      if (this.ddlRequired) {;
        await this.executeDDL(schema, systemInformation, ddlStatements);
      }
      
      return statementCache
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }
  }
  
}

module.exports = StatementGenerator;