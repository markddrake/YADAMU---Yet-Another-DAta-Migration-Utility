"use strict";

// Support for Oracle 11.2 
// Oracle 11g has not support for parsing JSON so we send the metadata as XML !!!

const Readable = require('stream').Readable;

const StatementGenerator = require('./StatementGenerator.js');

class StatementGenerator11 extends StatementGenerator {
  
  // I know.... Attmpting to build XML via string concatenation will end in tears...

  
  constructor(dbi, targetSchema, metadata, batchSize, commitSize, lobCacheSize, importWrapper) {
    super(dbi, targetSchema, metadata, batchSize, commitSize, lobCacheSize)
    this.importWapper = importWrapper
    this.mappinhgs = {}
  }
    
  generateDDL(targetSchema,tableName,dml,columns,declararions,assignments,variables){
   const plsqlFunctions = dml.substring(dml.indexOf('\WITH\n')+5,dml.indexOf('\nselect'));
   return `create or replace function "${this.targetSchema}"."{this.importWraper}"(P_TABLE_OWNER VARCHAR2,P_ANYDATA ANYDATA)\nreturn CLOB\nas\n${withClause}begin\nreturn SERIALIZE_OBJECT(P_TABLE_OWNER, P_ANYDATA);\nend;`;
  }

  getPLSQL(dml) {
    return dml.substring(dml.indexOf('\rWITH\r')+5,dml.indexOf('\rselect'));
  }

  metadataToXML() {
            
    const tablesXML = Object.keys(this.metadata).map(function(tableID){
      const table = this.metadata[tableID]
      const columnsXML = JSON.parse(`[${table.columns}]`).map(function(columnName){return `<column>${columnName}</column>`},this).join('');
      const dataTypesXML = table.dataTypes.map(function(dataType){return `<dataType>${dataType}</dataType>`},this).join('');
      const sizeConstraintsXML = table.sizeConstraints.map(function(sizeConstraint){return `<sizeConstraint>${sizeConstraint}</sizeConstraint>`},this).join('');
      return `<table><vendor>${table.vendor}</vendor><owner>${table.owner}</owner><tableName>${table.tableName}</tableName><columns>${columnsXML}</columns><dataTypes>${dataTypesXML}</dataTypes><sizeConstraints>${sizeConstraintsXML}</sizeConstraints></table>`
    },this).join('');
    return `<metadata>${tablesXML}</metadata>`
    
  }
  
  async getMetadataLob() {
    
    const metadataXML = this.metadataToXML();    
    return await this.dbi.lobFromString(metadataXML);
  }
      
}

module.exports = StatementGenerator11