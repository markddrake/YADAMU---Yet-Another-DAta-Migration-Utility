"use strict";

// const const { v4: uuidv4 } = require('uuid'); = require('uuid/v1');
const { v1: uuidv1 } = require('uuid');

// Support for Oracle 11.2 
// Oracle 11g has not support for parsing JSON so we send the metadata as XML !!!

// Oracle 11g does not support PL/SQL in With Clause so we need to generate PL/SQL Wrappers for operations that require a with clause
// 
// We need to assign a unique ID for each procedure generated. Serial Mode requires one for the entire operation. Parallel Mode requires a unqiue ID for each slave.
//
// Since the Wrappers have to be dropped and recreated for each table a unique ID is generated for each table.
//

const Readable = require('stream').Readable;

const StatementGenerator = require('./statementGenerator.js');

class StatementGenerator11 extends StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize) {
    super(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize)
	
	// 11.x does not support GeoJSON. We need to use WKX to convert GeoJSON to WKT
	this.GEOJSON_FUNCTION = 'DESERIALIZE_WKTGEOMETRY'
  }

  // In 11g the seperator character appears to be \r rather than \n

  getPLSQL(dml) {
    return dml.substring(dml.indexOf('\rWITH\r')+5,dml.indexOf('\rselect'));
  }
 
    
  metadataToXML() {
            
    // I know.... Attmpting to build XML via string concatenation will end in tears...

    const tablesXML = Object.keys(this.metadata).map((tableID) => {
      const table = this.metadata[tableID]
      const columnsXML = JSON.parse(`[${table.columns}]`).map((columnName) => {return `<column>${columnName}</column>`}).join('');
      const dataTypesXML = table.dataTypes.map((dataType) => {return `<dataType>${dataType}</dataType>`}).join('');
      const sizeConstraintsXML = table.sizeConstraints.map((sizeConstraint) => {return `<sizeConstraint>${sizeConstraint === null ? '' : sizeConstraint}</sizeConstraint>`}).join('');
      return `<table><vendor>${table.vendor}</vendor><owner>${table.owner}</owner><tableName>${table.tableName}</tableName><columns>${columnsXML}</columns><dataTypes>${dataTypesXML}</dataTypes><sizeConstraints>${sizeConstraintsXML}</sizeConstraints></table>`
    }).join('');
    return `<metadata>${tablesXML}</metadata>`
    
  }
  
  async getMetadataLob() {
    const metadataXML = this.metadataToXML();    
    return await this.dbi.blobFromString(metadataXML);
  }
      
}

module.exports = StatementGenerator11