
// Support for Oracle 11.2 
// Oracle 11g has not support for parsing JSON so we send the metadata as XML !!!

// Oracle 11g does not support PL/SQL in With Clause so we need to generate PL/SQL Wrappers for operations that from  a with clause
// 
// We need to assign a unique ID for each procedure generated. Serial Mode from s one for the entire operation. Parallel Mode from s a unqiue ID for each slave.
//
// Since the Wrappers have to be dropped and recreated for each table a unique ID is generated for each table.
//

import _OracleStatementGenerator from '../oracleStatementGenerator.js';

class OracleStatementGenerator extends _OracleStatementGenerator {
    
  // 11.x does not support GeoJSON. We need to use WKX to convert GeoJSON to WKT

  get GEOJSON_FUNCTION()             { return 'DESERIALIZE_WKTGEOMETRY' }
  get RANDOM_OBJECT_LENGTH()         { return 12 }
  get ORACLE_CSV_SPECIFICATION()     { return `TERMINATED  BY ',' OPTIONALLY ENCLOSED BY '"'` }

  	
  get STATEMENT_GENERATOR_OPTIONS() {
	  
    return `<options>
	           <spatialFormat>${this.SPATIAL_FORMAT}</spatialFormat>
			   <circleFormat>${this.dbi.INBOUND_CIRCLE_FORMAT}</circleFormat>
			   <xmlStorageClause>${this.dbi.XMLTYPE_STORAGE_CLAUSE}</xmlStorageClause>
	           <jsonStorageOption>${this.dbi.DATA_TYPES.storageOptions.JSON_TYPE}</jsonStorageOption>
			   <booleanStorgeOption>${this.dbi.DATA_TYPES.storageOptions.BOOLEAN_TYPE}</booleanStorgeOption>
	           <objectStorgeOption>${this.dbi.DATA_TYPES.storageOptions.OBJECT_TYPE}</objectStorgeOption>
			 </options>`;
  }
    
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

  // In 11g the seperator character appears to be \r rather than \n

  getPLSQL(dml) {
	const withOffset = dml.indexOf('\rWITH\r')
    return withOffset > -1 ? dml.substring(dml.indexOf('\rWITH\r')+5,dml.indexOf('\rselect')) : null
  }
    
  metadataToXML() {
            
    // I know.... Attmpting to build XML via string concatenation will end in tears...

    const metadataXML = Object.keys(this.metadata).map((tableName) => {
      const table = this.metadata[tableName]
      const columnsXML = table.columnNames.map((columnName) => {return `<columnName>${columnName}</columnName>`}).join('');
      const dataTypesXML = table.dataTypes.map((dataType) => {return `<dataType>${dataType}</dataType>`}).join('');
      const sizeConstraintsXML = table.sizeConstraints.map((sizeConstraint) => {
		switch (sizeConstraint.length) {
		  case 0:
			return `<sizeConstraint/>`
		  case 1:
		    return `<sizeConstraint><length>${sizeConstraint[0]}</length></sizeConstraint>`
		  case 2:
		    return `<sizeConstraint><length>${sizeConstraint[0]}</length><scale>${sizeConstraint[1]}</scale></sizeConstraint>`
		}
	  })
      return `<table><vendor>${table.vendor}</vendor><tableSchema>${table.tableSchema}</tableSchema><tableName>${table.tableName}</tableName><columnNames>${columnsXML}</columnNames><dataTypes>${dataTypesXML}</dataTypes><sizeConstraints>${sizeConstraintsXML}</sizeConstraints></table>`
    }).join('');
    return `<metadata>${metadataXML}</metadata>`
    
  }
  
  async getMetadata() {
    const metadataXML = this.metadataToXML();    
	return await this.dbi.stringToBlob(metadataXML);
  }

  getSourceTypeMappingsXML() {
     return `<typeMappings>${Array.from(this.TYPE_MAPPINGS.entries()).map((mapping) => { return `<typeMapping><vendorType>${mapping[0]}</vendorType><oracleType>${mapping[1]}</oracleType></typeMapping>` }).join('')}</typeMappings>`
  }
  
  async getSourceTypeMappings() {
	 return await this.dbi.stringToBlob(this.getSourceTypeMappingsXML())
  }
 
  generateCopyStatement(targetSchema,tableName,externalTableName,externalColumnNames,externalSelectList,plsql) {
	return `insert /*+ APPEND */ into "${targetSchema}"."${tableName}" (${externalColumnNames.join(",")})\nselect ${externalSelectList.join(",")} from ${externalTableName}`
  }

  generateCopyOperation(tableMetadata,tableInfo,externalColumnNames,externalColumnDefinitions,externalSelectList,copyColumnDefinitions) {
    super.generateCopyOperation(tableMetadata,tableInfo,externalColumnNames,externalColumnDefinitions,externalSelectList,copyColumnDefinitions) 
	const plsql = this.getPLSQL(tableInfo.dml)
	if (plsql) {
	  const functionList  = plsql.split('\rfunction')
	  functionList.shift()
	
	  const dropFunctions = functionList.map((functionDefinition,idx) => {
	    const functionName = functionDefinition.substring(2,functionDefinition.indexOf('"',2))
	    functionList[idx] = `create or replace function "${this.targetSchema}".${functionDefinition.substring(1)}`
	    return `drop function "${this.targetSchema}"."${functionName}"`
	  })

	  tableInfo.copy.createFunctions = functionList
	  tableInfo.copy.dropFunctions = dropFunctions
	}
  }
  
	
}

export { OracleStatementGenerator as default } 