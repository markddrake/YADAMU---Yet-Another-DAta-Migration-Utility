
import _MsSQLStatementGenerator from '../mssqlStatementGenerator.js';

class MsSQLStatementGenerator extends _MsSQLStatementGenerator {
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

 getMetadata() {
            
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
		    return `<sizeConstraint><precision>${sizeConstraint[0]}</precision></sizeConstraint>`
		  case 2:
		    return `<sizeConstraint><precision>${sizeConstraint[0]}</precision><scale>${sizeConstraint[1]}</scale></sizeConstraint>`
		}
	  }).join('');
      return `<table><vendor>${table.vendor}</vendor><tableSchema>${table.tableSchema}</tableSchema><tableName>${table.tableName}</tableName><columnNames>${columnsXML}</columnNames><dataTypes>${dataTypesXML}</dataTypes><sizeConstraints>${sizeConstraintsXML}</sizeConstraints></table>`
    }).join('');
    return `<metadata>${metadataXML}</metadata>`
    
  }

  getSourceTypeMappings() {
	 const mappings = `<typeMappings>${Array.from(this.TYPE_MAPPINGS.entries()).map((mapping) => { return `<typeMapping><vendorType>${mapping[0]}</vendorType><mssqlType>${mapping[1]}</mssqlType></typeMapping>` }).join('')}</typeMappings>`
     return mappings
  }
  
}
 
export { MsSQLStatementGenerator as default }
