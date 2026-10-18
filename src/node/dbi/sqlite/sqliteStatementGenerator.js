import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

/*
**
** SQLite is a file based database. It does not support the concept of a 'Schema'.
**
** Tables are located in the database's main (or temp) file, and can be qualified with the schema name 'main'. 
** However the database file itself is specified as part of the connection properties, so all table references 
** must be unqualified.
**
*/

class SqliteStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
    return `create table ${this.dbi.qualifiedTableName(tableName,schema)}(\n  ${columnDefinitions.join(',')})`;
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into ${this.dbi.qualifiedTableName(tableName,schema)} ("${columnNames.join('","')}") values (${insertOperators.join(',')})`;
  }

}

export { SqliteStatementGenerator as default }