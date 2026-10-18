class SqliteStatementLibrary {

  static get SQL_BEGIN_TRANSACTION()     { return 'BEGIN TRANSACTION' }
  static get SQL_COMMIT_TRANSACTION()    { return 'COMMIT' }
  static get SQL_ROLLBACK_TRANSACTION()  { return 'ROLLBACK' }
  static get SQL_CREATE_SAVE_POINT()     { return 'SAVEPOINT YADAMU_SAVEPOINT' }
  static get SQL_RESTORE_SAVE_POINT()    { return 'ROLLBACK TO SAVEPOINT YADAMU_SAVEPOINT' }
  static get SQL_RELEASE_SAVE_POINT()    { return 'RELEASE SAVEPOINT YADAMU_SAVEPOINT' }
  
  static get SQL_SELECT_SYSTEM_INFORMATION() {
    return `SELECT 
              sqlite_version() as DATABASE_VERSION
            , 'SQLite' as DATABASE_VENDOR
            , 'SQLite Consortium' as SOFTWARE_VENDOR
            , (SELECT page_size * page_count FROM pragma_page_count(), pragma_page_size()) as DATABASE_SIZE
            , (SELECT group_concat(name) FROM pragma_database_list()) as DATABASE_NAME`
  }

  static SQL_LIST_USER_TABLES(schema) {
    return `SELECT 
              name as TABLE_NAME
            FROM "${schema}".sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'`
  }

  /*
  **
  ** Both statements take the schema (the ATTACH name) so that they read the right attached
  ** database. Each attached database has its own sqlite_master, and pragma_table_info() takes the
  ** schema as its second argument. In single schema mode the caller passes 'main'.
  **
  */

  static SQL_SCHEMA_METADATA(schema) {
    return `SELECT 
              m.name as TABLE_NAME
            , json_group_array(p.name) as COLUMN_NAME_ARRAY
            , json_group_array(p.type) as DATA_TYPE_ARRAY
            , '[]' as SIZE_CONSTRAINT_ARRAY
            , m.sql as SQL_STATEMENT
            FROM "${schema}".sqlite_master m
            JOIN pragma_table_info(m.name,'${schema}') p
            WHERE m.type = 'table'
            AND m.name NOT LIKE 'sqlite_%'
            GROUP BY m.name`
  }

  static get SQL_GENERATE_STATEMENTS() {
    return `SELECT 
              name as TABLE_NAME
            , 'SELECT ' || group_concat(name) || ' FROM ' || name as SQL_STATEMENT
            FROM pragma_table_info(?)
            GROUP BY name`
  }
}

export { SqliteStatementLibrary as default }
