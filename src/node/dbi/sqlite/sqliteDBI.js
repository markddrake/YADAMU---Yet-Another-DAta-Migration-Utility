import { performance } from 'perf_hooks';
import fs from 'fs';
import path from 'path';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

import YadamuConstants from '../../lib/yadamuConstants.js'
import YadamuLibrary from '../../lib/yadamuLibrary.js'

import YadamuDBI from '../base/yadamuDBI.js'
import DBIConstants from '../base/dbiConstants.js'

import SqliteDataTypes from './sqliteDataTypes.js'
import SqliteStatementLibrary from './sqliteStatementLibrary.js'
import SqliteConstants from './sqliteConstants.js'
import SqliteError from './sqliteException.js'
import SqliteCompare from './sqliteCompare.js'
import SqliteParser from './sqliteParser.js'
import SqliteStatementGenerator from './sqliteStatementGenerator.js'
import SqliteOutputManager from './sqliteOutputManager.js'
import SqliteWriter from './sqliteWriter.js'
import SqliteReader from './sqliteReader.js'

class SqliteDBI extends YadamuDBI {

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...SqliteConstants.DBI_PARAMETERS
    })
    return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return SqliteDBI.DBI_PARAMETERS
  }

  get DATABASE_KEY()           { return SqliteConstants.DATABASE_KEY };
  get DATABASE_VENDOR()        { return SqliteConstants.DATABASE_VENDOR };
  get SOFTWARE_VENDOR()        { return SqliteConstants.SOFTWARE_VENDOR };
  get STATEMENT_TERMINATOR()   { return SqliteConstants.STATEMENT_TERMINATOR };

  get SQL_TRUNCATE_TABLE_OPERATION()   { return 'DELETE FROM' }

  /*
  **
  ** SQLite has no spatial type and no spatial functions: the build ships R*Tree (a bounding box
  ** index, not a geometry type) and nothing else, and SpatiaLite is a loadable extension that may
  ** not be present. There is therefore no format SQLite can interpret, only one it can store
  ** faithfully, and GeoJSON is text so it survives a TEXT column unchanged.
  **
  ** Pinned rather than derived from the parameter, exactly as Postgres pins GeoJSON when PostGIS is
  ** absent (postgresDBI.js: POSTGIS_INSTALLED ? parameters.SPATIAL_FORMAT ... : 'GeoJSON'). Honouring
  ** SPATIAL_FORMAT=WKB here would have SQLite claim a format it cannot produce on the way back out.
  **
  */

  get SPATIAL_FORMAT()                  { return 'GeoJSON' }

  get SUPPORTED_STAGING_PLATFORMS()     { return [] }
  get SUPPORTED_STAGING_FORMATS()       { return [] }

  /*
  **
  ** SQLite has no schema hierarchy inside a database file, but it does have ATTACH: a file can be
  ** attached under a name and its tables then addressed as "<name>"."<table>", with its own
  ** sqlite_master. A YADAMU schema therefore maps to a FILE, and the schema name is the ATTACH name.
  **
  ** Two connection shapes are supported:
  **
  **   { "database"  : "path/to/file.db" }   single schema. Everything lives in main, table
  **                                         references are unqualified. The original behaviour.
  **
  **   { "directory" : "path/to/folder"  }   schema per file. <folder>/<schema>.db is attached as
  **                                         "<schema>" and table references are qualified.
  **
  ** The second form is what makes a multi schema operation possible. Without it every schema in a
  ** run lands in one file under unqualified names, so each schema overwrites the previous one, and
  ** a roundtrip that clones HR to HR1 destroys its own source.
  **
  ** ATTACH creates the file if it does not exist (except for a reader, see READ_ONLY), and one transaction spans attached databases, so
  ** the writer transaction discipline is unaffected. SQLite permits at most 10 attached databases.
  **
  */

  get SCHEMA_FOLDER()          { return this.CONNECTION_PROPERTIES.directory }
  get MULTI_SCHEMA()           { return this.SCHEMA_FOLDER !== undefined }
  get SCHEMA_NAME()            { return this.MULTI_SCHEMA ? this.CURRENT_SCHEMA : SqliteConstants.MAIN_SCHEMA }

  get DATABASE_FILE()          { return this.MULTI_SCHEMA ? SqliteConstants.IN_MEMORY_DATABASE : (this.CONNECTION_PROPERTIES.database || SqliteConstants.IN_MEMORY_DATABASE) }

  schemaFile(schema) {
    return path.join(this.SCHEMA_FOLDER,`${schema}${SqliteConstants.DATABASE_EXTENSION}`)
  }

  qualifiedTableName(tableName,schema) {
    return this.MULTI_SCHEMA ? `"${schema || this.SCHEMA_NAME}"."${tableName}"` : `"${tableName}"`
  }

  async attachSchema(schema) {
    const schemaFile = this.schemaFile(schema)
    if (this.READ_ONLY) {
      this.assertDatabaseExists(schemaFile)
    }
    await this.connection.run(`ATTACH DATABASE '${schemaFile.split("'").join("''")}' AS "${schema}"`)
    if (this.IS_WRITER) {
      await this.connection.run(`PRAGMA "${schema}".journal_mode = WAL`)
    }
  }

  get IN_MEMORY_DATABASE()     { return this.DATABASE_FILE === SqliteConstants.IN_MEMORY_DATABASE }

  /*
  **
  ** A reader must leave the database exactly as it found it. Opening it read-write is not enough to
  ** break that, but asking for WAL is: journal_mode=WAL is persistent, so it rewrites the file header
  ** and creates <file>-wal and <file>-shm beside it. A source on read-only storage then fails with
  ** SQLITE_READONLY, and a writable one has its checksum changed by being read.
  **
  ** So a reader opens the database with SQLITE_OPEN_READONLY, and only a writer asks for WAL. ATTACH takes
  ** its flags from the main database, so an attached schema file is read-only too, and neither
  ** creates a file that does not exist. A missing source is an error, not an empty database.
  **
  ** Only a reader is read-only, and only a writer asks for WAL. A DBI with neither or both of
  ** FROM_USER and TO_USER (compare, for instance) opens read-write but leaves the journal mode as it
  ** finds it, which for a database YADAMU wrote is already WAL, since journal_mode is persistent.
  **
  ** A worker's connection is opened by the manager (setWorkerConnection, _reconnect), but configured
  ** by the worker (configureConnection), so a worker takes READ_ONLY from its manager rather than
  ** working it out again. A worker of a reader can never be a writer - it inherits FROM_USER - so it
  ** never asks a read-only connection for WAL, which would itself be SQLITE_READONLY.
  **
  */

  get READ_ONLY()              { return this.isManager() ? this.IS_READER : this.manager.READ_ONLY }

  assertDatabaseExists(databaseFile) {
    if (!fs.existsSync(databaseFile)) {
      throw new Error(`SQLite database "${path.resolve(databaseFile)}" not found. A ${this.DATABASE_VENDOR} source is opened read-only and is never created.`)
    }
  }
  get BUSY_TIMEOUT()           { return this.parameters.BUSY_TIMEOUT || SqliteConstants.BUSY_TIMEOUT }

  
  /*
  **
  ** SQLite allows one write transaction per database file, so parallel writers serialize on the
  ** write lock. On its own that would only make them queue. What makes it fail is the threadpool:
  ** node-sqlite3 runs every statement on the libuv threadpool, and a writer that finds the lock
  ** held blocks inside sqlite3_step() for the whole busy timeout, holding its thread while it
  ** waits. With W writers, W-1 can be blocked at once, and the pool must also have a thread free
  ** for the writer holding the lock so that it can run the COMMIT that releases it. Once blocked
  ** writers fill the pool nothing can commit, every writer waits out the full busy timeout, and
  ** the operation collapses. That is starvation, not contention: raising the pool fixes it.
  **
  ** Measured, postgres -> sqlite, HR (7 tables), busy_timeout 30000:
  **
  **    7 workers, pool  4    52 SQLITE_BUSY, 48 errors, 6 tables failed   00:06:34
  **    7 workers, pool  7     0 SQLITE_BUSY,  0 errors                    00:00:03.5
  **    7 workers, pool  8     0 SQLITE_BUSY,  0 errors                    00:00:03.5
  **    7 workers, pool 16     0 SQLITE_BUSY,  0 errors                    00:00:03.4
  **    3 workers, pool  4     0 SQLITE_BUSY,  0 errors                    00:00:03.4
  **
  ** So W <= pool is the boundary, and initialize() caps PARALLEL to fit rather than refusing
  ** parallel writes outright. See MAX_PARALLEL_WRITERS.
  **
  */

  get PARALLEL_WRITE_OPERATIONS()       { return true }

  constructor(yadamu, manager, connectionSettings, parameters) {
    super(yadamu, manager, connectionSettings, parameters)
    this.DATABASE_ERROR_CLASS = SqliteError
    this.DATA_TYPES = SqliteDataTypes
    this.COMPARITOR_CLASS = SqliteCompare
    this.PARSER_CLASS = SqliteParser
    this.STATEMENT_GENERATOR_CLASS = SqliteStatementGenerator
    this.STATEMENT_LIBRARY_CLASS = SqliteStatementLibrary
    this.OUTPUT_MANAGER_CLASS = SqliteOutputManager
    this.WRITER_CLASS = SqliteWriter
  }

  /*
  **
  ** YadamuDBI.CONNECTION_PROPERTIES caches whatever addVendorExtensions() returns. The base
  ** implementation returns undefined, so a driver that does not override it caches undefined
  ** and every consumer fails - redactPasswords() throws in structuredClone(), and
  ** createConnectionPool() cannot read the database file name. SQLite has no vendor specific
  ** connection properties beyond the database file, so return the properties unchanged.
  **
  */
  addVendorExtensions(connectionProperties) {
    return connectionProperties
  }

  /*
  **
  ** UV_THREADPOOL_SIZE is read by libuv the first time the pool is used and cannot be changed from
  ** inside the process, so raising this ceiling means setting it in the environment before node
  ** starts. One thread is held back below the measured boundary because the pool is shared with
  ** everything else libuv schedules on it - file system operations in particular, which is exactly
  ** what YADAMU starts doing when it writes exception and rejection traces.
  **
  */

  get THREADPOOL_SIZE()        { return parseInt(process.env.UV_THREADPOOL_SIZE) || SqliteConstants.DEFAULT_THREADPOOL_SIZE }

  get MAX_PARALLEL_WRITERS()   { return Math.max(1,this.THREADPOOL_SIZE - 1) }

  async initialize(requirePassword) {

    await super.initialize(requirePassword)

    // Only the target serializes on the write lock: readers open no transactions and never block on it.

    if (this.IS_WRITER) {
      const requested = parseInt(this.yadamu.PARALLEL) || 0
      if (requested > this.MAX_PARALLEL_WRITERS) {
        this.LOGGER.warning([this.DATABASE_VENDOR,'PARALLEL'],`Reduced from ${requested} to ${this.MAX_PARALLEL_WRITERS}. A SQLite writer blocked on the write lock holds a libuv threadpool thread, and UV_THREADPOOL_SIZE is ${this.THREADPOOL_SIZE}. Set UV_THREADPOOL_SIZE in the environment before starting node to raise the limit.`)
        this.yadamu.parameters.PARALLEL = this.MAX_PARALLEL_WRITERS
      }
    }

  }

  async testConnection() {
    try {
      await this.connection.get('SELECT 1')
      return true
    } catch (e) {
      throw this.getDatabaseException(e, new Error().stack, 'SELECT 1')
    }
  }
  
  /*
  **
  ** SQLite has no connection pool because there is nothing to pool. A connection is a handle on a
  ** database file, not a session on a server, so opening one is a local open() rather than a network
  ** handshake and an authentication round trip. What SQLite does have is any number of concurrent
  ** connections to the same database file.
  **
  ** That distinction matters because a transaction belongs to a connection: BEGIN, COMMIT and
  ** SAVEPOINT apply to the whole connection, not to the statement that issued them. Handing every
  ** parallel table pipeline the same connection therefore puts all of their transactions on top of
  ** one another - workers nest their BEGIN statements ("cannot start a transaction within a
  ** transaction"), and one worker's COMMIT commits the rows another worker is still writing.
  **
  ** So each worker takes its own connection, exactly as the Postgres and MySQL drivers hand each
  ** worker its own pooled connection. There is nothing for createConnectionPool() to create - the
  ** database file is the pool - and getConnectionFromPool() opens a connection on demand. The base
  ** class calls configureConnection() for each one, so every connection gets its own journal mode
  ** (a writer's), foreign key enforcement and busy timeout, all of which are per connection settings.
  **
  */

  async createConnectionPool() { /* NO-OP: the database file is the pool */ }

  async getConnectionFromPool() {
    if (this.READ_ONLY && !this.IN_MEMORY_DATABASE) {
      this.assertDatabaseExists(this.DATABASE_FILE)
    }
    return await open({
      filename : this.DATABASE_FILE
    , driver   : sqlite3.Database
      // undefined is the driver default: OPEN_READWRITE | OPEN_CREATE | OPEN_FULLMUTEX
    , mode     : this.READ_ONLY ? sqlite3.OPEN_READONLY | sqlite3.OPEN_FULLMUTEX : undefined
    })
  }

  async getConnection() {
    return this.connection
  }

  async configureConnection() {

    await this.connection.run('PRAGMA foreign_keys = ON')

    /*
    **
    ** WAL is a property of the database file. An in-memory database has no journal file, and
    ** reports journal_mode 'memory' regardless of what is requested, so do not ask for it. Only a
    ** writer asks for it: from a reader it would rewrite the source (see READ_ONLY).
    **
    */

    if (!this.IN_MEMORY_DATABASE && this.IS_WRITER) {
      await this.connection.run('PRAGMA journal_mode = WAL')
    }

    /*
    **
    ** SQLite allows one write transaction per database file. Without a busy timeout a connection
    ** that finds the write lock held fails immediately with SQLITE_BUSY: with one, it waits for
    ** the current writer to commit. Required as soon as anything else - another YADAMU worker,
    ** another process, a session inspecting the file - writes to the same database.
    **
    */

    await this.connection.run(`PRAGMA busy_timeout = ${this.BUSY_TIMEOUT}`)

    /*
    **
    ** ATTACH is a property of the connection, not of the database, so every connection - the
    ** manager's and each worker's - has to attach the schema it works with.
    **
    */

    if (this.MULTI_SCHEMA) {
      await this.attachSchema(this.SCHEMA_NAME)
    }

  }

  async closeConnection(options) {
    if (this.connection) {
      await this.connection.close()
      this.connection = null
    }
  }
  
  async closePool(options) { /* NO-OP: there is no pool. Connections are closed by closeConnection() */ }

  /*
  **
  ** run() executes a statement but discards any rows it produces, so anything that returns a
  ** result set has to go through all(). As well as SELECT that covers common table expressions,
  ** VALUES, EXPLAIN, and the PRAGMA statements that report a value ("PRAGMA journal_mode = WAL"
  ** returns the resulting journal mode, "PRAGMA foreign_keys = ON" returns nothing). Leading
  ** whitespace and comments are skipped before the statement is classified.
  **
  */

  static #RESULT_SET_STATEMENT = /^(?:\s|--[^\n]*|\/\*[\s\S]*?\*\/)*(SELECT|WITH|VALUES|EXPLAIN|PRAGMA)\b/i

  returnsResultSet(sqlStatement) {
    return (typeof sqlStatement === 'string') && SqliteDBI.#RESULT_SET_STATEMENT.test(sqlStatement)
  }

  async executeSQL(sqlStatement, args) {
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    if (typeof sqlStatement === 'string') {
      this.SQL_TRACE.traceSQL(sqlStatement)
    }

    while (true) {
      let stack
      try {
        stack = new Error().stack
        const sqlStartTime = performance.now()
        let results
        
        if (this.returnsResultSet(sqlStatement)) {
          results = await this.connection.all(sqlStatement, args || [])
        } else {
          results = await this.connection.run(sqlStatement, args || [])
        }
        
        this.SQL_TRACE.traceTiming(sqlStartTime, performance.now())
        return results;
      } catch (e) {
        const cause = this.getDatabaseException(e, stack, sqlStatement)
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
          await this.reconnect(cause, 'SQL')
          continue;
        }
        throw cause
      }      
    } 
  }

  async beginTransaction() {
    await this.executeSQL(SqliteStatementLibrary.SQL_BEGIN_TRANSACTION)
    super.beginTransaction()
  }

  async commitTransaction() {
    super.commitTransaction()
    await this.executeSQL(SqliteStatementLibrary.SQL_COMMIT_TRANSACTION)
  }

  async rollbackTransaction(cause) {
    this.checkConnectionState(cause)
    try {
      super.rollbackTransaction()
      await this.executeSQL(SqliteStatementLibrary.SQL_ROLLBACK_TRANSACTION)
    } catch (newIssue) {
      this.checkCause('ROLLBACK TRANSACTION', cause, newIssue)
    }
  }

  async createSavePoint() {
    await this.executeSQL(SqliteStatementLibrary.SQL_CREATE_SAVE_POINT)
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {
    this.checkConnectionState(cause)
    let stack
    try {
      await this.executeSQL(SqliteStatementLibrary.SQL_RESTORE_SAVE_POINT)
      super.restoreSavePoint()
    } catch (newIssue) {
      this.checkCause('RESTORE SAVEPOINT', cause, newIssue)
    }
  }

  async releaseSavePoint(cause) {
    await this.executeSQL(SqliteStatementLibrary.SQL_RELEASE_SAVE_POINT)
    super.releaseSavePoint()
  }

  async getSystemInformation() {
    const sysInfo = await this.executeSQL(SqliteStatementLibrary.SQL_SELECT_SYSTEM_INFORMATION)
    return Object.assign(
      super.getSystemInformation()
    , sysInfo[0] || {}
    )
  }

  async getDDLOperations() {
    return []
  }
  
  generateTableInfo() {
    const queryInfo = Object.keys(this.metadata).map((value) => {
      return { TABLE_NAME: value, SQL_STATEMENT: this.metadata[value].sqlStatement }
    })
    return queryInfo;
  }
  
  /*
  **
  ** The sqlite3 binding returns every INTEGER column as a JavaScript number, so a 64 bit integer
  ** beyond Number.MAX_SAFE_INTEGER (9007199254740991) is rounded on the way out even though SQLite
  ** stored it exactly: 9223372036854775807 is returned as 9223372036854776000, and the target then
  ** rejects it as out of range for its own 64 bit type. The value is not lost - CAST(col AS TEXT)
  ** evaluated inside the engine returns the exact digits - so 64 bit integer columns are selected
  ** as text and the exact value is handed to the target to bind. The cast is aliased back to the
  ** column name because SqliteReader maps result columns by name.
  **
  ** The cast is unconditional rather than applied only to values that overflow a double, because
  ** SQL Server requires it: mssqlDBI binds every bigint parameter as sql.VarChar(20) and casts in
  ** the DML ("Bind as VarChar to avoid rounding issues"), for exactly the reason this cast exists.
  ** Handing it a number raises "Validation failed for parameter 'C0'. Invalid string." on every
  ** WorldWideImporters fact table. Postgres and Oracle accept the numeric string, so text is the
  ** representation that satisfies all three.
  **
  ** This is the generateSelectListEntry() hook the base class calls per column when it builds
  ** CLIENT_SELECT_LIST (see buildSchemaInfo), the same hook Vertica and Redshift use to cast on
  ** the way out. SQL_SCHEMA_METADATA returns one row per table rather than one row per column, so
  ** getSchemaMetadata calls the hook itself with the [schema,table,column,dataType] columnInfo the
  ** base class would have passed.
  **
  ** Narrower declarations (INTEGER(16), INTEGER(32)) cannot overflow a double and are left alone.
  ** A bare INTEGER is included because that is how a database YADAMU did not create declares a
  ** 64 bit column.
  **
  */

  /*
  **
  ** The size constraint carried on a SQLite INTEGER comes from the source and is not in consistent
  ** units: Postgres int8 arrives as INTEGER(64) (bits) and SQL Server bigint as INTEGER(19)
  ** (decimal digits). Both denote a 64 bit integer, so both are listed, along with undefined for a
  ** database YADAMU did not create. INTEGER(10), (16) and (32) cannot overflow a double and are
  ** left alone. The units are ambiguous enough that a numeric threshold would misclassify
  ** INTEGER(32), so the markers are enumerated rather than compared.
  **
  */

  static #SIXTY_FOUR_BIT = [undefined,19,64]

  generateSelectListEntry(columnInfo) {
    const dataType = SqliteDataTypes.decomposeDataType(columnInfo[3])
    switch (dataType.type.toUpperCase()) {
      case 'INTEGER':
      case 'BIGINT':
      case 'INT8':
        return SqliteDBI.#SIXTY_FOUR_BIT.includes(dataType.length)
             ? `CAST("${columnInfo[2]}" AS TEXT) "${columnInfo[2]}"`
             : `"${columnInfo[2]}"`
      default:
        return `"${columnInfo[2]}"`
    }
  }

  /*
  **
  ** SQLite rows from SQL_SCHEMA_METADATA are keyed objects (TABLE_NAME, COLUMN_NAME_ARRAY, ...)
  ** rather than the positional arrays base generateSchemaInfo expects ([0]=TABLE_SCHEMA ...).
  ** Map them into the base schemaInformation contract here, then let base generateSchemaInfo run.
  ** SQLite tables are never schema qualified (the database file is the connection), so the
  ** schema position is always 'main'.
  **
  */
  async getSchemaMetadata() {
    const results = await this.executeSQL(SqliteStatementLibrary.SQL_SCHEMA_METADATA(this.SCHEMA_NAME))
    const schemaInformation = results.map((row) => {
      const parseArray = (value) => JSON.parse(value)
      const columnNames = parseArray(row.COLUMN_NAME_ARRAY)

      /*
      **
      ** pragma_table_info reports the DECLARED type, size and all - INTEGER(64), VARCHAR(32). The
      ** statement generator needs those split apart: generateStorageClause() indexes
      ** SIZE_CONSTRAINT_ARRAY per column and reads .length on the entry, so a single '[]' for the
      ** whole table leaves every entry undefined. That is unreachable while SQLite is only ever a
      ** target, because the size constraints then come from the source; it fails the moment SQLite
      ** becomes the source.
      **
      ** The split mirrors what the base buildSchemaInfo() does for drivers whose metadata query
      ** returns one row per column. The RAW declared type is kept for generateSelectListEntry,
      ** which needs the length to identify a 64 bit integer.
      **
      */

      const declaredTypes = parseArray(row.DATA_TYPE_ARRAY)
      const decomposed = declaredTypes.map((declaredType) => SqliteDataTypes.decomposeDataType(declaredType))
      const dataTypes = decomposed.map((dataType) => dataType.typeQualifier ? `${dataType.type} ${dataType.typeQualifier}` : dataType.type)
      const sizeConstraints = decomposed.map((dataType) => dataType.length ? (dataType.scale ? [dataType.length,dataType.scale] : [dataType.length]) : [])

      const selectList = columnNames.map((columnName,idx) => {
        return this.generateSelectListEntry([this.SCHEMA_NAME,row.TABLE_NAME,columnName,declaredTypes[idx]])
      }).join(',')
      return [
        this.SCHEMA_NAME                    // TABLE_SCHEMA
      , row.TABLE_NAME                     // TABLE_NAME
      , columnNames                        // COLUMN_NAME_ARRAY
      , dataTypes                          // DATA_TYPE_ARRAY
      , sizeConstraints                    // SIZE_CONSTRAINT_ARRAY
      , selectList                         // CLIENT_SELECT_LIST
      ]
    })
    return this.generateSchemaInfo(schemaInformation)
  }
   
  /*
  **
  ** SQLite table references cannot be schema qualified. Override generateSQLQuery to
  ** emit an unqualified table reference. See SqliteStatementGenerator header notes.
  **
  */
  generateSQLQuery(tableMetadata) {
    const queryInfo = {...tableMetadata}
    queryInfo.SQL_STATEMENT = `select ${tableMetadata.CLIENT_SELECT_LIST} from ${this.qualifiedTableName(tableMetadata.TABLE_NAME,tableMetadata.TABLE_SCHEMA)} t`
    queryInfo.MAPPED_TABLE_NAME = this.getMappedTableName(queryInfo.TABLE_NAME,this.IDENTIFIER_MAPPINGS) || queryInfo.TABLE_NAME
    return queryInfo;
  }

  async _getInputStream(queryInfo) {
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
        stack = new Error().stack
        const sqlStartTime = performance.now()
        const statement = await this.connection.prepare(queryInfo.SQL_STATEMENT)
        this.SQL_TRACE.traceTiming(sqlStartTime, performance.now())
        return new SqliteReader(statement,queryInfo.COLUMN_NAME_ARRAY)
      } catch (e) {
        const cause = this.getDatabaseException(e, stack, queryInfo.SQL_STATEMENT)
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
          await this.reconnect(cause, 'SQL')
          continue;
        }
        throw cause
      }      
    }
  }

  /*
  **
  ** SQLite has no schema heirarchy and no CREATE SCHEMA statement: the database file is the
  ** namespace, which is why table references are never schema qualified (see generateSQLQuery)
  ** and getSchemaMetadata reports every table as 'main'. Additional namespaces are separate
  ** database files introduced with ATTACH. The schema therefore exists as soon as the database
  ** file is opened and there is nothing to create. SqliteQA.recreateSchema() implements the QA
  ** contract by dropping the user tables.
  **
  */

  /*
  **
  ** SQLite has no TRUNCATE TABLE. A DELETE with no WHERE clause is the equivalent, and SQLite
  ** applies its truncate optimisation to exactly that shape - dropping the pages rather than
  ** walking the rows - when the table has no triggers and no foreign key actions to run.
  **
  ** The base builds "<schema>"."<table>" unconditionally, which is only right for a vendor that
  ** always qualifies. A single schema SQLite connection holds its tables in main and addresses
  ** them unqualified, so qualifiedTableName() is used to suit whichever connection shape is in
  ** play - a bare name, or "<schema>"."<table>" when a schema has been attached.
  **
  ** Reachable through TRUNCATE_ON_LOAD, and through the retry that DBReader performs after a
  ** failed pipeline, which is where it first surfaced as SQLITE_ERROR: near "TRUNCATE".
  **
  */

  async truncateTable(schema,tableName) {

    const activeTransaction = this.TRANSACTION_IN_PROGRESS
    if (this.TRANSACTION_IN_PROGRESS) {
      await this.rollbackTransaction()
    }

    const sqlStatement = `${this.SQL_TRUNCATE_TABLE_OPERATION} ${this.qualifiedTableName(tableName,schema)}`

    await this.beginTransaction()
    await this.executeSQL(sqlStatement)
    await this.commitTransaction()

    if (activeTransaction) {
      await this.beginTransaction()
    }

  }

  async createSchema(schema) {
    // ATTACH creates the file if it is missing, so attaching IS creating the schema.
    if (this.MULTI_SCHEMA && (schema !== this.SCHEMA_NAME)) {
      await this.attachSchema(schema)
    }
  }
  
  classFactory(yadamu) {
    return new SqliteDBI(yadamu, this, this.CONNECTION_SETTINGS, this.parameters)
  }
  
  async getConnectionID() {
    return `sqlite-${Date.now()}`
  }

}

export { SqliteDBI as default }