# YADAMU DBI API REFERENCE (machine-oriented)

> REVISION NOTE. Sections 4, 5.1, 5.2, 8 and 9 were corrected after the
> SQLite driver was completed; several statements in the original draft
> described intentions rather than the code. Line references throughout are
> indicative only - the SQLite driver has been substantially rewritten since
> they were taken. For the contracts a new driver must satisfy, see
> `docs/Driver-Contracts.md`, which is the authoritative companion to this
> file.

Version: documents the code as verified in `src/node/` on 2026-09-04.
Audience: LLM agents and automated tooling. Every symbol below was read from
source; file paths are exact. Sections are normative — treat them as contracts.

---

## 1. VOCABULARY (use these terms exactly)

- **DBI** — Database Interface. A JS class that mediates ALL SQL execution
  against one database. Two layers: abstract base (`YadamuDBI`), concrete
  vendor DBI (e.g. `sqliteDBI`).
- **Application** — the orchestrator that owns DBI instances for source and
  target and drives the COPY/IMPORT/Upload/Load action.
- **Statement** — one SQL fragment. Ends with `STATEMENT_TERMINATOR`
  (`';'`). Statements in a stream are joined by `STATEMENT_SEPERATOR`
  (`'\n--\n'`).
- **Batch** — a contiguous slice of rows from one table's InputStream,
  bounded by `BATCH_SIZE`.
- **Stream** — the ordered row payload delivered to a Writer.
- **Savepoint** — a named transaction mark; see §5 SQL_RELEASE_SAVEPOINT.
- **Transformer** — a per-column value rewrite applied by an OutputManager.
- **Trace** — a structured error record under `exceptions/`.

---

## 2. PACKAGE LAYOUT (all paths relative to repo root)

| Path | Role |
|---|---|
| `src/node/dbi/base/yadamuDBI.js` | Abstract DBI base class |
| `src/node/dbi/base/dbiConstants.js` | `DBIConstants` defaults |
| `src/node/dbi/base/yadamuStatementGenerator.js` | Statement cache builder |
| `src/node/dbi/base/yadamuOutputManager.js` | OutputManager base |
| `src/node/dbi/base/yadamuDataTypes.js` | Vendor-neutral type schema |
| `src/node/dbi/sqlite/sqliteDBI.js` | SQLite DBI concrete class |
| `src/node/dbi/sqlite/sqliteWriter.js` | SQLite Writer (batch contract) |
| `src/node/dbi/sqlite/sqliteOutputManager.js` | SQLite transformers |
| `src/node/dbi/sqlite/sqliteStatementGenerator.js` | SQLite statement gen shim |
| `src/node/dbi/sqlite/sqliteStatementLibrary.js` | SQLite SQL statement shapes |
| `src/node/dbi/sqlite/sqliteDataTypes.js` | SQLite type mappings |
| `src/node/dbi/sqlite/sqliteConstants.js` | SQLite constants |
| `src/node/dbi/sqlite/node_modules/sqlite/build/Database.js` | SQLite `Database` driver class |
| `src/node/core/dbReader.js` | Reader core |
| `src/node/core/dbReaderParallel.js` | Parallel reader core |
| `src/node/core/dbWriter.js` | Writer core |
| `src/node/cli/copy.js` | COPY CLI entry |
| `bin/copy.sh` | Shell wrapper: runs CLI against a config JSON |
| `qa/local/sqliteCopy.json` | Canonical local COPY run config |
| `qa/regression/*.json` | Regression run configs |
| `exceptions/exception_<ISO8601>.trace` | Error traces |

Run pattern (verified in `bin/copy.sh`):

```bash
bash bin/copy.sh <path-to-config.json>
```

---

## 3. `YadamuDBI` — abstract base class

`class YadamuDBI extends EventEmitter` (`yadamuDBI.js:112`)

### 3.1 Identity getters (fixed values)

| Getter | Value |
|---|---|
| `DATABASE_KEY` | `'yadamu'` |
| `DATABASE_VENDOR` | `'YADAMU'` |
| `SOFTWARE_VENDOR` | `'YABASC - Yet Another Bay Area Software Company'` |
| `DATATYPE_IDENTITY_MAPPING` | `true` |
| `SQL_COPY_OPERATIONS` | `false` |
| `PASSWORD_KEY_NAME` | `'password'` |
| `STATEMENT_TERMINATOR` | `';'` |
| `STATEMENT_SEPERATOR` | `'\n--\n'` |
| `RETRY_COUNT` | `3` |
| `SQL_TRUNCATE_TABLE_OPERATION` | `'TRUNCATE TABLE'` |

### 3.2 Capability getters

- `PARALLEL_READ_OPERATIONS` → `true`
- `PARALLEL_WRITE_OPERATIONS` → `true`
- `PARALLEL_OPERATIONS` → `PARALLEL_READ_OPERATIONS && PARALLEL_WRITE_OPERATIONS`

`PARALLEL_ENABLED` (on the Yadamu object) is
`source.PARALLEL_OPERATIONS && target.PARALLEL_OPERATIONS`. Because
`PARALLEL_OPERATIONS` is READ *and* WRITE, a vendor that declares writes
sequential also loses parallel reads, in both directions.

`dbReader.pipe()` clamps `yadamu.parameters.PARALLEL = 0` when the target
reports `PARALLEL_WRITE_OPERATIONS === false`. `Yadamu.PARALLEL` itself is a
getter only; assigning to it throws in an ES module.

### 3.3 Parameter-overridable getters

Resolution order is ALWAYS: `this.parameters.<NAME>` → `DBIConstants.<NAME>`
(or `DATA_TYPES` for type-shape fields).

`TABLE_MAX_ERRORS`, `TOTAL_MAX_ERRORS`, `MODE`, `ON_ERROR`,
`INFINITY_MANAGEMENT`, `STAGING_FILE_RETENTION`, `BYTE_TO_CHAR_RATIO`,
`RESET_IDENTITY`, `PARTITION_LEVEL_OPERATIONS`, `COMMIT_RATIO`, `BATCH_LIMIT`.

Type-shape fields (fall back to `this.DATA_TYPES` instead of DBIConstants):

- `SPATIAL_FORMAT` ← `parameters.SPATIAL_FORMAT || DATA_TYPES?.storageOptions.SPATIAL_FORMAT`
- `CIRCLE_FORMAT` ← `parameters.CIRCLE_FORMAT || DATA_TYPES?.storageOptions.CIRCLE_FORMAT`
- `TIMESTAMP_PRECISION` ← `parameters.TIMESTAMP_PRECISION || DATA_TYPES?.TIMESTAMP_PRECISION`

Staging areas are macro-expanded via
`YadamuLibrary.macroSubstitions(...)`:

- `LOCAL_STAGING_AREA`
- `REMOTE_STAGING_AREA`

### 3.4 Stateful getters/setters

- `DESTROYED` (get/set)
- `DRIVER_ID` (get/set)
- `IDENTIFIER_MAPPINGS` (get/set) — set by `generateDatabaseMappings` merge
- `RECONNECT_ON_ABORT` — GET-AND-CLEAR: read resets to `false`
- `SQL_TRACE` — lazy memoized: `set SQL_TRACE(writer)` installs an
  `SQLTrace` writer configured with the DBI's separator/terminator/role
- `SQL_CUMLATIVE_TIME` → delegates to `SQL_TRACE.SQL_CUMLATIVE_TIME`
- `BATCH_SIZE` — lazily computed from `BATCH_LIMIT`/`COMMIT_RATIO`
- `DATA_TYPES` — the vendor type schema object (set by concrete DBI)
- `IDENTIFIER_TRANSFORMATION` ← `this.yadamu.IDENTIFIER_TRANSFORMATION`

### 3.5 Core methods

| Method | Location (yadamuDBI.js) | Purpose |
|---|---|---|
| `configure(...)` | — | install parameters/credentials |
| `generateSchemaInfo(schemaInfo)` | :1717 | build schema info object |
| `getDDLOperations()` | :1712 (sqlite override `sqliteDBI.js:183`) | enumerate DDL statements |
| `generateDatabaseMappings(...)` | ~:995 | build table/column identifier mappings |
| `mergeMappings(...)` | :1017-1019 | merge generated + user mappings; `this.IDENTIFIER_MAPPINGS = mergeMappings(generatedMappings, databaseMappings)` |
| `applyIdentifierMappings(metadata, mappings, reportMappedIdentifiers)` | :1070 | apply rename table; see §3.6 invariants |
| `getMappedTableName(tableName, IDENTIFIER_MAPPINGS)` | :1913 | single-table lookup |
| `generateSQLQuery(...)` | ~:1900-1990 | build SELECT/INSERT statements; sets `queryInfo.MAPPED_TABLE_NAME = getMappedTableName(queryInfo.TABLE_NAME, IDENTIFIER_MAPPINGS) || queryInfo.TABLE_NAME` (:1941) |
| `workerDBI(workerNumber)` | :2087 | clone DBI identity for a parallel worker; propagates `IDENTIFIER_MAPPINGS` (:2084) |
| `main()` / `run()` | — | execute the operation |

### 3.6 IDENTIFIER_MAPPings invariants (normative)

From `generateDatabaseMappings`/`applyIdentifierMappings` (verified :995-1070):

1. **User-specified mappings in an IDENTIFIER_MAPPings file are sacrosanct**
   (source comment :1004). They are never overwritten by generated mappings.
2. `applyIdentifierMappings` **never renames object keys**. It only mutates
   the `tableName` property value of each mapped `metadata[table]` entry.
3. Column remap: for each `tableMappings.columnMappings`, find
   `idx = columnNames.indexOf(columnName)`; if `idx > -1`, set
   `columnNames[idx] = mappedColumnName` (where
   `mappedColumnName = tableMappings.columnMappings[columnName].name ||
   columnNames[idx]`).
4. Every applied remap is reported via `this.LOGGER.info(
   [DATABASE_VENDOR, 'IDENTIFIER MAPPING', <table|table.col>], <message>)`
   when `reportMappedIdentifiers` is truthy.

---

## 4. Concrete SQLite DBI — `sqliteDBI`

File: `src/node/dbi/sqlite/sqliteDBI.js`

Verified facts:

- `this.DATA_TYPES = SqliteDataTypes` — the SQLite data-type schema.
- `getDDLOperations()` returns `[]`. SQLite DDL is emitted by the statement
  generator, not enumerated here.
- `generateSQLQuery` emits an UNQUALIFIED table reference:
  `select <CLIENT_SELECT_LIST> from "<TABLE_NAME>" t`. SQLite has no schema
  hierarchy, so table references are never schema qualified and
  `getSchemaMetadata` reports every table as `main`.
- `createSchema()` is a NO-OP. SQLite has no `CREATE SCHEMA`; additional
  namespaces are separate database files introduced with `ATTACH`.
- `SPATIAL_FORMAT` is pinned to `'GeoJSON'` (the vendor cannot honour WKB
  or WKT on the way back out).
- `PARALLEL_WRITE_OPERATIONS` is `false`; see `sqliteDBI.js` for the
  measurements behind that.
- Connection properties are `{ filename, driver: sqlite3.Database }`. No
  `mode` is passed.

### 4.1 The `sqlite` / `sqlite3` bindings

`sqlite3` (the binding) is declared in `src/package.json` and resolves from
`src/node_modules`. There is no per-driver `node_modules`; an earlier nested
copy pinned a different major version and silently shadowed the declared
one.

- `open({ filename, driver })` from the `sqlite` promise wrapper returns a
  `Database`.
- Every statement is dispatched to the libuv threadpool. See
  `docs/Driver-Contracts.md` §9 for why that constrains parallelism.
- Repeated `statement.get()` iterates the result set and returns `undefined`
  once exhausted; this is what `SqliteReader` is built on.

---

## 5. Statement pipeline

### 5.1 Shapes

`sqliteStatementLibrary.js` defines the SQL statement shapes, including:

- `SQL_GENERATE_STATEMENTS` — the master "generate all statements for a
  table" shape consumed by the generator.
- Transaction control constants are `SQL_BEGIN_TRANSACTION`
  (`BEGIN TRANSACTION`), `SQL_COMMIT_TRANSACTION` (`COMMIT`),
  `SQL_ROLLBACK_TRANSACTION` (`ROLLBACK`), `SQL_CREATE_SAVE_POINT`
  (`SAVEPOINT YADAMU_SAVEPOINT`), `SQL_RESTORE_SAVE_POINT`
  (`ROLLBACK TO SAVEPOINT YADAMU_SAVEPOINT`) and `SQL_RELEASE_SAVE_POINT`
  (`RELEASE SAVEPOINT YADAMU_SAVEPOINT`). Note SAVE_POINT, not SAVEPOINT,
  in the constant names.
- Create/drop/select/truncate family — see file for exact strings.

`yadamuStatementGenerator.js` (base, :23-440) builds the statement cache:

- `generateStatementCache` — produces `SQL_GENERATE_STATEMENTS` entries per
  table.
- `generateStorageClause(...)` — per-column storage clause; requires a
  non-null `DATA_TYPES` object with `storageOptions`. A null/missing
  DATA_TYPES raises the `SOURCE_STORAGE_OPTIONS` error class seen in
  traces.
- Uses `getTargetDataTypes` / `SOURCE_DATA_TYPES` to resolve final storage
  classes.

`sqliteStatementGenerator.js` is the SQLite shim re-exporting the base
generator with SQLite semantics.

### 5.2 Execution order (normative)

For a table in COPY-to-SQLite:

```
DDL (drop/create as MODE requires)
→ writer doConstruct()      : SQL_BEGIN_TRANSACTION      (before any rows)
→ per batch                 : SQL_CREATE_SAVE_POINT
                               INSERT x rowCount
                               SQL_RELEASE_SAVE_POINT
                               releaseBatch(batch)
→ when written >= COMMIT_COUNT : SQL_COMMIT_TRANSACTION then a new BEGIN
→ doFinal()                 : SQL_COMMIT_TRANSACTION
```

COMMIT_COUNT is `BATCH_SIZE * COMMIT_RATIO` (10000 by default), so a table
smaller than that never reaches an intermediate commit: the transaction runs
from `doConstruct()` to `doFinal()`. On a mid-batch failure the writer does
`SQL_RESTORE_SAVE_POINT`, finalizes the prepared statement and switches to
Iterative mode.

---

## 6. Writer batch contract (`sqliteWriter.js`, `dbWriter.js`)

THE CONTRACT (all five are hard requirements):

1. The Writer receives batches via its InputStream. Batches are bounded by
   the DBI's `BATCH_SIZE`.
2. **`releaseBatch` MUST be called exactly ONCE per received batch.**
   The framework does NOT deliver the next batch until `releaseBatch`
   returns. Calling it zero times deadlocks; calling twice corrupts the
   stream.
3. **Writer modes:**
   - `Batch` mode — accumulate rows, execute statements once per batch.
   - `Iterative` mode — row-at-a-time with savepoint progress.
4. **Savepoint recovery:** on mid-batch failure the SQLite writer uses
   `sqliteDBI.releaseSavePoint` / `restoreSavePoint` (backed by
   `SQL_RELEASE_SAVEPOINT`) to rewind to the last released mark.
5. **Finalize statements:** after the last batch, the Writer emits the
   finalization statement(s) (`SQL_RELEASE_SAVEPOINT` and the closing
   commit). Failing to finalize leaves the DBI in a locked state.

Deadlock symptom: COPY stalls after N batches → a Writer never called
`releaseBatch`, or called it >once.

---

## 7. Transformers (`sqliteOutputManager.js`, `yadamuOutputManager.js`)

- Transformers are **value-driven**: they operate per-column on batch
  values, defaulting to identity.
- The base `yadamuOutputManager.js` defines the pipeline stages the SQLite
  output manager plugs into.
- Chain: InputStream → OutputManager(transformers) → Writer → DBI
  statements.

---

## 8. Configuration (CLI run configs)

QA/regression configs are passed to `src/qa/cli/test.js` as
`CONFIG=<file> CONN=<connections file>`. Canonical examples live in
`qa/regression/`, e.g. `sqliteAsTargetRoundtrip.json`.

```json
{
  "connections"     : "sqliteConnections.json",
  "operation"       : "dbRoundtrip",
  "tasks"           : "tasks.json",
  "recreateSchema"  : true,
  "verifyOperation" : true,
  "parameters"      : { "SPATIAL_FORMAT": "GeoJSON" },
  "tests"           : [
    { "source": "postgres#1", "target": "sqlite#1", "tasks": ["HR","BI"] }
  ]
}
```

The connections file is separate and is gitignored
(`.gitignore` matches `*[Cc]onnections.json`):

```json
{
  "sqlite#1"   : { "sqlite"   : { "database": "qa/regression/sqlite/test.db" } },
  "postgres#1" : { "postgres" : { "user": "...", "host": "...", "database": "...", "port": 5432 } }
}
```

`parameters` accepts the §3.3 fields — `SPATIAL_FORMAT`,
`INFINITY_MANAGEMENT`, `CIRCLE_FORMAT`, `PARALLEL`, `BATCH_SIZE` and so on.
`PARALLEL` is also accepted on the command line. Note that the CLI path
upper-cases parameter values, so `SPATIAL_FORMAT=GeoJSON` on the command
line becomes `GEOJSON` and will not string-match `GeoJSON`; set it in the
config file instead.

`bin/copy.sh` and `src/node/cli/copy.js` are the non-QA entry points.

---

## 9. Error protocol

- Failures append `exceptions/exception_<ISO8601 timestamp>.trace`,
  e.g. `exceptions/exception_2026-09-04T06.01.17.424Z.trace`.
- Trace content: timestamp, DBI identity, role, operation, stack of method
  invocations, and error classification.
- Rejected rows are additionally written to `rejections/` and the offending
  batch to a `.data` file alongside the `.trace`. The `.data` file contains
  `systemInformation` (including `driverSettings.spatialFormat`), the table
  `metadata` with `columnNames` and `dataTypes`, and the rejected rows — it
  is the fastest way to correlate a failing bind parameter (`C0`, `C4`, ...)
  with its declared column type.

- `RejectedColumnValue` (`src/node/core/yadamuException.js`) is the
  exception an OutputManager throws to reject a single column value, e.g.
  under `INFINITY_MANAGEMENT: REJECT`.

NOTE. An earlier draft of this section listed `SOURCE_SCHEMA_INFO`,
`TYPE_MISMATCH` and `SOURCE_STORAGE_OPTIONS` as error classes. None of those
identifiers exist anywhere in the codebase; they have been removed. Errors
surface as the vendor's own codes (`ORA-`, `SQLITE_`, tedious validation
messages) wrapped in the driver's exception class.

---

## 10. DATA TYPES schema

- `yadamuDataTypes.js` — vendor-neutral types carrying `storageOptions`
  (including `SPATIAL_FORMAT`, `CIRCLE_FORMAT`), `TIMESTAMP_PRECISION`,
  numeric precision/scale.
- `sqliteDataTypes.js` — SQLite `DriverDataTypes` mapping to SQLite storage
  classes; assigned into the DBI as `this.DATA_TYPES` (`sqliteDBI.js:48`).
- The statement generator and output manager read type info EXCLUSIVELY
  via the DBI's `DATA_TYPES` getter — never from raw constants.

---

## 11. Quick-reference call sequence (sqlite → sqlite COPY)

```
config JSON → cli/copy.js
  → sourceDbi = new sqliteDBI(connectionProperties...)   // DATA_TYPES set
  → targetDbi = new sqliteDBI(...)
  → sourceDbi.generateSchemaInfo(...)                    // :1717
  → applyIdentifierMappings / mergeMappings if provided  // §3.6
  → statementGenerator.generateStatementCache            // SQL_GENERATE_STATEMENTS
  → targetDbi.getDDLOperations / statement execution      // :183
  → per table: InputStream batches → writer.releaseBatch ×N → finalize
```