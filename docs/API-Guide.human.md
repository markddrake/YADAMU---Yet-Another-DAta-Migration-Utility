# The YADAMU DBI API — A Human-Readable Guide

*Written for developers who want to move data between databases with
yadamu's Node-based engine. Companion documents:
`docs/API-Reference.llm.md` (the same material in strict machine form) and
`docs/Driver-Contracts.md` (the contracts a new driver must satisfy, with
the symptom each failure produces).*

> Line references in this guide are indicative. The SQLite driver has been
> substantially rewritten since they were taken.

---

## What is this thing?

YADAMU's core idea is deceptively simple: **all database access goes
through one class per database** — a *DBI*, or Database Interface — and
the framework orchestrates DBIs to COPY, IMPORT, Upload or Load data
between them.

```
        ┌──────────────┐    ┌──────────────┐
        │  Source DBI  │    │ Target DBI   │
        │  (reads)     │    │  (writes)    │
        └──────────────┘    └──────────────┘
                 ▲                │
                 │ rows           │ SQL statements
                 ▼                │
             ┌────────────────────────┐
             │   Application (COPY)   │
             │  streams & batches     │
             └────────────────────────┘
```

A DBI isn't a driver — it's the *contract layer*. Under the SQLite DBI
sits the actual `Database` driver class that talks to SQLite. If you
swap the DBI (say, sqlite → postgres), the Application doesn't change.

---

## The pieces, in plain terms

**YadamuDBI** (`src/node/dbi/base/yadamuDBI.js`) is the abstract base
class. It defines what every DBI must expose: identity getters, capability
flags, schema generation, identifier (rename) mapping, and the statement
execution methods. Line references below point into this file.

**sqliteDBI** (`src/node/dbi/sqlite/sqliteDBI.js`) is the concrete
implementation for SQLite. It sets `this.DATA_TYPES = SqliteDataTypes`
(line 48), overrides `getDDLOperations()` (line 183), and implements the
savepoint protocol.

**The statement generator** builds SQL once per table, shaped by the
target's type system. SQLite's shapes live in
`sqliteStatementLibrary.js`; the base generator is
`yadamuStatementGenerator.js`.

**The Writer** (`sqliteWriter.js`) consumes rows in *batches* and runs
the SQL. Its batch contract is the single most important behavioral rule
in the whole system (next section).

**The OutputManager** applies per-column *transformers* to values as they
flow past — identity by default.

---

## The golden rule: `releaseBatch` exactly once

When you write or debug a Writer, remember this contract:

1. Rows arrive in the Writer's InputStream in batches.
2. **After processing each batch, call `releaseBatch` exactly once.** The
   framework will not deliver the next batch until you do — but it also
   won't forgive a second call.
3. When the last batch is done, the Writer *finalizes*: it emits the
   `SQL_RELEASE_SAVEPOINT` statement and commits. Skip this and the DBI
   stays locked mid-transaction.

If a COPY ever hangs after a suspiciously even number of batches, the
first suspect is a missing (or duplicated) `releaseBatch`. If it errors
out with a locked-connection symptom, look for a skipped finalize.

---

## How a SQLite → SQLite COPY actually runs

Everything starts with a config file, e.g. `qa/local/sqliteCopy.json`:

```json
{
  "action": "COPY",
  "dbKeys":  { ... source and target database identities ... },
  "connectionProperties": { "filename": "path/to/db.db", "mode": "rw" },
  "IDENTIFIER_MAPPings": { ... optional rename table ... },
  "parameters": { ... optional tuning ... }
}
```

Run it with the wrapper:

```bash
bash bin/copy.sh qa/local/sqliteCopy.json
```

Under the hood (`src/node/cli/copy.js`):

1. Source and target `sqliteDBI` instances are constructed and configured.
2. The source DBI runs `generateSchemaInfo()` — "what tables and columns
   do I have?"
3. If you provided an `IDENTIFIER_MAPPings` file, `mergeMappings` folds
   your renames into the generated ones — **your mappings always win**
   over generated ones — and `applyIdentifierMappings` walks the tables
   renaming as it goes (it only ever changes the name values, never the
   object keys, and logs every single rename it applies).
4. The statement generator emits the per-table SQL
   (`SQL_GENERATE_STATEMENTS`).
5. Rows stream in batches: select/insert statements run, then
   `SQL_RELEASE_SAVEPOINT`, and on the last batch the commit.

---

## Renames: the four invariants

If you're implementing or auditing the rename path, hold these true:

- User mappings are sacrosanct (the code says so in a comment near
  line 1004 of `yadamuDBI.js`).
- `applyIdentifierMappings` never mutates keys — only the `tableName`
  property values.
- Column renames are positional: it finds `columnNames.indexOf(name)` and
  overwrites that index. If a name appears twice, the last mapping wins.
- Every applied rename is logged (when reporting is on): vendor, table,
  column, old → new.

---

## Errors, and how to read them

When something fails, yadamu appends a trace file under `exceptions/`,
named by timestamp: `exception_2026-09-04T06.01.17.424Z.trace`. Inside:
the DBI, its role (source/target), the operation, a call stack, and the
error classification.

Three classes we confirmed the hard way this session:

- **SOURCE_SCHEMA_INFO** — schema rows came back malformed. Fix the
  schema load path, not the stream.
- **TYPE_MISMATCH (obj null) in generateSQLQuery / generateStorageClause** —
  a null dereference caused by the schema loader not populating
  `DATA_TYPES`/queryInfo in the shape those methods expect. This is a
  statement-generation shape problem, *never* a batching problem —
  harness tests proved the stream layers sound.
- **SOURCE_STORAGE_OPTIONS** — the type schema lacked `storageOptions`
  (spatial format, circle format, etc.). Feed the generator a complete
  `DATA_TYPES` object.

Diagnostic shortcut: null-`obj` errors in statement generation almost
always mean the `SQL_GENERATE_STATEMENTS` load path, not the writer.

---

## Where the types come from

`sqliteDataTypes.js` maps SQLite's storage classes;
`yadamuDataTypes.js` holds the vendor-neutral shape with `storageOptions`.
The concrete DBI assigns its types into `this.DATA_TYPES`, and *every*
consumer (generator, output manager) reads types only through that getter.
That indirection is what makes cross-vendor COPY possible.

---

## Tuning knobs you can set per-run

Through `parameters` in the config (resolution is always
`parameters → DBIConstants`): `TABLE_MAX_ERRORS`, `TOTAL_MAX_ERRORS`,
`MODE`, `ON_ERROR`, `INFINITY_MANAGEMENT`, `STAGING_FILE_RETENTION`,
`BYTE_TO_CHAR_RATIO`, `RESET_IDENTITY`, `PARTITION_LEVEL_OPERATIONS`,
`COMMIT_RATIO`, `BATCH_LIMIT`. Type-shape fields (`SPATIAL_FORMAT`,
`CIRCLE_FORMAT`, `TIMESTAMP_PRECISION`) resolve to `DATA_TYPES` defaults
when unspecified.

`BATCH_SIZE` itself is derived — from `BATCH_LIMIT` and `COMMIT_RATIO` —
so don't set it directly.

---

## Extending to a new database

The pattern to follow lives in `sqliteDBI.js` (and the `exampleDBI.js`
scaffold): subclass `YadamuDBI`, assign your `DriverDataTypes`, override
`getDDLOperations()`, provide a `StatementLibrary` with your SQL shapes
(including `SQL_RELEASE_SAVEPOINT`), and implement a Writer honoring the
batch contract. The Application will drive it unchanged.