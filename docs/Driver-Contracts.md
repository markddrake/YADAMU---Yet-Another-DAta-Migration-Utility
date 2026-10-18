# Writing a YADAMU DBI Driver — Contracts That Bite

Companion to `API-Guide.human.md` and `API-Reference.llm.md`. Everything
below was established while building the SQLite driver, mostly by watching
it fail. Each entry names the symptom, because the symptom is rarely near
the cause.

---

## 1. Connection lifecycle

### `addVendorExtensions(connectionProperties)` MUST return the object

The base implementation returns `undefined`, and `CONNECTION_PROPERTIES`
caches whatever it returns. A driver that does not override it — or that
overrides it and forgets to return — caches `undefined`.

Symptom: `redactPasswords()` throws inside `structuredClone()`, and
`createConnectionPool()` cannot read the database name. MySQL is the model:
mutate the object, then `return connectionProperties`.

### `getConnectionFromPool()` returns a NEW connection each call

Not a shared one. The base `setWorkerConnection()` does:

```js
this.connection = await this.manager.getConnectionFromPool()
```

so every parallel worker calls it. Returning one shared connection puts all
workers on the same session. Where a transaction belongs to a connection,
that means workers nest their `BEGIN` statements and one worker's `COMMIT`
commits another worker's rows.

Symptom: `cannot start a transaction within a transaction`, plus rows
committed that should have rolled back.

If the vendor has no pool concept, `createConnectionPool()` can be a no-op
and `getConnectionFromPool()` can open on demand. `closeConnection()` then
closes only the caller's own handle and `closePool()` becomes a no-op. The
base calls `configureConnection()` for each worker connection, so
per-connection settings are applied automatically.

---

## 2. Reading

### `_getInputStream(queryInfo)` MUST return a Readable

Not an array. The base attaches listeners to it immediately:

```js
inputStream.once('readable', ...).on('end', ...).on('error', ...)
```

Symptom: `inputStream.once is not a function`, and every table skipped.
Returning an array also materialises the whole table in memory, which
defeats the pipeline.

### Rows are POSITIONAL ARRAYS, not objects

`YadamuParser.rowTransformation` indexes `row[idx]`. This is why the
Postgres drivers request `rowMode: 'array'` and the MySQL ones
`rowsAsArray: true`. If the binding has no such option, map each row
yourself using `queryInfo.COLUMN_NAME_ARRAY`.

### `generateSelectListEntry(columnInfo)` is the read-side cast hook

`columnInfo` is `[schema, table, column, dataType]`. Return the select-list
entry for that column, aliased back to the column name. Vertica and
Redshift both override it:

```js
return `CAST("${columnInfo[2]}" AS VARCHAR) "${columnInfo[2]}"`
```

Use it when the binding cannot return a value faithfully. The write-side
equivalent is `getInsertOperator(targetDataType)` on the statement
generator, which returns the bind placeholder and may wrap it in a cast —
DB2 returns `XMLPARSE(DOCUMENT CAST(? AS BLOB) PRESERVE WHITESPACE)`.

The base calls `generateSelectListEntry` only from `buildSchemaInfo`, i.e.
when the metadata query returns one row per COLUMN. A driver whose metadata
query returns one row per TABLE must call the hook itself with the same
`columnInfo` shape.

---

## 3. Capabilities and parallelism

```js
PARALLEL_OPERATIONS = PARALLEL_READ_OPERATIONS && PARALLEL_WRITE_OPERATIONS
PARALLEL_ENABLED    = source.PARALLEL_OPERATIONS && target.PARALLEL_OPERATIONS
```

Declaring writes sequential therefore also disables parallel READS from that
vendor, in both directions. There is currently no way to express "parallel
reads, sequential writes".

`dbReader.pipe()` is where a target constrains parallelism:

```js
if (!this.dbWriter.dbi.PARALLEL_WRITE_OPERATIONS) {
  this.dbi.yadamu.parameters.PARALLEL = 0
}
```

`Yadamu.PARALLEL` is a GETTER ONLY — assigning to it throws in an ES module.
Write to `yadamu.parameters.PARALLEL`.

A driver can cap rather than disable, from its own `initialize()`:
`target.initialize()` runs before `PARALLEL_ENABLED` is computed and before
the worker count is read, so a clamp there is honoured. Gate it on
`this.IS_WRITER` so the source role is not throttled as well.

---

## 4. System information and format negotiation

`getSystemInformation()` in the base is SYNCHRONOUS. An override that does
`Object.assign(super.getSystemInformation(), ...)` is correct — do not
await it.

It carries `driverSettings` from `getDriverSettings()`:

```js
{ spatialFormat: this.SPATIAL_FORMAT, timestampPrecision: this.TIMESTAMP_PRECISION }
```

The TARGET reads the SOURCE's value back as `INBOUND_SPATIAL_FORMAT`, which
is how it knows what format the incoming data is actually in. Two separate
obligations follow:

1. DECLARE the format the driver stores (`SPATIAL_FORMAT`).
2. STORE that format — recode on input when the source produced something
   else, e.g. `YadamuSpatialLibrary.toGeoJSON(this.dbi.INBOUND_SPATIAL_FORMAT)`.

Declaring without storing is the failure mode: the target parses the
declared format and finds something else. Symptom: `ORA-40441: JSON syntax
error`, or a spatial converter throwing.

Pin the format when the vendor cannot honour alternatives. Postgres does
exactly this when PostGIS is absent, returning `'GeoJSON'` unconditionally
rather than consulting `parameters.SPATIAL_FORMAT`.

---

## 4a. Serialized storage, and the reversal contract

A target frequently has no native type that holds a value at full fidelity. The
rule is:

  **If the target cannot hold it natively, store a serialized form - and the
  transformation MUST reverse itself.**

Reversal is what makes it legitimate. A roundtrip test compares against a table
built by a DDL-only clone of the source, so the schema is identical by
construction; the driver's obligation is that the VALUE survives the trip
through whatever representation it had to use.

This is not a SQLite quirk. It already exists in at least two drivers, invented
independently and documented in neither:

| driver | what it serializes | why |
|---|---|---|
| MSSQL | `bigint` as `sql.VarChar(20)` | "Bind as VarChar to avoid rounding issues" |
| MSSQL | decimals as `sql.VarChar(precision + 2)` | same |
| SQLite | 64 bit integers, `CAST(col AS TEXT)` on read | the binding returns every INTEGER as a double |
| SQLite | exact decimals, stored as text | REAL is a double |
| SQLite | spatial, recoded to GeoJSON | no spatial type at all |

### It is not always a preference

Where two representations both work, serializing is a fidelity choice a user
could reasonably override. Often they do not both work:

- MSSQL binds decimals and bigints as VarChar, so it REJECTS a number outright
  with `Validation failed for parameter 'C0'. Invalid string.` A source that
  stores those natively cannot feed an MSSQL target at all.
- Postgres `money` has a hard range of +/-92233720368547758.08. Through a double
  the boundary value becomes 92233720368547760, which is outside the type. That
  is an insert failure, not a precision difference.

Measured: mapping SQLite's exact decimals to REAL instead of text produced 189
errors across 18 tables against an MSSQL target, and put `money` out of range
against Postgres. Serialized storage is load bearing, not stylistic.

### What a comparison tolerance can and cannot do

`compareRules.json` carries `DOUBLE_PRECISION`, `NUMERIC_SCALE`,
`SPATIAL_PRECISION` and `TIMESTAMP_PRECISION`, keyed source then target. They
absorb differences in how two engines RENDER the same value - MongoDB, which
also stores numbers as doubles, declares `DOUBLE_PRECISION: 14`.

A tolerance only helps once the row has landed. It cannot rescue a value the
target refuses to insert.

### Irreversible transformations go elsewhere

Anything that cannot reverse is declared in `compareRules.json` instead, where
it is announced in the log and can be made fatal:

  `EMPTY_STRING_IS_NULL`   Oracle treats `''` as NULL
  `MIN_BIGINT_IS_NULL`     the most negative BIGINT becomes NULL
  `INFINITY_IS_NULL`       NaN and +/-Infinity become NULL
  `CSV_EMPTY_STRING_IS_NULL`

These are opt in per vendor pair - absent means the difference is an error -
and `INFINITY_IS_NULL` only applies while `INFINITY_MANAGEMENT` is `NULLIFY`,
so setting it to `REJECT` turns the tolerance off.

### The SQLite constraint on naming

SQLite derives a column's affinity from its declared type name, testing for
`INT` BEFORE `CHAR`/`CLOB`/`TEXT`. A serialized marker must therefore contain
`CHAR`, `CLOB` or `TEXT` to get TEXT affinity, and must not contain `INT`. A
literal `SERIALIZED_NUMERIC` would take NUMERIC affinity and silently convert
the value - the exact failure it exists to prevent. Hence `NUMERIC_TEXT`, and
`DURATION_` rather than `INTERVAL_` for the interval family.

---

## 5. NaN and infinity — a THREE part contract

Miss any part and values are lost silently.

1. The driver's OutputManager implements the `INFINITY_MANAGEMENT` switch on
   float columns: `REJECT` throws `RejectedColumnValue`, `NULLIFY` logs a
   warning and returns null. MSSQL, MySQL, MariaDB and DB2 all do this.
2. `src/qa/cfg/yadamuDefaults.json` gives the vendor
   `"INFINITY_MANAGEMENT": "NULLIFY"` when that is the right default.
3. `src/node/cfg/compareRules.json` registers the vendor under
   `INFINITY_IS_NULL`, keyed source then target, so the comparison treats
   the nullified values as equal.

`compareRules.json` also holds `SPATIAL_PRECISION`, `DOUBLE_PRECISION`,
`TIMESTAMP_PRECISION` and `NUMERIC_SCALE` tolerances. A vendor absent from a
rule falls back to `default`, which is usually full precision — that shows
up as data differences rather than errors.

---

## 6. Type mappings

`<vendor>TypeMappings.json` has `mappings`, `limits` and `storageOptions`.
`storageOptions` carries `SPATIAL_FORMAT`, `CIRCLE_FORMAT`, `BOOLEAN_TYPE`.

`UNBOUNDED_TYPES` on the DataTypes class lists declared types that take no
size constraint.

`YadamuDataTypes.decomposeDataType('INTEGER(64)')` returns
`{type:'INTEGER', length:64}`. It is a STATIC and is inherited by every
vendor subclass, so `<Vendor>DataTypes.decomposeDataType(...)` works without
importing the base.

The size constraint is NOT in consistent units — it comes from the source.
Postgres `int8` arrives as `INTEGER(64)` (bits); SQL Server `bigint` arrives
as `INTEGER(19)` (decimal digits). Any `length > n` test misclassifies one
of them. Enumerate the markers instead.

`YadamuDataTypes.isSpatial(dataType)` matches a fixed list (`GEOMETRY`,
`POINT`, `POLYGON`, ...). A driver that maps every spatial type to `TEXT`
erases spatial-ness: on the return leg the column is just text and the
target falls back to its own default format. Declare a type the classifier
recognises.

---

## 7. Writer batch contract

`releaseBatch(batch)` MUST be called exactly once per batch, on every path
out of `_writeBatch` — including the path where the writer downgrades from
Batch to Iterative mode. `sqliteWriter` and `mariadbWriter` each call it
twice for that reason: once on the Batch success path, once at the end of
the Iterative path.

The transaction is opened in `doConstruct()`, BEFORE any rows arrive, and
committed only once `PIPELINE_STATE.written >= COMMIT_COUNT`
(`BATCH_SIZE * COMMIT_RATIO`; 10000 by default). For tables smaller than
that threshold there is no intermediate commit at all — the transaction is
held from construction through to `doFinal()`.

---

## 8. Registering a driver

| File | What it needs |
|---|---|
| `src/node/cfg/yadamuDefaults.json` | driver path under the vendor key |
| `src/qa/cfg/yadamuDefaults.json` | QA driver path, plus vendor defaults |
| `src/node/dbi/cfg/dataTypeConfiguration.json` | DataTypes class + mappings file |
| `src/node/cfg/compareRules.json` | comparison tolerances |
| `src/package.json` | the binding dependency |

A driver missing from `compareRules.json` still runs — it just compares at
default precision.

---

## 9. Embedded databases: what SQLite taught us

Applicable to any in-process/embedded target.

**The binding may run on the libuv threadpool.** `node-sqlite3` dispatches
every statement there (default 4 threads). A writer blocked on a lock holds
its thread for the whole busy timeout, so once blocked writers fill the pool
nothing can commit and the operation deadlocks rather than merely queueing.
Measured, 7 workers on a 4-thread pool: 6m34s and 48 errors; the same 7
workers on a 16-thread pool: 3.4s clean. `UV_THREADPOOL_SIZE` is read by
libuv at startup and cannot be changed from inside the process.

**Dynamic typing erases the source type.** SQLite has five storage classes,
so a bigint, a decimal and a geometry all land as TEXT or INTEGER unless the
driver declares something more specific. Two consequences: pick declared
type names the classifiers recognise (`GEOMETRY`, not `TEXT`), and remember
that the declared type is the only surviving evidence of what the column was.

**The binding may not be able to return every value faithfully.**
`node-sqlite3` returns every INTEGER as a JS number, so anything beyond
`Number.MAX_SAFE_INTEGER` is rounded on the way out even though SQLite stored
it exactly. `CAST(col AS TEXT)` inside the engine returns the exact digits.

**Targets may require a particular representation.** SQL Server binds bigint
parameters as `sql.VarChar(20)` and casts in the DML, specifically to avoid
this rounding — so it requires a string and rejects a number with
`Validation failed for parameter 'C0'. Invalid string.`

**Some values have no representation at all.** SQLite stores NaN and
±Infinity as NULL, silently. See §5.
