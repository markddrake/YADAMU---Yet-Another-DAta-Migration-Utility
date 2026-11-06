# Teradata

YADAMU reads from and writes to Teradata (tested with Teradata 17.10) using Teradata's Node.js driver,
[teradatasql](https://www.npmjs.com/package/teradatasql). The driver is not one of YADAMU's dependencies:
install it yourself before using a Teradata connection.

## Installing the driver

Install exactly version 20.0.72 in the directory that holds YADAMU's `node_modules` (`src` in a git
checkout, `/usr/src/YADAMU` in the YADAMU images):

```bash
npm install --no-save teradatasql@20.0.72
```

`--no-save` installs the driver without adding it to `package.json`, so a later `npm ci` removes it again.
Reinstall it after every `npm ci` or `npm install`.

YADAMU is pinned to 20.0.72 because every teradatasql release since 20.0.20 leaks the memory of each row it
fetches (the driver no longer frees the buffer its native library returns). YADAMU works around the leak
in its worker thread (`freeFetchedRowBuffers` in `src/node/dbi/teradata/worker/teradataWorker.js`), and the
workaround depends on the driver's internals. With another version YADAMU logs a warning; without the
driver, a Teradata connection fails with:

```
The Teradata driver is not installed. Install it with "npm install --no-save teradatasql@20.0.72" ...
```

teradatasql is licensed under the Teradata License Agreement, which is not an open source licence. Read it
before redistributing the driver, for example in a Docker image.

### Docker

`docker/build/teradata` builds `yadamu/teradata`: the `yadamu/secure` image plus teradatasql.

```bash
docker compose --file docker/build/docker-compose.yml build teradata
```

The build argument `BASE_IMAGE` selects another YADAMU image to start from, and `TERADATASQL_VERSION` the
driver version.

## Connections

A Teradata connection's settings are passed to `teradatasql.connect()` unchanged, so any of the driver's
connection parameters can be used (see the teradatasql documentation):

```json
"tera#1": {
  "teradata": {
    "host": "TDATA17-01",
    "user": "dbc",
    "password": "dbc"
  }
}
```

The driver does its database work in a worker thread, one per connection, so a long fetch never blocks
Node's event loop.

## Parameters

| Parameter | Default | |
|---|---|---|
| `TERADATA_FETCH_SIZE` | 50 | Rows fetched from the worker thread at a time. The original name, `TERDATA_FETCH_SIZE`, still works. |
| `TERADATA_BOOLEAN_STORAGE_OPTION` | `BYTEINT` | Teradata type that stores a boolean. |
| `SPATIAL_FORMAT` | `WKB` | Format spatial data is read in. Teradata's own WKT doesn't print enough digits to reproduce every coordinate, so use `WKT` only when a target needs it. |

## How data is stored

- Character columns (`CHAR`, `VARCHAR`, `CLOB`) are created `CHARACTER SET UNICODE`, whatever the source
  type, so the column limits are the UNICODE limits: 32,000 characters for `VARCHAR` and 1,048,544,000 for
  `CLOB`. Longer values become `CLOB`.
- Spatial data is stored as `ST_GEOMETRY`, JSON as `JSON` (`CHARACTER SET UNICODE`), XML as `XML`, and
  MySQL's `YEAR` as `INTERVAL YEAR(4)` (see [YEAR and INTERVAL YEAR](#year-and-interval-year)).
- `INTERVAL` values are exchanged as ISO 8601 durations (`P45DT12H30M15.123456S`), as the Oracle and Postgres
  drivers do. A negative interval has a leading minus sign (`-P1DT2H`), which Oracle accepts and the Postgres
  driver rewrites as negative fields (`P-1DT-2H`); intervals with negative fields are accepted too. A database
  without Teradata's other interval types (for example `INTERVAL HOUR TO MINUTE`) stores them as its
  day-to-second or year-to-month interval type. Oracle creates them with a leading precision of 4
  (`INTERVAL YEAR(4) TO MONTH`, `INTERVAL DAY(4) TO SECOND(6)`), enough for any Teradata interval.
- `TIME WITH TIME ZONE` and `TIMESTAMP WITH TIME ZONE` values are exchanged in UTC, as the Oracle and Postgres
  drivers do, so the offset isn't kept. A `TIME` is placed on 1970-01-01, and converting it to UTC can move it
  to the day before or after.
- SQL Server's `tinyint` is unsigned, so a `BYTEINT` (-128 to 127) is stored there as `smallint`.
- MongoDB's Decimal128 holds 34 significant digits, so a `DECIMAL` or `NUMBER` with a larger precision,
  including a `NUMBER` without one (up to 38 digits), is stored in MongoDB as a string (in Teradata's text form,
  for example `.0000000001`), which keeps every value exactly.
- MySQL's `DECIMAL` has at most 30 decimal places and MariaDB's 38, and they store a `NUMBER` without a precision
  as `DECIMAL(65,30)` and `DECIMAL(65,32)`, so its fraction is rounded to 30 or 32 places. The regression tests
  compare such values rounded to the same scale (`NUMERIC_SCALE` in `src/node/cfg/compareRules.json`).
- A `PERIOD` is exchanged as JSON in the form the Postgres driver uses for ranges (`lowerBound`, `upperBound`,
  the beginning included and the end excluded), and a database without `PERIOD` stores it as JSON.
- Teradata to Teradata, `LATIN` columns become `UNICODE` and `CASESPECIFIC` isn't kept: the metadata YADAMU
  exchanges has no character set or case sensitivity.
- A table whose first column is a LOB, JSON, XML or spatial column, which can't be indexed, is created
  `NO PRIMARY INDEX`. Others get Teradata's default primary index, on the first column.
- Every insert locks its table for the rest of the transaction (`LOCKING TABLE ... FOR WRITE`). A
  partitioned source table is written by several sessions at once, and without the table lock their row
  hash locks deadlock.
- XML is read with `XMLSERIALIZE`, and a carriage return in XML text is written as `&#xD;`, because
  Teradata writes it as a raw carriage return, which XML parsers turn into a line feed.

## YEAR and INTERVAL YEAR

Teradata has no `YEAR` type, so MySQL's and MariaDB's `YEAR` is stored as an `INTERVAL YEAR(4)` holding the
year: 2026 is `INTERVAL '2026' YEAR`. Every MySQL `YEAR` value (1901 to 2155) fits.

An `INTERVAL YEAR` is a duration, not a calendar year, so YADAMU exports it as one, as an ISO 8601 duration
(`P2026Y`, `-P1Y`), and other databases store it as their year-to-month interval type (`INTERVAL YEAR(4) TO
MONTH` in Oracle, `interval year to month` in Postgres, a `VARCHAR(32)` in databases without intervals, such as MySQL,
MariaDB and SQL Server). Every value keeps its meaning: an `INTERVAL YEAR` of 5 or -1 is a duration of 5 or -1
years, not the year 2005 or 0.

What that means for a copy:

- **MySQL or MariaDB to Teradata** is exact: each `YEAR` becomes the `INTERVAL YEAR` with the same number.
- **Teradata to MySQL or MariaDB**, into a new table, gives a `VARCHAR(32)` column of durations (`P2026Y`), not
  a `YEAR` column. Every value is kept.
- **Teradata to an existing MySQL or MariaDB `YEAR` column**, for example copying the data back to the
  database it came from, is exact for a value from 1901 to 2155, which arrives as that year. Any other value
  isn't a year MySQL can store: without strict mode MySQL would quietly store it as 0, so YADAMU rejects the row
  and writes it to the rejection file instead.

So a `YEAR` round trip through Teradata (MySQL to Teradata to MySQL) is safe when the data goes back into the
original `YEAR` columns, for example with `MODE` `DATA_ONLY` into the existing tables. Created from scratch, the
column comes back as `VARCHAR(32)` with the same values written as durations. A Teradata `INTERVAL YEAR` that
holds durations rather than years should only be copied into a `YEAR` column if every value is a year from 1901
to 2155.

## Lost connections

A lost session (`[Error 301] [SQLState 08S01]`) is reported as a lost connection, so YADAMU reconnects
and retries the table, as it does for other databases. A database that refuses connections
(`[SQLState 08001]`) is treated as restarting.

## Testing

The regression suites are `qa/regression/teradataShortRegression.json`, `teradataDataTypes.json` and
`teradataTestSuite.json`. They expect a Teradata database called `TDATA17-01` (the `tera#1` connection);
`docker/rdbms/teradata/docker/linux/kvm/README.md` explains how to run Teradata's 17.10 appliance in a
container.

`teradataDataTypes` copies two data sets: Postgres's data types (`t_postgres`, staged as a file), and
Teradata's own (`t_teradata`), which `docker/rdbms/teradata/testdata/dataTypeTesting.sql` creates. Run that
script once, as DBC, after installing YADAMU's Teradata objects (`src/sql/teradata/INSTALL_YADAMU.sql`):

```bash
bteq < docker/rdbms/teradata/testdata/dataTypeTesting.sql
```

(after `.LOGON TDATA17-01/dbc,dbc`). Teradata 17.10 negates an interval whose seconds are below one
incorrectly (`-INTERVAL '0.5' SECOND` is `-1.5`), so the script's negative intervals have at least one
second.

`docker/regressionTests/teradata/docker-compose.yml` builds `yadamu/teradata` and runs the three suites
(`chooseTest.sh teradata`) on the regression stack's network and shared volume. `TERADATA` is the address
of the Teradata host:

```bash
docker compose --file docker/regressionTests/teradata/docker-compose.yml build
TERADATA=192.168.1.233 docker compose --file docker/regressionTests/teradata/docker-compose.yml run --rm teradata
```

In the extended orchestrator run the `teradata` test does the same, and is skipped when Teradata isn't
answering or the image hasn't been built.

Teradata's compare (`teradataCompare.js`) checks LOB, JSON, XML and spatial values by their length and a
hash of their first two and last 4,000 characters, because Teradata 17.10 can't apply `EXCEPT` to them and
has no MD5 or SHA function. A change inside a longer value that keeps its length isn't detected.
