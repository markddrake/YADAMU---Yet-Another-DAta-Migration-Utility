# YADAMU regression tests

This folder holds the regression tests YADAMU's maintainers run before a release, and the
configuration they use. This page explains how to build the same test environment: the databases,
where the test data comes from, the connection files, and how to start a run.

It covers only the Linux Docker Compose stack (`docker/regressionTests` and `docker/orchestrator`).
The Windows scripts in the repository are older and are not maintained. Everything here is provided
as-is.

## No performance results

This repository contains no benchmark or performance results, and none should be read into it. The
suites check that data arrives intact and that comparisons match; they are not built to measure
speed. Commercial database licences (Oracle, Microsoft SQL Server and others) restrict the
publication of benchmark results. If you run these tests, your own database vendors' licence terms
apply to anything you publish about them.

## The test stack

`docker/regressionTests/docker-compose.yml` defines the databases: Oracle (11.2, 12.2, 18c and
23.26), SQL Server 2022 and 2025, PostgreSQL, MySQL, MariaDB, MongoDB, Vertica, plus MinIO and
Azurite for the cloud storage tests and a Samba container (`YADAMU-SMB`) that shares the test
volume.

* **Oracle images.** `yadamu/oracle:<version>` is built locally from the files in
  `docker/rdbms/oracle/build`. It is not on any registry. You need Oracle's installation media,
  used under Oracle's licence terms.
* **MinIO.** `MINIO-01` runs MinIO AIStor, which needs a licence key. AIStor has a free licence;
  save yours as `docker/License Keys/minio.license` before building `yadamu/aistor`.
* **Databases outside the stack.** Some suites use databases that don't run in containers: SQL
  Server 2014 (the `mssql2014` suites in the standard run), SQL Server 2012 and Teradata (extended
  run). `docker/orchestrator/docker-compose.yml` gives their addresses as `MSSQL14`, `MSSQL12` and
  `TERADATA`. If one of them doesn't answer, the orchestrator skips its suites with one error line.
* **Teradata.** The Teradata suites need Teradata's VMware appliance, converted with
  `docker/rdbms/teradata/docker/linux/kvm/README.md`. Nothing from Teradata is included in YADAMU.
  Teradata no longer offers this image for download; contact Teradata about its current
  availability and licence terms. The image we built and tested with:
  * File: `Teradata_Database_17.10.02.01_SLES12_SP3_on_VMware_20210603130901.ova`
  * Size: 4,593,858,560 bytes
  * SHA-256 (whole .ova): `383522f5546c40b9b156bd429d82e1fdd82a144b382a0ba15d3bbd01d0502ef1`
  * Teradata's manifest inside the .ova (.mf, SHA-1): .ovf
    `0564efe2cada9f11392a1e024609b794f952a7e2`; disk-0.vmdk
    `85a5bfa6b0a3e1c1e1f774ef877fa5afc83262a6`
* **Snowflake** (extended run) needs your own Snowflake account. To run the extended suites
  without it, set `EXTENDED_EXCLUDE=snowflake` in the orchestrator's environment.

### Host requirements

* An x86_64 Linux host with Docker and Docker Compose v2.
* Memory: the compose file lets each database container use up to 16 GB, and several run at once.
  Size the host, or lower `mem_limit`, accordingly.
* `fs.aio-max-nr` of at least 1048576 (`sysctl -w fs.aio-max-nr=1048576`, and persist it in
  `/etc/sysctl.d`). With the default, Oracle fails with ORA-27090.
* The compose file creates the `YADAMU-NET` network and the `YADAMU_01-SHARED` volume. The test
  logs are written to that volume, and `YADAMU-SMB` shares it over SMB.

## Test data

The setup scripts load sample databases from `docker/rdbms/<database>/testdata`. Most of this data
belongs to the database vendors and is not part of YADAMU, so download it from the links below and
put each file where the table says. Files marked "included" are already in the repository.

| Database | File(s) | Put in | Source |
|---|---|---|---|
| Oracle | `sampleSchemas.zip` (HR, OE, PM, IX, SH, BI) | `docker/rdbms/oracle/testdata/` | not published; see [Oracle sample schemas](#oracle-sample-schemas) |
| Oracle | `pm_online_media.exp` | included | Oracle's sample data, MIT licence; see `pm_online_media.LICENSE.txt` beside it |
| SQL Server | `AdventureWorks<year>.bak`, `AdventureWorksDW<year>.bak` | `docker/rdbms/mssql/testdata/<year>/` | [Microsoft sql-server-samples, AdventureWorks release](https://github.com/microsoft/sql-server-samples/releases/tag/adventureworks) |
| SQL Server | `WideWorldImporters-Full.bak`, `WideWorldImportersDW-Full.bak` | `docker/rdbms/mssql/testdata/WideWorldImporters/` | [Microsoft sql-server-samples, Wide World Importers release](https://github.com/microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0) |
| SQL Server | `instnwnd.sql` (Northwind) | `docker/rdbms/mssql/testdata/Northwind/` | [Microsoft sql-server-samples, northwind-pubs](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs) |
| MySQL, MariaDB | `sakila-schema.sql`, `sakila-data.sql` | `docker/rdbms/mysql/testdata/sakila/` and `docker/rdbms/mariadb/testdata/sakila/` | [MySQL sakila database](https://dev.mysql.com/doc/index-other.html) |
| PostgreSQL | `dataTypeTesting.sql`, `ddlTesting.sql`, `vectorTesting.sql` | included | YADAMU |
| Teradata | the `financial` dataset and `fin_install.btq` | `docker/rdbms/teradata/testdata/` | [1999 Czech financial dataset for Teradata](https://github.com/dnoeth/1999_Czech_financial_dataset_Teradata) |

`<year>` is the SQL Server release (2022, 2025, and 2014 or 2012 if you run those suites). The
setup script picks the folder from the version the container reports.

### Oracle sample schemas

The Oracle QA databases are built from `docker/rdbms/oracle/testdata/sampleSchemas.zip`, a locally
modified copy of release v12.2.0.1 of Oracle's
[db-sample-schemas](https://github.com/oracle-samples/db-sample-schemas) (MIT licence). It is not
published. The changes make the scripts find their own directory, on Linux and Windows, instead of
needing the release's `__SUB__CWD__` token replaced before they run, and skip the Oracle Multimedia
(ORDSYS) part of the PM schema on 19c and later, where Oracle Multimedia no longer exists. Newer
releases have archived OE and PM, which the tests use.

`pm_online_media.exp` is loaded only when `PM.ONLINE_MEDIA` exists and is empty after the sample
schemas are installed. On 11.2, 12.2 and 18c the sample schemas install those rows themselves, and
23.26 doesn't create the table, so on the standard stack the dump is never needed. The dump
is an `EXPORT:V18` file, which `imp` from 11.2 and 12.2 can't read.

### Data you can't get

The MySQL and MariaDB setup also loads `jtest.audit.sql` (schema `jtest`), a private data set kept
for its difficult Chinese and Japanese text. It is not available. Without it the setup logs one
error and carries on; remove `jsonExample` from the `mysql` task list in `qa/regression/tasks.json`
so the tests don't look for it. You will probably want to add your own data sets anyway.

## Connection files

`qa/connections/connections.json` is the reference connection file for the stack. It uses the
containers' host names and the passwords the setup scripts set. Those passwords are deliberately
predictable, so don't expose the stack outside a test network. The cloud entries use the public
example keys for MinIO and Azurite.

The test image (`docker/build/secure`) copies `qa/connections/secureConnections.json` to
`qa/regression/connections.json`. That file is not in the repository: create it from
`connections.json`, and add the cloud connections you have (`awsS3#1`, `awsS3-staging`,
`redshift#1`, `snow#1`) with your own credentials. Keep it out of version control.

## Running the tests

The orchestrator (`docker/orchestrator`) runs everything in containers on the docker host. Its
compose file takes the source from `YADAMU_SOURCE`; set that to your checkout.

1. Build `yadamu/environment` (Node.js, the database drivers, Oracle Instant Client):
   `docker compose -f docker/build/docker-compose.yml build environment`.
2. Build the orchestrator: `docker compose -f docker/orchestrator/docker-compose.yml build`. It
   copies the source in at build time, so rebuild it after every change.
3. `docker compose -f docker/orchestrator/docker-compose.yml up recreate-db` drops and recreates
   the stack, waits for the Oracle databases to be created, then loads the sample data.
4. `... up standard` runs the standard suites; `... up extended` runs the extended ones. Each run
   rebuilds the YADAMU test images from the orchestrator's copy of the source.
5. The logs are in `/mount/log` on `YADAMU-SMB`. `docker exec YADAMU-SMB sh /mount/status.sh`
   prints one line per suite with its error, warning and failure counts.

Server-side upload (parser `SQL`, where the database reads the export file itself) is frozen: it
gets crash fixes only. Its suites aren't part of the standard or extended runs; `... up upload` runs
them on their own. They read the data that the initialization task (the first part of a standard or
extended run) stages, so on a new stack run one of those first.

Some suites produce errors on purpose: the lost connection suites kill sessions while data is
moving, and the data type suites include mappings that a target can't hold. So zero errors is not
the pass mark. A run passes when each suite's counts match your own earlier, reviewed run, and every
difference is explained. The first run after the databases are recreated can show Oracle errors
that don't come back in a second run.
