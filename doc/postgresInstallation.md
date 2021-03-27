# Installing Yadamu Database support for Postgres

Procedures are installed using the script file src/install/postgres/sql/YADAMU_IMPORT.sql.
Procedures should be installed by the database owner. 
Procedures need to installed in each database where Yadamu will be used.

YADAMU assumes that the postgres extension pgcypto is enabled in the database where you are installed the Postgres Database support.

```bat
psql -Upostgres -hyadamu-db1 -dyadamu <src/install/postgres/sql/YADAMU_IMPORT.sql
```

```
Password for user postgres:
DO
DO
DO
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
CREATE FUNCTION
          YADAMU_INSTANCE_ID          | YADAMU_INSTALLATION_TIMESTAMP
--------------------------------------+-------------------------------
 55540516-6F29-4277-AE74-AD69DDB496CB | 2021-03-15T02:35:10+00:00
(1 row)
```