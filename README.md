# YADAMU - Yet Another Data Migration Utility

This project has morphed in scope from being a simple drop in replacement for Oracle's traditional IMP and EXP utilities.
It now supports homegeneous IMPORT and EXPORT operations on Oracle, Postgres, MySQL, MariaDB, SQL Server, Vertica, Amazon Redshift as well as hetrogeneous migrations between any of the supported databases. Support for MongoDB and Snowflake is also available on an experimental basis. Hence the new name - YADAMU. 

YADAMU migrates DDL and Content.

For homogeneous Oracle migrations all schema objects supported by DBMS_METADATA are migrated.

For databases other than Oracle, or hetrogeneous migrations DDL operations are currently restriced to table layouts. Migration of indexes and other schema objects is not currently supported.

Supported Databases :
* Oracle 21c
* Oracle 19c
* Oracle 18c
* Oracle 12cR2
* Oracle 11gR2
* Microsoft SQL Server 2014
* Microsoft SQL Server 2017
* Microsoft SQL Server 2019
* Postgres 11 through Postgres 14
* MySQL 8.0.12 and later
* MariaDB 5.5 and later
* MongoDB 4.0 and later
* Snowflake Data Warehouse
* Vertica 9 through Vertica 11
* DB2 11 LUW
* Yugabyte

* Amazon Redshift (Experimental)
* Teradata 17 (Expermintal)

Curerntly a work in progress

* CockroachDB

Yadamu can also export data sets to Amazon AWS S3 Storage and Microsoft Azure BLOB storage. Data can be exported as CSV, JSON Arrays, or JSON.

YADAMU has three modes of operation. 
* It can export data from the source database to a file, and then import the contents of the file into the target database. (EXPORT/IMPORT)
* It can also pump data directly from the source database to the target database. (PUMP)
* It can stage data from a source database as set of CSV files and then instruct the target database to load the data directly from the CSV files (COPY)

COPY operations are currently restrictred to Vertica, Snowflake and Redshift.

Support for other databases as well as older versions of Oracle, Postgres, MySQL/MariaDB and SQL Server is under consideration.

Click [here](doc/README.md) for a a set of Crib sheets that provide more information on installing and using YADAMU.
