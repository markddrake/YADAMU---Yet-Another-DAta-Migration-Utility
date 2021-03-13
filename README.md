# YADAMU - Yet Another Data Migration Utility

This project has morphed in scope from being a simple drop in replacement for Oracle's traditional IMP and EXP utilities.
It now supports homegeneous IMPORT and EXPORT operations on Oracle 11g, 12c 18c and 19c, Postgres, MySQL, MariaDB, SQL Server, as well as hetrogeneous migrations between any of the supported databases. Support for MongoDB and Snowflake is also available on an experimental basis. Hence the new name - YADAMU. 

YADAMU migrates DDL and Content.

For homogeneous Oracle migrations all schema objects supported by DBMS_METADATA are migrated.

For homogeneous Postgres, MySQL and SQL Server migrations, or hetrogeneous migrations DDL operations are currently restriced to table layouts. Migration of indexes and other schema objects is not currently supported.

Supported Databases :
* Oracle 11.2.x and later
* Microsoft SQL Server 2014 and later
* Postgres 11 and later
* MySQL 8.0.12 and later
* MariaDB 5.5 and later
* MongoDB 4.0 and later
* Snowflake Data Warehouse

Support for other databases as well as older versions of Oracle, Postgres, MySQL/MariaDB and SQL Server is under consideration.

Click [here](doc/README.md) for a a set of Crib sheets that provide more information on installing and using YADAMU.
