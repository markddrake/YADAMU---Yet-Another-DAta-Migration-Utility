# YADAMU - Yet Another Data Migration Utility

This project has morphed in scope from being a simple drop in replacement for Oracle's traditional IMP and EXP utilities.
It now supports homegeneous IMPORT and EXPORT operations on Oracle 12c, Postgres, MySQL and SQL Server, as well as hetrogeneous migrations between any of the supported databases. Hence the new name - YADAMU. 

YADAMU migrates DDL and Content.

For homogeneous Oracle migrations all schema objects supported by DBMS_METADATA are migrated.

For homogeneous Postgres, MySQL and SQL Server migrations, or hetrogeneous migrations DDL operations are currently restriced to table layouts. Migration of indexes and other schema objects is not currently supported.

Details about this project can be found [here](http://markddrake.github.io/YADAMU---Yet-Another-DAta-Migration-Utility/docs)

YADAMU currently supports Oracle Database versions 12cR2 and later, Postgres 11,  MySQL 8.0.12 and SQL Server 14.

Support for other database and old versions of Oracle Postgres, MySQL and SQL Server is under consideration.
