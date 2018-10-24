# YADAMU - Yet Another Data Migration Utility

This project has morphed in scope from being a simple drop in replacement for Oracle's traditional IMP and EXP utilities.
It now supports homegeneous IMPORT and EXPORT operations on Oracle 12c, Postgres, MySQL and SQL Server, as well as hetrogeneous migrations between any of the supported databases. Hence the new name - YADAMU. 

YADAMU migrates DDL and Content.

For homogeneous Oracle migrations all schema objects supported by DBMS_METADATA are migrated.

For homogeneous Postgres, MySQL and SQL Server migrations, or hetrogeneous migrations DDL operations are currently restriced to table layouts. Migration of indexes and other schema objects is not currently supported.

Details about this project can be found [here](http://markddrake.github.io/YADAMU---Yet-Another-DAta-Migration-Utility/docs)

YADAMU currently supports Oracle Database versions 12cR2 and later, Postgres 11,  MySQL 8.0.12 and SQL Server 14.

Support for other database and old versions of Oracle Postgres, MySQL and SQL Server is under consideration.

Examples of using YADAMU

Pulling data from SQL Server Northwind database and loading it into Oracle

~~~
C:\Development\YADAMU\MSSQL>node node\export --USERNAME=sa --PASSWORD=pwd --HOSTNAME=192.168.1.250 --DATABASE=Northwind        --OWNER=\"dbo\"            --FILE=JSON\Northwind.json
2018-10-20T03:40:52.330Z - Table: "Categories". Rows: 8. Elaspsed Time: 9ms. Throughput: 889 rows/s.
2018-10-20T03:40:52.338Z - Table: "CustomerCustomerDemo". Rows: 0. Elaspsed Time: 3ms. Throughput: 0 rows/s.
2018-10-20T03:40:52.340Z - Table: "CustomerDemographics". Rows: 0. Elaspsed Time: 2ms. Throughput: 0 rows/s.
2018-10-20T03:40:52.356Z - Table: "Customers". Rows: 91. Elaspsed Time: 15ms. Throughput: 6067 rows/s.
2018-10-20T03:40:52.370Z - Table: "Employees". Rows: 9. Elaspsed Time: 14ms. Throughput: 643 rows/s.
2018-10-20T03:40:52.375Z - Table: "EmployeeTerritories". Rows: 49. Elaspsed Time: 4ms. Throughput: 12250 rows/s.
2018-10-20T03:40:52.460Z - Table: "Order Details". Rows: 2155. Elaspsed Time: 85ms. Throughput: 25353 rows/s.
2018-10-20T03:40:52.521Z - Table: "Orders". Rows: 830. Elaspsed Time: 55ms. Throughput: 15091 rows/s.
2018-10-20T03:40:52.532Z - Table: "Products". Rows: 77. Elaspsed Time: 5ms. Throughput: 15400 rows/s.
2018-10-20T03:40:52.537Z - Table: "Region". Rows: 4. Elaspsed Time: 5ms. Throughput: 800 rows/s.
2018-10-20T03:40:52.547Z - Table: "Shippers". Rows: 3. Elaspsed Time: 3ms. Throughput: 1000 rows/s.
2018-10-20T03:40:52.552Z - Table: "Suppliers". Rows: 29. Elaspsed Time: 5ms. Throughput: 5800 rows/s.
2018-10-20T03:40:52.566Z - Table: "Territories". Rows: 53. Elaspsed Time: 4ms. Throughput: 13250 rows/s.
Export operation successful.

C:\Development\YADAMU\MSSQL>cd ..\Oracle
C:\Development\YADAMU\Oracle>node node/jSaxImport userid=system/oracle@ORCL18c toUser=ADVWRK1 file=..\MSSQL\JSON\Northwind.json
(node:33520) [DEP0005] DeprecationWarning: Buffer() is deprecated due to security and usability issues. Please use the Buffer.alloc(), Buffer.allocUnsafe(), or Buffer.from() methods instead.
2018-10-20T03:43:06.625Z: Table "Categories". Rows 8. Elaspsed Time 54ms. Throughput 148 rows/s.
2018-10-20T03:43:06.632Z: Table "CustomerCustomerDemo". Rows 0. Elaspsed Time 0ms. Throughput NaN rows/s.
2018-10-20T03:43:06.632Z: Table "CustomerDemographics". Rows 0. Elaspsed Time 0ms. Throughput NaN rows/s.
2018-10-20T03:43:06.658Z: Table "Customers". Rows 91. Elaspsed Time 21ms. Throughput 4333 rows/s.
2018-10-20T03:43:06.736Z: Table "Employees". Rows 9. Elaspsed Time 62ms. Throughput 145 rows/s.
2018-10-20T03:43:06.754Z: Table "EmployeeTerritories". Rows 49. Elaspsed Time 0ms. Throughput Infinity rows/s.
2018-10-20T03:43:06.869Z: Table "Order Details". Rows 2155. Elaspsed Time 93ms. Throughput 23172 rows/s.
2018-10-20T03:43:06.922Z: Table "Orders". Rows 830. Elaspsed Time 29ms. Throughput 28621 rows/s.
2018-10-20T03:43:06.931Z: Table "Products". Rows 77. Elaspsed Time 1ms. Throughput 77000 rows/s.
2018-10-20T03:43:06.934Z: Table "Region". Rows 4. Elaspsed Time 0ms. Throughput Infinity rows/s.
2018-10-20T03:43:06.938Z: Table "Shippers". Rows 3. Elaspsed Time 0ms. Throughput Infinity rows/s.
2018-10-20T03:43:06.968Z: Table "Suppliers". Rows 29. Elaspsed Time 18ms. Throughput 1611 rows/s.
2018-10-20T03:43:06.973Z: Table "Territories". Rows 53. Elaspsed Time 0ms. Throughput Infinity rows/s.

Import operation completed successfully.
C:\Development\YADAMU\Oracle>
~~~
