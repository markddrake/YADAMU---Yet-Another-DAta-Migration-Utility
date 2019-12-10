# YADAMU - Yet Another Data Migration Utility

This project has morphed in scope from being a simple drop in replacement for Oracle's traditional IMP and EXP utilities.
It now supports homegeneous IMPORT and EXPORT operations on Oracle 11g, 12c 18c and 19c, Postgres, MySQL, MariaDB, SQL Server, as well as hetrogeneous migrations between any of the supported databases. Support for MongoDB is also available on an experimental basis. Hence the new name - YADAMU. 

YADAMU migrates DDL and Content.

For homogeneous Oracle migrations all schema objects supported by DBMS_METADATA are migrated.

For homogeneous Postgres, MySQL and SQL Server migrations, or hetrogeneous migrations DDL operations are currently restriced to table layouts. Migration of indexes and other schema objects is not currently supported.

Details about this project can be found [here](http://markddrake.github.io/YADAMU---Yet-Another-DAta-Migration-Utility/docs)

YADAMU currently supports Oracle Database versions 11.2.x and later, Postgres 11 and later,  MySQL 8.0.12, MariaDB 5.5 and SQL Server 14.

Support for other databases as well as older versions of Oracle, Postgres, MySQL/MariaDB and SQL Server is under consideration.

Simple examples of using YADAMU

Exporting data from the HR Schema in an Oracle database iddentified by the TNS Alias ORCL19c and storing it in a file called HR.json
~~~
C:\Development\YADAMU>bin\export rdbms=oracle userid=system/@ORCL19c file=HR.json FROM_USER=HR

C:\Development\YADAMU>REM Run from YADAMU_HOMEC:\Development\YADAMU>node C:\Development\YADAMU\app\YADAMU\common\export.js rdbms=oracle userid=system/@ORCL19c file=HR.json FROM_USER=HR
Enter password for Oracle connection: ******

2019-12-10T06:55:52.734Z [DBReader][Oracle]: Ready. Mode: DATA_ONLY.
2019-12-10T06:55:52.767Z [DBWriter][FILE]: Ready. Mode: DATA_ONLY.
2019-12-10T06:55:52.776Z [FileDBI]: Writing file "C:\Development\YADAMU\HR.json".
2019-12-10T06:55:54.039Z [DBReader][JOBS]: Rows read: 19. Elaspsed Time: 00:00:00.006s. Throughput: 2948 rows/s.
2019-12-10T06:55:54.046Z [DBWriter][JOBS][JSON]: Rows written 19. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.014s. Throughput 1399 rows/s.
2019-12-10T06:55:54.052Z [DBReader][REGIONS]: Rows read: 4. Elaspsed Time: 00:00:00.005s. Throughput: 741 rows/s.
2019-12-10T06:55:54.053Z [DBWriter][REGIONS][JSON]: Rows written 4. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.006s. Throughput 630 rows/s.
2019-12-10T06:55:54.064Z [DBReader][COUNTRIES]: Rows read: 25. Elaspsed Time: 00:00:00.010s. Throughput: 2283 rows/s.
2019-12-10T06:55:54.065Z [DBWriter][COUNTRIES][JSON]: Rows written 25. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.012s. Throughput 2132 rows/s.
2019-12-10T06:55:54.087Z [DBReader][EMPLOYEES]: Rows read: 107. Elaspsed Time: 00:00:00.021s. Throughput: 4988 rows/s.
2019-12-10T06:55:54.088Z [DBWriter][EMPLOYEES][JSON]: Rows written 107. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.022s. Throughput 4888 rows/s.
2019-12-10T06:55:54.103Z [DBReader][LOCATIONS]: Rows read: 23. Elaspsed Time: 00:00:00.014s. Throughput: 1610 rows/s.
2019-12-10T06:55:54.104Z [DBWriter][LOCATIONS][JSON]: Rows written 23. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.015s. Throughput 1542 rows/s.
2019-12-10T06:55:54.119Z [DBReader][DEPARTMENTS]: Rows read: 27. Elaspsed Time: 00:00:00.014s. Throughput: 1922 rows/s.
2019-12-10T06:55:54.122Z [DBWriter][DEPARTMENTS][JSON]: Rows written 27. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.017s. Throughput 1587 rows/s.
2019-12-10T06:55:54.132Z [DBReader][JOB_HISTORY]: Rows read: 10. Elaspsed Time: 00:00:00.008s. Throughput: 1150 rows/s.
2019-12-10T06:55:54.149Z [DBWriter][JOB_HISTORY][JSON]: Rows written 10. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.026s. Throughput 378 rows/s.
2019-12-10T06:55:54.183Z [Yadamu][EXPORT]: Operation completed successfully. Elapsed time: 00:00:09.314.
2019-12-10T06:55:54.207Z [INFO][Export.doExport()]: Operation complete: File:"HR.json". Elapsed Time: 00:00:08.974s.

C:\Development\YADAMU>
~~~
Importing the data in HR.json into the HR1 schema in a postgres database named yadamu running on a server called yadamu-db1
~~~
C:\Development\YADAMU>bin\import rdbms=postgres username=postgres database=yadamu hostname=yadamu-db1 file=HR.json TO_USER=HR1

C:\Development\YADAMU>REM Run from YADAMU_HOME

C:\Development\YADAMU>node C:\Development\YADAMU\app\YADAMU\common\import.js rdbms=postgres username=postgres database=yadamu hostname=yadamu-db1 file=HR.json TO_USER=HR1
Enter password for Postgres connection: ******

2019-12-10T07:02:56.494Z [DBReader][FILE]: Ready. Mode: DATA_ONLY.
2019-12-10T07:02:56.509Z [FileDBI]: Processing file "C:\Development\YADAMU\HR.json". Size 18847 bytes.
2019-12-10T07:02:56.510Z [DBWriter][Postgres]: Ready. Mode: DATA_ONLY.
2019-12-10T07:02:57.605Z [INFO][Client.onNotice()]: notice: schema "HR1" already exists, skipping
2019-12-10T07:02:57.621Z [INFO][PostgresDBI.executeDDL()]: Executed 7 DDL statements. Elapsed time: 00:00:00.016s.
2019-12-10T07:02:57.641Z [DBWriter][JOBS][Batch]: Rows written 19. DB Time: 00:00:00.015s. Elaspsed Time 00:00:00.015s. Throughput 1307 rows/s.
2019-12-10T07:02:57.648Z [DBWriter][REGIONS][Batch]: Rows written 4. DB Time: 00:00:00.006s. Elaspsed Time 00:00:00.004s. Throughput 931 rows/s.
2019-12-10T07:02:57.663Z [DBWriter][COUNTRIES][Batch]: Rows written 25. DB Time: 00:00:00.007s. Elaspsed Time 00:00:00.006s. Throughput 4152 rows/s.
2019-12-10T07:02:57.694Z [DBWriter][EMPLOYEES][Batch]: Rows written 107. DB Time: 00:00:00.021s. Elaspsed Time 00:00:00.029s. Throughput 3706 rows/s.
2019-12-10T07:02:57.712Z [DBWriter][LOCATIONS][Batch]: Rows written 23. DB Time: 00:00:00.009s. Elaspsed Time 00:00:00.007s. Throughput 3344 rows/s.
2019-12-10T07:02:57.725Z [DBWriter][DEPARTMENTS][Batch]: Rows written 27. DB Time: 00:00:00.012s. Elaspsed Time 00:00:00.008s. Throughput 3193 rows/s.
2019-12-10T07:02:57.736Z [DBWriter][JOB_HISTORY][Batch]: Rows written 10. DB Time: 00:00:00.008s. Elaspsed Time 00:00:00.005s. Throughput 1933 rows/s.
2019-12-10T07:02:57.774Z [Yadamu][IMPORT]: Operation completed successfully. Elapsed time: 00:00:03.538.
2019-12-10T07:02:57.864Z [INFO][Import.doImport()]: Operation complete: File:"HR.json". Elapsed Time: 00:00:03.536s.

C:\Development\YADAMU>
~~~
YADAMU can also copy data directly between any of the supported databases. When copying between databases a confgurationf file is used to identify the source and target for the copy operation. This prevents the command line from becoming overly complex.

The following shows a simple configuration file that can be used to copy the contents of the Northwind database from SQL Server into MySQL. The file defines two named connections, "sourceDB" and "targetDB". It also defines two named schemas, "sourceSchema" and "targetSchema". It then defines a single job that performs the copy. The JOB consists of a source and a target, which are defined in terms of the named connections and schemas.
~~~json
{
  "connections"                       : {
    "sourceDB"                        : {
      "mssql"                         : {  
        "user"                        : "sa"
      , "server"                      : "yadamu-db1"
      , "database"                    : "Northwind"
      }
    }
  , "targetDB"                        : {
      "mysql"                         : {
        "user"                        : "root"
      , "host"                        : "yadamu-db1"
      , "database"                    : "sys"
	    }
    }
  }
, "schemas"                           : {
    "sourceSchema"                    : {
      "database"                      : "Northwind"
    , "owner"                         : "dbo"
    }
  , "targetSchema"                    : {
      "schema"                        : "Northwind1"
    }       
  }
, "parameters"                        : {
    "MODE"                            : "DATA_ONLY"
  }
, "jobs"                              : [{
    "source"                          : {
      "connection"                    : "sourceDB"
    , "schema"                        : "sourceSchema"
    }
  , "target"                          : {
      "connection"                    : "targetDB"
    , "schema"                        : "targetSchema"
    }
  }]
}
~~~
It is used with the copy command as shown below
~~~
C:\Development\YADAMU>bin\copy CONFIG=qa\regression\sampleJob.json

C:\Development\YADAMU>REM Run from YADAMU_HOME

C:\Development\YADAMU>node C:\Development\YADAMU\app\YADAMU\common\copy.js CONFIG=qa\regression\sampleJob.json
Enter password for MSSQLSERVER connection: ********

Enter password for MySQL connection: ********

2019-12-10T08:16:50.488Z [DBReader][MSSQLSERVER]: Ready. Mode: DATA_ONLY.
2019-12-10T08:16:50.489Z [DBWriter][MySQL]: Ready. Mode: DATA_ONLY.
2019-12-10T08:16:50.698Z [INFO][MySQLDBI.executeDDL()]: Executed 13 DDL statements. Elapsed time: 00:00:00.006s.
2019-12-10T08:16:50.710Z [DBReader][Categories]: Rows read: 8. Elaspsed Time: 00:00:00.155s. Throughput: 52 rows/s.
2019-12-10T08:16:50.798Z [DBWriter][Categories][Batch]: Rows written 8. DB Time: 00:00:00.086s. Elaspsed Time 00:00:00.090s. Throughput 89 rows/s.
2019-12-10T08:16:50.807Z [DBReader][CustomerCustomerDemo]: Rows read: 0. Elaspsed Time: 00:00:00.095s. Throughput: 0 rows/s.
2019-12-10T08:16:50.808Z [DBWriter][CustomerCustomerDemo][Batch]: Rows written 0. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.000s. Throughput N/A rows/s.
2019-12-10T08:16:50.813Z [DBReader][CustomerDemographics]: Rows read: 0. Elaspsed Time: 00:00:00.005s. Throughput: 0 rows/s.
2019-12-10T08:16:50.823Z [DBWriter][CustomerDemographics][Batch]: Rows written 0. DB Time: 00:00:00.000s. Elaspsed Time 00:00:00.000s. Throughput N/A rows/s.
2019-12-10T08:16:50.845Z [DBReader][Customers]: Rows read: 91. Elaspsed Time: 00:00:00.022s. Throughput: 4132 rows/s.
2019-12-10T08:16:50.877Z [DBWriter][Customers][Batch]: Rows written 91. DB Time: 00:00:00.031s. Elaspsed Time 00:00:00.052s. Throughput 1764 rows/s.
2019-12-10T08:16:50.887Z [DBReader][Employees]: Rows read: 9. Elaspsed Time: 00:00:00.041s. Throughput: 216 rows/s.
2019-12-10T08:16:50.968Z [DBWriter][Employees][Batch]: Rows written 9. DB Time: 00:00:00.080s. Elaspsed Time 00:00:00.081s. Throughput 112 rows/s.
2019-12-10T08:16:50.976Z [DBReader][EmployeeTerritories]: Rows read: 49. Elaspsed Time: 00:00:00.087s. Throughput: 559 rows/s.
2019-12-10T08:16:51.049Z [DBWriter][EmployeeTerritories][Batch]: Rows written 49. DB Time: 00:00:00.072s. Elaspsed Time 00:00:00.073s. Throughput 667 rows/s.
2019-12-10T08:16:51.104Z [DBReader][Order Details]: Rows read: 2155. Elaspsed Time: 00:00:00.127s. Throughput: 16899 rows/s.
2019-12-10T08:16:51.200Z [DBWriter][Order Details][Batch]: Rows written 2155. DB Time: 00:00:00.087s. Elaspsed Time 00:00:00.141s. Throughput 15236 rows/s.
2019-12-10T08:16:51.229Z [DBReader][Orders]: Rows read: 830. Elaspsed Time: 00:00:00.116s. Throughput: 7131 rows/s.
2019-12-10T08:16:51.301Z [DBWriter][Orders][Batch]: Rows written 830. DB Time: 00:00:00.066s. Elaspsed Time 00:00:00.090s. Throughput 9241 rows/s.
2019-12-10T08:16:51.307Z [DBReader][Products]: Rows read: 77. Elaspsed Time: 00:00:00.071s. Throughput: 1071 rows/s.
2019-12-10T08:16:51.329Z [DBWriter][Products][Batch]: Rows written 77. DB Time: 00:00:00.021s. Elaspsed Time 00:00:00.021s. Throughput 3695 rows/s.
2019-12-10T08:16:51.332Z [DBReader][Region]: Rows read: 4. Elaspsed Time: 00:00:00.024s. Throughput: 163 rows/s.
2019-12-10T08:16:51.359Z [DBWriter][Region][Batch]: Rows written 4. DB Time: 00:00:00.025s. Elaspsed Time 00:00:00.026s. Throughput 154 rows/s.
2019-12-10T08:16:51.371Z [DBReader][Shippers]: Rows read: 3. Elaspsed Time: 00:00:00.037s. Throughput: 79 rows/s.
2019-12-10T08:16:51.399Z [DBWriter][Shippers][Batch]: Rows written 3. DB Time: 00:00:00.027s. Elaspsed Time 00:00:00.026s. Throughput 115 rows/s.
2019-12-10T08:16:51.400Z [DBReader][Suppliers]: Rows read: 29. Elaspsed Time: 00:00:00.028s. Throughput: 1031 rows/s.
2019-12-10T08:16:51.439Z [DBWriter][Suppliers][Batch]: Rows written 29. DB Time: 00:00:00.019s. Elaspsed Time 00:00:00.037s. Throughput 790 rows/s.
2019-12-10T08:16:51.453Z [DBReader][Territories]: Rows read: 53. Elaspsed Time: 00:00:00.033s. Throughput: 1581 rows/s.
2019-12-10T08:16:51.466Z [DBWriter][Territories][Batch]: Rows written 53. DB Time: 00:00:00.011s. Elaspsed Time 00:00:00.009s. Throughput 5628 rows/s.
2019-12-10T08:16:51.480Z [Yadamu][COPY]: Operation completed successfully. Elapsed time: 00:00:09.373.
2019-12-10T08:16:51.481Z [INFO][Copy.doCopy()]: Operation complete. Source:["sourceDB"://"Northwind"."dbo"]. Target:["targetDB"://"Northwind1"].
2019-12-10T08:16:51.482Z [INFO][Copy.doCopy()]: Operation complete: Configuration:"qa\regression\sampleJob.json". Elapsed Time: 00:00:09.376s.

C:\Development\YADAMU>
~~~
YADAMU includes a simple Graphical User Interface (GUI) that allows you to peform YADAMU operations without mastering the command line interface. The GUI can also be used to create and edit YADAMU configuration files. You can learn more about YADAMU's GUI here.
