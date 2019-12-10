# YADAMU - Yet Another Data Migration Utility

This project has morphed in scope from being a simple drop in replacement for Oracle's traditional IMP and EXP utilities.
It now supports homegeneous IMPORT and EXPORT operations on Oracle 12c, Postgres, MySQL and SQL Server, as well as hetrogeneous migrations between any of the supported databases. Hence the new name - YADAMU. 

YADAMU migrates DDL and Content.

For homogeneous Oracle migrations all schema objects supported by DBMS_METADATA are migrated.

For homogeneous Postgres, MySQL and SQL Server migrations, or hetrogeneous migrations DDL operations are currently restriced to table layouts. Migration of indexes and other schema objects is not currently supported.

Details about this project can be found [here](http://markddrake.github.io/YADAMU---Yet-Another-DAta-Migration-Utility/docs)

YADAMU currently supports Oracle Database versions 11.2.x and later, Postgres 11 and later,  MySQL 8.0.12 and SQL Server 14.

Support for other database and old versions of Oracle Postgres, MySQL and SQL Server is under consideration.

Simple Examples of using YADAMU

Pulling data from Oracle HR database and storing in a file called HR.json
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
Copying the data in HR.json into the HR1 schema in a postgres database named yadamu running on a server called yadamu-db1
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
