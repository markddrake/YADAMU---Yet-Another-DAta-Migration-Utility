# Installing Yadamu Database support for Oracle

Packages are installed using the script file src/install/oracle/sql/COMPILE_ALL.sql.
Packages should be installed by a DBA. Yadamu creates public synonymns for the installed packages.
The packages use conditional compilation to manage version specific dependancies.
The script takes one parameter, which is the folder where the installation log files will be written

```bat
sqlplus system@YDB11903 @src/install/oracle/sql/COMPILE_ALL.sql /tmp
```

```
SQL*Plus: Release 12.2.0.1.0 Production on Mon Mar 15 01:54:10 2021

Copyright (c) 1982, 2016, Oracle.  All rights reserved.

Enter password

Connected to:
Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production

SQL> def LOGDIR = &1
SQL> spool &LOGDIR/COMPILE_ALL.log
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> set define off
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> set define on
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> --
SQL> show errors
No errors.
SQL> --
SQL> @@SET_TERMOUT
SQL> SET TERMOUT OFF
SQL> set serveroutput on
SQL> --
SQL> set linesize 128
SQL> --
SQL> begin
  2    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.YADAMU_INSTANCE_ID:            ' || YADAMU_FEATURE_DETECTION.YADAMU_INSTANCE_ID);
  3    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.YADAMU_INSTALLATION_TIMESTAMP: ' || YADAMU_FEATURE_DETECTION.YADAMU_INSTALLATION_TIMESTAMP);
  4    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.SPATIAL_INSTALLED:             ' || case when YADAMU_FEATURE_DETECTION.SPATIAL_INSTALLED then 'TRUE' else 'FALSE' end);
  5    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED:        ' || case when YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED then 'TRUE' else 'FALSE' end);
  6    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED:        ' || case when YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED then 'TRUE' else 'FALSE' end);
  7    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED:                ' || case when YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED  then 'TRUE' else 'FALSE' end);
  8    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED:        ' || case when YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED       then 'TRUE' else 'FALSE' end);
  9    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED:       ' || case when YADAMU_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED then 'TRUE' else 'FALSE' end);
 10    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.OBJECTS_AS_JSON                ' || case when YADAMU_FEATURE_DETECTION.OBJECTS_AS_JSON then 'TRUE' else 'FALSE' end);
 11    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED: ' || case when YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED then 'TRUE' else 'FALSE' end);
 12    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.XMLSCHEMA_SUPPORTED:           ' || case when YADAMU_FEATURE_DETECTION.XMLSCHEMA_SUPPORTED  then 'TRUE' else 'FALSE' end);
 13    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.COLLECT_PLSQL_SUPPORTED:       ' || case when YADAMU_FEATURE_DETECTION.COLLECT_PLSQL_SUPPORTED  then 'TRUE' else 'FALSE' end);
 14    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.ORACLE_MANAGED_SERVICE:        ' || case when YADAMU_FEATURE_DETECTION.ORACLE_MANAGED_SERVICE  then 'TRUE' else 'FALSE' end);
 15    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.C_RETURN_TYPE:                 ' || YADAMU_FEATURE_DETECTION.C_RETURN_TYPE);
 16    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE:             ' || YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE);
 17    DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.XML_STORAGE_MODEL:             ' || YADAMU_FEATURE_DETECTION.XML_STORAGE_MODEL);
 18  end;
 19  /
YADAMU_FEATURE_DETECTION.YADAMU_INSTANCE_ID:            BD8A895C-88E3-4130-E053-040011AC873C
YADAMU_FEATURE_DETECTION.YADAMU_INSTALLATION_TIMESTAMP: 2021-03-15T01:54:56-+00:00
YADAMU_FEATURE_DETECTION.SPATIAL_INSTALLED:             TRUE
YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED:        TRUE
YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED:     TRUE
YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED:                TRUE
YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED:     TRUE
YADAMU_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED:       TRUE
YADAMU_FEATURE_DETECTION.OBJECTS_AS_JSON                TRUE
YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED:      FALSE
YADAMU_FEATURE_DETECTION.XMLSCHEMA_SUPPORTED:           TRUE
YADAMU_FEATURE_DETECTION.COLLECT_PLSQL_SUPPORTED:       FALSE
YADAMU_FEATURE_DETECTION.ORACLE_MANAGED_SERVICE:        FALSE
YADAMU_FEATURE_DETECTION.C_RETURN_TYPE:                 CLOB
YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE:             18446744073709551615
YADAMU_FEATURE_DETECTION.XML_STORAGE_MODEL:             BINARY

PL/SQL procedure successfully completed.

Elapsed: 00:00:00.04
SQL> spool off
SQL> --
SQL> exit
Disconnected from Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production
```

Note that the final output is dependant on the version of Oracle you are targetting and features and options installed.