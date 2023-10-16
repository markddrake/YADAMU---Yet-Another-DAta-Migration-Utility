set echo on
def LOGDIR = &1
spool &LOGDIR/COMPILE_ALL.log
--
@@SET_TERMOUT
--
set timing on
set feedback on
set echo on
--
ALTER SESSION SET NLS_LENGTH_SEMANTICS = 'CHAR' PLSQL_CCFLAGS = 'DEBUG:FALSE'
/
set serveroutput on
--
create or replace type T_VC4000_TABLE is TABLE of VARCHAR2(4000)
/
create or replace public synonym T_VC4000_TABLE for T_VC4000_TABLE
/  
create or replace type T_CLOB_TABLE is TABLE of CLOB
/
create or replace public synonym T_CLOB_TABLE for T_CLOB_TABLE
/  
--
-- Execute Version Specific Code: Note may raise Error SP2-0310 if Version specific code is not required
-- 
@@YADAMU_&_O_RELEASE..sql
--
spool &LOGDIR/YADAMU_FEATURE_DETECTION.log
--
@@YADAMU_FEATURE_DETECTION.sql
--
create or replace public synonym YADAMU_FEATURE_DETECTION for YADAMU_FEATURE_DETECTION
/
grant execute on YADAMU_FEATURE_DETECTION to public
/
spool &LOGDIR/YADAMU_UTILITIES.log
--
@@YADAMU_UTILITIES.sql
--
create or replace public synonym YADAMU_UTILITIES for YADAMU_UTILITIES
/
grant execute on YADAMU_UTILITIES to public
/
spool &LOGDIR/OBJECT_SERIALIZATION.log
--
@@OBJECT_SERIALIZATION.sql
--
create or replace public synonym OBJECT_SERIALIZATION for OBJECT_SERIALIZATION
/
grant execute on OBJECT_SERIALIZATION to public
/
spool &LOGDIR/OBJECT_TO_JSON.log
--
@@OBJECT_TO_JSON.sql
--
create or replace public synonym OBJECT_TO_JSON for OBJECT_TO_JSON
/
grant execute on OBJECT_TO_JSON to public
/
spool &LOGDIR/YADAMU_EXPORT_DDL.log
--
@@YADAMU_EXPORT_DDL.sql
--
create or replace public synonym YADAMU_EXPORT_DDL for YADAMU_EXPORT_DDL
/
spool &LOGDIR/YADAMU_IMPORT.log
--
@@YADAMU_IMPORT.sql
--
create or replace public synonym YADAMU_IMPORT for YADAMU_IMPORT
/
spool &LOGDIR/YADAMU_EXPORT.log
--
@@YADAMU_EXPORT.sql
--
create or replace public synonym YADAMU_EXPORT for YADAMU_EXPORT
/
spool &LOGDIR/COMPILE_ALL.log APPEND
--
desc OBJECT_SERIALIZATION
--
desc YADAMU_EXPORT_DDL
--
desc YADAMU_IMPORT
--
desc YADAMU_EXPORT_PLSQL
--
desc YADAMU_EXPORT
--
set TERMOUT ON
set serveroutput on
--
set linesize 128
--
begin
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.YADAMU_INSTANCE_ID:            ' || YADAMU_FEATURE_DETECTION.YADAMU_INSTANCE_ID);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.YADAMU_INSTALLATION_TIMESTAMP: ' || YADAMU_FEATURE_DETECTION.YADAMU_INSTALLATION_TIMESTAMP);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.SPATIAL_INSTALLED:             ' || case when YADAMU_FEATURE_DETECTION.SPATIAL_INSTALLED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED:        ' || case when YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED:     ' || case when YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED:                ' || case when YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED  then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED:     ' || case when YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED  then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED:       ' || case when YADAMU_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.OBJECTS_AS_JSON                ' || case when YADAMU_FEATURE_DETECTION.OBJECTS_AS_JSON then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED:      ' || case when YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.XMLSCHEMA_SUPPORTED:           ' || case when YADAMU_FEATURE_DETECTION.XMLSCHEMA_SUPPORTED  then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.COLLECT_PLSQL_SUPPORTED:       ' || case when YADAMU_FEATURE_DETECTION.COLLECT_PLSQL_SUPPORTED  then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.ORACLE_MANAGED_SERVICE:        ' || case when YADAMU_FEATURE_DETECTION.ORACLE_MANAGED_SERVICE  then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.C_RETURN_TYPE:                 ' || YADAMU_FEATURE_DETECTION.C_RETURN_TYPE);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE:             ' || YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.XML_STORAGE_MODEL:             ' || YADAMU_FEATURE_DETECTION.XML_STORAGE_MODEL);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.UNICODE_DATABASE:              ' || case when YADAMU_FEATURE_DETECTION.UNICODE_DATABASE then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('YADAMU_FEATURE_DETECTION.DATABASE_VERSION:              ' || YADAMU_FEATURE_DETECTION.DATABASE_VERSION);
end;
/
spool off
--
exit