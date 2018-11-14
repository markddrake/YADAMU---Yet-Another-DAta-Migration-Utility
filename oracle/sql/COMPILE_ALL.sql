set echo on
def LOGDIR = &1
spool &LOGDIR/install/COMPILE_ALL.log
--
@@SET_TERMOUT
--
set timing on
set feedback on
set echo on
--
ALTER SESSION SET PLSQL_CCFLAGS = 'DEBUG:TRUE'
/
set serveroutput on
--
create or replace TYPE CHUNKED_CLOB_T is TABLE of VARCHAR2(4000);
/
create or replace public synonym CHUNKED_CLOB_T for CHUNKED_CLOB_T
/
create or replace type T_VC4000_TABLE is TABLE of VARCHAR2(4000)
/
create or replace public synonym T_VC4000_TABLE for T_VC4000_TABLE
/  
spool &LOGDIR/install/JSON_FEATURE_DETECTION.log
--
@@JSON_FEATURE_DETECTION.sql
--
create or replace public synonym JSON_FEATURE_DETECTION for JSON_FEATURE_DETECTION
/
spool &LOGDIR/install/OBJECT_SERIALIZATION.log
--
@@OBJECT_SERIALIZATION
--
create or replace public synonym OBJECT_SERIALIZATION for OBJECT_SERIALIZATION
/
spool &LOGDIR/install/JSON_EXPORT_DDL.log
--
@@JSON_EXPORT_DDL
--
create or replace public synonym JSON_EXPORT_DDL for JSON_EXPORT_DDL
/
spool &LOGDIR/install/JSON_IMPORT.log
--
@@JSON_IMPORT
--
create or replace public synonym JSON_IMPORT for JSON_IMPORT
/
spool &LOGDIR/install/JSON_EXPORT.log
--
@@JSON_EXPORT
--
create or replace public synonym JSON_EXPORT for JSON_EXPORT
/
spool &LOGDIR/install/COMPILE_ALL.log APPEND
--
desc OBJECT_SERIALIZATION
--
desc JSON_EXPORT_DDL
--
desc JSON_IMPORT
--
desc JSON_EXPORT_PLSQL
--
desc JSON_EXPORT
--
set TERMOUT ON
set serveroutput on
--
begin
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED:   ' || case when JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.CLOB_SUPPORTED:            ' || case when JSON_FEATURE_DETECTION.CLOB_SUPPORTED  then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED: ' || case when JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED  then 'TRUE' else 'FALSE' end);
end;
/
spool off
--
exit
