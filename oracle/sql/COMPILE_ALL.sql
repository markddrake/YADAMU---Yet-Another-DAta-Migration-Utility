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
grant execute on JSON_FEATURE_DETECTION to public
/
spool &LOGDIR/install/YADAMU_UTILITIES.log
--
@@YADAMU_UTILITIES.sql
--
create or replace public synonym YADAMU_UTILITIES for YADAMU_UTILITIES
/
spool &LOGDIR/install/OBJECT_SERIALIZATION.log
--
grant execute on YADAMU_UTILITIES to public
/
@@OBJECT_SERIALIZATION.sql
--
create or replace public synonym OBJECT_SERIALIZATION for OBJECT_SERIALIZATION
/
grant execute on OBJECT_SERIALIZATION to public
/
spool &LOGDIR/install/YADAMU_EXPORT_DDL.log
--
@@YADAMU_EXPORT_DDL.sql
--
create or replace public synonym YADAMU_EXPORT_DDL for YADAMU_EXPORT_DDL
/
spool &LOGDIR/install/YADAMU_IMPORT.log
--
@@YADAMU_IMPORT.sql
--
create or replace public synonym YADAMU_IMPORT for YADAMU_IMPORT
/
spool &LOGDIR/install/YADAMU_EXPORT.log
--
@@YADAMU_EXPORT.sql
--
create or replace public synonym YADAMU_EXPORT for YADAMU_EXPORT
/
spool &LOGDIR/install/COMPILE_ALL.log APPEND
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
begin
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.PARSING_SUPPORTED:         ' || case when JSON_FEATURE_DETECTION.PARSING_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.GENERATION_SUPPORTED:      ' || case when JSON_FEATURE_DETECTION.GENERATION_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED:   ' || case when JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.CLOB_SUPPORTED:            ' || case when JSON_FEATURE_DETECTION.CLOB_SUPPORTED  then 'TRUE' else 'FALSE' end);
  DBMS_OUTPUT.put_line('JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED: ' || case when JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED  then 'TRUE' else 'FALSE' end);
end;
/
spool off
--
exit
