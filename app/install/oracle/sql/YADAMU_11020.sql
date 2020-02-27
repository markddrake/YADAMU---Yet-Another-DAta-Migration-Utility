--
create or replace type TABLE_INFO_RECORD_GT as OBJECT (
  OWNER	    VARCHAR2(128)
 ,TABLE_NAME VARCHAR2(128)
)
/
--
show errors
--
create or replace public synonym TABLE_INFO_RECORD_GT for TABLE_INFO_RECORD_GT
/  
--
create or replace type TABLE_INFO_TABLE_GT is TABLE of TABLE_INFO_RECORD_GT
/
--
show errors
--
create or replace public synonym TABLE_INFO_TABLE_GT for TABLE_INFO_TABLE_GT
/  
--