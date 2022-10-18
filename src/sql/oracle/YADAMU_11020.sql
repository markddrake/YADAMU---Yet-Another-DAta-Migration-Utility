--
create or replace type TABLE_INFO_RECORD as OBJECT (
  OWNER	    VARCHAR2(128)
 ,TABLE_NAME VARCHAR2(128)
)
/
--
show errors
--
create or replace public synonym TABLE_INFO_RECORD for TABLE_INFO_RECORD
/  
--
create or replace type TABLE_INFO_TABLE is TABLE of TABLE_INFO_RECORD
/
--
show errors
--
create or replace type TYPE_LIST as OBJECT (
  OWNER               VARCHAR2(128)
, TYPE_NAME           VARCHAR2(128)
, ATTR_COUNT          NUMBER
, TYPECODE            VARCHAR2(32)
)
/
--
show errors
--
create or replace public synonym TYPE_LIST for TYPE_LIST
/  
--
create or replace type TYPE_LIST_TABLE is TABLE of TYPE_LIST
/
--
show errors
--
create or replace public synonym TYPE_LIST_TABLE for TYPE_LIST_TABLE
/  
--
create or replace type TYPE_MAPPING_RECORD as OBJECT (
    VENDOR_TYPE VARCHAR2(256),
	ORACLE_TYPE VARCHAR2(256)
)
/
--
show errors
--
create or replace public synonym TYPE_MAPPING_RECORD for TYPE_MAPPING_RECORD
/  
--
create or replace type TYPE_MAPPING_TABLE is TABLE of TYPE_MAPPING_RECORD
/
--
show errors
--
create or replace public synonym TYPE_MAPPING_TABLE for TYPE_MAPPING_TABLE
/  
--