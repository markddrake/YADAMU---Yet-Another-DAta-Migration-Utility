--
create or replace package JSON_IMPORT
AUTHID CURRENT_USER
as
  C_VERSION_NUMBER constant NUMBER(4,2) := 1.0;
--  
  TYPE T_SQL_OPERATION_REC is RECORD (
    OWNER           VARCHAR2(128)
   ,TABLE_NAME      VARCHAR2(128)
   ,SQL_STATEMENT   CLOB
   ,SQLCODE         NUMBER
   ,RESULT          VARCHAR2(4000)
   ,STATUS          VARCHAR2(4000)
  );
   
  TYPE T_SQL_OPERATIONS_TAB is TABLE of T_SQL_OPERATION_REC;

  SQL_OPERATIONS_TABLE   T_SQL_OPERATIONS_TAB; 
  
  C_SUCCESS          CONSTANT VARCHAR2(32) := 'SUCCESS';
  C_FATAL_ERROR      CONSTANT VARCHAR2(32) := 'FATAL';
  C_WARNING          CONSTANT VARCHAR2(32) := 'WARNING';
  C_IGNOREABLE       CONSTANT VARCHAR2(32) := 'IGNORE';

  function IMPORT_VERSION return NUMBER deterministic;

  procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY CLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'));
  function IMPORT_DML_LOG return T_SQL_OPERATIONS_TAB pipelined;

end;
/
show errors
--
create or replace package body JSON_IMPORT 
as
-- 
  C_NEWLINE         CONSTANT CHAR(1) := CHR(10);
  C_SINGLE_QUOTE    CONSTANT CHAR(1) := CHR(39);
--
function IMPORT_VERSION return NUMBER deterministic
as
begin
  return C_VERSION_NUMBER;
end;
--
function IMPORT_DML_LOG return T_SQL_OPERATIONS_TAB pipelined
as
  cursor getRecords
  is
  select *
    from TABLE(SQL_OPERATIONS_TABLE);
begin
  for r in getRecords loop
    pipe row (r);
  end loop;
end;
--
procedure SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2)
as
  USER_NOT_FOUND EXCEPTION ; PRAGMA EXCEPTION_INIT( USER_NOT_FOUND , -01435 );
  V_SQL_STATEMENT CONSTANT VARCHAR2(4000) := 'ALTER SESSION SET CURRENT_SCHEMA = ' || P_TARGET_SCHEMA;
begin
  if (SYS_CONTEXT('USERENV','CURRENT_SCHEMA') <> P_TARGET_SCHEMA) then
    execute immediate V_SQL_STATEMENT;
  end if;
end;
--
function GENERATE_DML_STATEMENTS(P_JSON_DUMP_FILE IN OUT NOCOPY CLOB)
return T_SQL_OPERATIONS_TAB
as
  V_SQL_OPERATIONS T_SQL_OPERATIONS_TAB;
begin
  select OWNER
        ,TABLE_NAME
        ,'insert into "' || TABLE_NAME ||'"(' || SELECT_LIST || ')' || C_NEWLINE ||
		 'select ' || SELECT_LIST || C_NEWLINE ||
		 '  from JSON_TABLE(' || C_NEWLINE ||
	     '         :JSON,' || C_NEWLINE ||
		 '         ''$.data."' || TABLE_NAME || '"[*]''' || C_NEWLINE ||
		 '         COLUMNS(' || C_NEWLINE ||  COLUMN_PATTERNS || C_NEWLINE || '))' 
	    ,NULL
	    ,NULL
		,NULL
	bulk collect into V_SQL_OPERATIONS
    from JSON_TABLE(
	        P_JSON_DUMP_FILE,
			'$.metadata.*' ERROR ON ERROR
			COLUMNS (
			  OWNER       VARCHAR2(128) PATH '$.owner'
			, TABLE_NAME  VARCHAR2(128) PATH '$.tableName'
            $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN   
			,  SELECT_LIST         CLOB PATH '$.columns'
			,  DATA_TYPES          CLOB PATH '$.dataTypes'
			,  COLUMN_PATTERNS     CLOB PATH '$.columnPatterns'
            $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
			,  SELECT_LIST         VARCHAR2(32767) PATH '$.columns'
			,  DATA_TYPES          VARCHAR2(32767) PATH '$.dataTypes'
			,  COLUMN_PATTERNS     VARCHAR2(32767) PATH '$.columnPatterns'
			$ELSE
			,  SELECT_LIST         VARCHAR2(4000) PATH '$.columns'
			,  DATA_TYPES          VARCHAR2(4000) PATH '$.dataTypes'
			,  COLUMN_PATTERNS     VARCHAR2(4000) PATH '$.columnPatterns'
			$END
			)
		  );
    return V_SQL_OPERATIONS;
end;
--
procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY CLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
as
  V_CURRENT_SCHEMA           CONSTANT VARCHAR2(128) := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');
  
  V_START_TIME TIMESTAMP(6);
  V_END_TIME   TIMESTAMP(6);
begin
  SET_CURRENT_SCHEMA(P_TARGET_SCHEMA);
  
  SQL_OPERATIONS_TABLE := GENERATE_DML_STATEMENTS(P_JSON_DUMP_FILE);
							
  for i in 1 .. SQL_OPERATIONS_TABLE.count loop
    begin
      V_START_TIME := SYSTIMESTAMP;
      execute immediate SQL_OPERATIONS_TABLE(i).SQL_STATEMENT using P_JSON_DUMP_FILE;  
 	  SQL_OPERATIONS_TABLE(i).RESULT := 'Operation completed succecssfully at ' || SYS_EXTRACT_UTC(SYSTIMESTAMP) || '. Processed ' || TO_CHAR(SQL%ROWCOUNT)|| ' rows. Elapsed time: ' || (V_END_TIME - V_START_TIME) || '.';
	  commit;
	  SQL_OPERATIONS_TABLE(i).STATUS := C_SUCCESS;
    exception
      when others then
	    SQL_OPERATIONS_TABLE(i).RESULT := DBMS_UTILITY.format_error_stack;
		SQL_OPERATIONS_TABLE(i).STATUS := C_FATAL_ERROR;
	end;
  end loop;
  SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
exception
  when OTHERS then
    SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
	RAISE;
end;
--
end;
/
show errors
--