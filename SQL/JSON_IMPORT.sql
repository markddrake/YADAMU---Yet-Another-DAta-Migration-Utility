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
  );
   
  TYPE T_SQL_OPERATIONS_TAB is TABLE of T_SQL_OPERATION_REC;

  SQL_OPERATIONS_TABLE   T_SQL_OPERATIONS_TAB; 
  
  function VERSION return NUMBER deterministic;
  procedure DATA_ONLY_MODE(P_DATA_ONLY_MODE BOOLEAN);
  procedure DDL_ONLY_MODE(P_DDL_ONLY_MODE BOOLEAN);

  procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY CLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'));
  function SQL_OPERATIONS return T_SQL_OPERATIONS_TAB pipelined;

end;
/
show errors
--
create or replace package body JSON_IMPORT 
as
-- 
  C_NEWLINE         CONSTANT CHAR(1) := CHR(10);
  C_SINGLE_QUOTE    CONSTANT CHAR(1) := CHR(39);
  
  G_INCLUDE_DATA    BOOLEAN := TRUE;
  G_INCLUDE_DDL     BOOLEAN := FALSE;
--
function VERSION 
return NUMBER deterministic
as
begin
  return C_VERSION_NUMBER;
end;
--
procedure DATA_ONLY_MODE(P_DATA_ONLY_MODE BOOLEAN)
as
begin
  if (P_DATA_ONLY_MODE) then
	G_INCLUDE_DDL := false;
  else
	G_INCLUDE_DDL := true;
  end if;
end;
--
procedure DDL_ONLY_MODE(P_DDL_ONLY_MODE BOOLEAN)
as
begin
  if (P_DDL_ONLY_MODE) then
	G_INCLUDE_DATA := false;
  else
	G_INCLUDE_DATA := true;
  end if;
end;
--
function SQL_OPERATIONS
return T_SQL_OPERATIONS_TAB
pipelined
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
        ,'insert ' ||
		case 
		  when DESERIALIZER is not NULL
		    then '/*+ WITH_PLSQL */ '
		    else ''
		end
		|| 'into "' || TABLE_NAME ||'"(' || COLUMN_LIST || ')' || C_NEWLINE ||
		case 
		  when DESERIALIZER is not NULL
		    then 'WITH' || C_NEWLINE || DESERIALIZER
			$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
			else TO_CLOB('')
			$ELSE
			else ''
			$END
		 end ||
		 'select ' || SELECT_LIST || C_NEWLINE ||
		 '  from JSON_TABLE(' || C_NEWLINE ||
	     '         :JSON,' || C_NEWLINE ||
		 '         ''$.data."' || TABLE_NAME || '"[*]''' || C_NEWLINE ||
		 '         COLUMNS(' || C_NEWLINE ||  COLUMN_PATTERNS || C_NEWLINE || '))' 
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
			,  COLUMN_LIST         CLOB PATH '$.columns'
			,  DATA_TYPES          CLOB PATH '$.dataTypes'
			,  COLUMN_PATTERNS     CLOB PATH '$.columnPatterns'
			,  SELECT_LIST         CLOB PATH '$.importSelectList'
			,  DESERIALIZER        CLOB PATH '$.deserializer'
            $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
			,  COLUMN_LIST         VARCHAR2(32767) PATH '$.columns'
			,  DATA_TYPES          VARCHAR2(32767) PATH '$.dataTypes'
			,  COLUMN_PATTERNS     VARCHAR2(32767) PATH '$.columnPatterns'
			,  SELECT_LIST         VARCHAR2(32767) PATH '$.importSelectList'
			,  DESERIALIZER        VARCHAR2(32767) PATH '$.deserializer'
			$ELSE
			,  COLUMN_LIST         VARCHAR2(4000) PATH '$.columns'
			,  DATA_TYPES          VARCHAR2(4000) PATH '$.dataTypes'
			,  COLUMN_PATTERNS     VARCHAR2(4000) PATH '$.columnPatterns'
			,  SELECT_LIST         VARCHAR2(4000) PATH '$.importSelectList'
			,  DESERIALIZER        VARCHAR2(4000) PATH '$.deserializer'
			$END
			)
		  );
    return V_SQL_OPERATIONS;
end;
--
function GENERATE_DISABLE_CONSTRAINT_DDL(P_TARGET_SCHEMA VARCHAR2)
return T_SQL_OPERATIONS_TAB
as
  V_SQL_OPERATIONS T_SQL_OPERATIONS_TAB;
begin
  select OWNER
        ,TABLE_NAME
		,'ALTER TABLE "' || P_TARGET_SCHEMA || '"."' || TABLE_NAME  || '" DISABLE CONSTRAINT "' || CONSTRAINT_NAME || '"'
	    ,NULL
	    ,NULL
    bulk collect into V_SQL_OPERATIONS
    from ALL_CONSTRAINTS
   where OWNER = P_TARGET_SCHEMA 
	 AND constraint_type = 'R';   
    return V_SQL_OPERATIONS;
end;
--
function GENERATE_ENABLE_CONSTRAINT_DDL(P_TARGET_SCHEMA VARCHAR2)
return T_SQL_OPERATIONS_TAB
as
  V_SQL_OPERATIONS T_SQL_OPERATIONS_TAB;
begin
  select OWNER
        ,TABLE_NAME
		,'ALTER TABLE "' || P_TARGET_SCHEMA || '"."' || TABLE_NAME  || '" ENABLE CONSTRAINT "' || CONSTRAINT_NAME || '"' 
		,NULL
		,NULL
    bulk collect into V_SQL_OPERATIONS
    from ALL_CONSTRAINTS
   where OWNER = P_TARGET_SCHEMA 
	 AND constraint_type = 'R';   
    return V_SQL_OPERATIONS;
end;
--
procedure REFRESH_MATERIALIZED_VIEWS(P_TARGET_SCHEMA VARCHAR2)
as
  V_MVIEW_COUNT NUMBER;
  V_MVIEW_LIST  VARCHAR2(32767);
begin
  select COUNT(*), LISTAGG('"' || MVIEW_NAME || '"',',') WITHIN GROUP (ORDER BY MVIEW_NAME) 
    into V_MVIEW_COUNT, V_MVIEW_LIST
    from ALL_MVIEWS
   where OWNER = P_TARGET_SCHEMA;
   
  if (V_MVIEW_COUNT > 0) then
    DBMS_MVIEW.REFRESH(V_MVIEW_LIST);
  end if;
end;
--
procedure MANAGE_MUTATING_TABLE(P_SQL_OPERATION IN OUT NOCOPY T_SQL_OPERATION_REC, P_JSON_DUMP_FILE IN OUT NOCOPY CLOB)
as
  V_SQL_STATEMENT         CLOB;
  V_SQL_FRAGMENT          VARCHAR2(1024);
  V_JSON_TABLE_OFFSET     NUMBER;

  V_START_TIME   TIMESTAMP(6);
  V_END_TIME     TIMESTAMP(6);
  V_ROW_COUNT    NUMBER;
begin
   V_SQL_FRAGMENT := 'declare' || C_NEWLINE
                  || '  cursor JSON_TO_RELATIONAL' || C_NEWLINE
				  || '  is' || C_NEWLINE
				  || '  select *' || C_NEWLINE
				  || '    from ';
			     
   DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);	
   DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
   V_JSON_TABLE_OFFSET := DBMS_LOB.INSTR(P_SQL_OPERATION.SQL_STATEMENT,' JSON_TABLE(');
   DBMS_LOB.COPY(V_SQL_STATEMENT,P_SQL_OPERATION.SQL_STATEMENT,((DBMS_LOB.GETLENGTH(P_SQL_OPERATION.SQL_STATEMENT)-V_JSON_TABLE_OFFSET)+1),DBMS_LOB.GETLENGTH(V_SQL_STATEMENT)+1,V_JSON_TABLE_OFFSET);
 

   V_SQL_FRAGMENT := ';' || C_NEWLINE
                  || '  type T_JSON_TABLE_ROW_TAB is TABLE of JSON_TO_RELATIONAL%ROWTYPE index by PLS_INTEGER;' || C_NEWLINE
				  || '  V_ROW_BUFFER T_JSON_TABLE_ROW_TAB;' || C_NEWLINE
				  || '  V_ROW_COUNT PLS_INTEGER := 0;' || C_NEWLINE
				  || 'begin' || C_NEWLINE
				  || '  open JSON_TO_RELATIONAL;' || C_NEWLINE
				  || '  loop' || C_NEWLINE
				  || '    fetch JSON_TO_RELATIONAL' || C_NEWLINE
				  || '    bulk collect into V_ROW_BUFFER LIMIT 25000;' || C_NEWLINE
				  || '    exit when V_ROW_BUFFER.count = 0;' || C_NEWLINE
				  || '    V_ROW_COUNT := V_ROW_COUNT + V_ROW_BUFFER.count;' || C_NEWLINE
				  -- || '    forall i in 1 .. V_ROW_BUFFER.count' || C_NEWLINE
				  || '    for i in 1 .. V_ROW_BUFFER.count loop' || C_NEWLINE
				  || '      insert into "' || P_SQL_OPERATION.TABLE_NAME || '"' || C_NEWLINE
				  || '      values V_ROW_BUFFER(i);'|| C_NEWLINE
				  || '    end loop;'|| C_NEWLINE
				  || '    commit;' || C_NEWLINE
				  || '  end loop;' || C_NEWLINE
				  || '  :2 := V_ROW_COUNT;' || C_NEWLINE
                  || 'end;' || C_NEWLINE;

   DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
   P_SQL_OPERATION.SQL_STATEMENT := V_SQL_STATEMENT;

   V_START_TIME := SYSTIMESTAMP;
   execute immediate P_SQL_OPERATION.SQL_STATEMENT using P_JSON_DUMP_FILE, out V_ROW_COUNT;
   V_END_TIME := SYSTIMESTAMP;		
   P_SQL_OPERATION.RESULT := 'Operation completed succecssfully. Processed ' || V_ROW_COUNT || ' rows. Elapsed time: ' || (V_END_TIME - V_START_TIME) || '.';
   
exception
  when OTHERS then
	P_SQL_OPERATION.RESULT := DBMS_UTILITY.format_error_stack;	   
end;
--
procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY CLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
as
  V_CURRENT_SCHEMA           CONSTANT VARCHAR2(128) := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');
  
  MUTATING_TABLE EXCEPTION ; PRAGMA EXCEPTION_INIT( MUTATING_TABLE , -04091 );
  
  V_START_TIME TIMESTAMP(6);
  V_END_TIME   TIMESTAMP(6);
begin
  SET_CURRENT_SCHEMA(P_TARGET_SCHEMA);
  
  if (G_INCLUDE_DDL) then
    JSON_EXPORT_DDL.APPLY_DDL_STATEMENTS(P_JSON_DUMP_FILE,P_TARGET_SCHEMA);
  end if;

  if (G_INCLUDE_DATA) then


    SQL_OPERATIONS_TABLE := GENERATE_DISABLE_CONSTRAINT_DDL(P_TARGET_SCHEMA) 
	                        MULTISET UNION ALL
							GENERATE_DML_STATEMENTS(P_JSON_DUMP_FILE)
							MULTISET UNION ALL
							GENERATE_ENABLE_CONSTRAINT_DDL(P_TARGET_SCHEMA);
							
	DBMS_OUTPUT.put_line('STATEMENT COUNT:' || 	SQL_OPERATIONS_TABLE.count);
								
	for i in 1 .. SQL_OPERATIONS_TABLE.count loop
      begin
	    if instr(SQL_OPERATIONS_TABLE(i).SQL_STATEMENT,'ALTER') = 1 then
		  V_START_TIME := SYSTIMESTAMP;
          execute immediate SQL_OPERATIONS_TABLE(i).SQL_STATEMENT; 
		  V_END_TIME := SYSTIMESTAMP;		
		  SQL_OPERATIONS_TABLE(i).RESULT := 'Operation completed succecssfully. Elapsed time: ' || (V_END_TIME - V_START_TIME) || '.';
		else
		  V_START_TIME := SYSTIMESTAMP;
          execute immediate SQL_OPERATIONS_TABLE(i).SQL_STATEMENT using P_JSON_DUMP_FILE;  
		  SQL_OPERATIONS_TABLE(i).RESULT := 'Operation completed succecssfully. Processed ' || TO_CHAR(SQL%ROWCOUNT)|| ' rows. Elapsed time: ' || (V_END_TIME - V_START_TIME) || '.';
		  commit;
		end if;
	  exception
        when MUTATING_TABLE then
           MANAGE_MUTATING_TABLE(SQL_OPERATIONS_TABLE(i),P_JSON_DUMP_FILE);		
		when others then
		  SQL_OPERATIONS_TABLE(i).RESULT := DBMS_UTILITY.format_error_stack;
	  end;
    end loop;
	REFRESH_MATERIALIZED_VIEWS(P_TARGET_SCHEMA);
  end if;
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