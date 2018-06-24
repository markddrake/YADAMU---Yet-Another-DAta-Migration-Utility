--
create or replace type T_VC4000_TABLE is TABLE of VARCHAR2(4000)
/
--
create or replace package JSON_EXPORT
authid CURRENT_USER
as
  SQL_STATEMENT CLOB;
  function DUMP_SQL_STATEMENT return CLOB;
  
  function EXPORT_SCHEMA(P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) return CLOB;
end;
/
--
show errors 
--
create or replace package body JSON_EXPORT
as
--
  C_NEWLINE         CONSTANT CHAR(1) := CHR(10);
  C_SINGLE_QUOTE    CONSTANT CHAR(1) := CHR(39);
--
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  C_RETURN_TYPE CONSTANT VARCHAR2(32) := 'CLOB';
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  C_RETURN_TYPE CONSTANT VARCHAR2(32):= 'VARCHAR2(32767)';
  $ELSE
  C_RETURN_TYPE CONSTANT VARCHAR2(32):= 'VARCHAR2(4000)';
  $END  
--
function DUMP_SQL_STATEMENT
return CLOB
as
begin
  return SQL_STATEMENT;
end;
--
function TABLE_TO_LIST(P_TABLE T_VC4000_TABLE, P_DELIMITER VARCHAR2 DEFAULT ',') 
return CLOB
as
  V_LIST CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_LIST,TRUE,DBMS_LOB.CALL);
  for i in P_TABLE.first .. P_TABLE.last loop
    if (i > 1) then 
  	  DBMS_LOB.WRITEAPPEND(V_LIST,length(P_DELIMITER),P_DELIMITER); 
	end if;
	DBMS_LOB.WRITEAPPEND(V_LIST,length(P_TABLE(i)),P_TABLE(i));
  end loop;
  return V_LIST;
end;
--
procedure GENERATE_STATEMENT(P_SOURCE_SCHEMA VARCHAR2)
/*
** Generate SQL Statement to create a JSON document from the contents of the supplied schema.
*/
as
  V_SQL_FRAGMENT  VARCHAR2(32767);
  
  cursor getTableMetadata
  is
  select aat.owner
        ,aat.table_name
		,cast(collect('"' || COLUMN_NAME || '"' order by COLUMN_ID) as T_VC4000_TABLE) COLUMN_LIST
    from ALL_ALL_TABLES aat
	     inner join ALL_TAB_COLUMNS atc
		         on atc.OWNER = aat.OWNER
		        and atc.TABLE_NAME = aat.TABLE_NAME
   where aat.OWNER = P_SOURCE_SCHEMA
   group by aat.OWNER, aat.TABLE_NAME;
   
   
    
  V_FIRST_ROW BOOLEAN := TRUE;
begin
  DBMS_LOB.CREATETEMPORARY(SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
  V_SQL_FRAGMENT := 'select JSON_OBJECT(''data'' value JSON_OBJECT (' || C_NEWLINE;
  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

  for t in getTableMetadata loop  
	V_SQL_FRAGMENT := C_SINGLE_QUOTE || t.TABLE_NAME || C_SINGLE_QUOTE || ' value ( select JSON_ARRAYAGG(JSON_ARRAY(';
    if (not V_FIRST_ROW) then
      V_SQL_FRAGMENT := ',' || V_SQL_FRAGMENT;
	end if;
	V_FIRST_ROW := FALSE;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	DBMS_LOB.APPEND(SQL_STATEMENT,TABLE_TO_LIST(t.COLUMN_LIST));
    V_SQL_FRAGMENT := ' NULL on NULL returning ' || C_RETURN_TYPE || ') returning ' || C_RETURN_TYPE || ') from "' || t.OWNER || '"."' || t.TABLE_NAME || '")' || C_NEWLINE;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  end loop;

  V_SQL_FRAGMENT := '             returning ' || C_RETURN_TYPE || C_NEWLINE
                 || '           )' || C_NEWLINE
                 || '         returning ' || C_RETURN_TYPE || C_NEWLINE
                 || '       )' || C_NEWLINE
                 || '  from DUAL';
  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
end;
--
function EXPORT_SCHEMA(P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return CLOB
as
  V_JSON_DOCUMENT  CLOB;
  V_CURSOR         SYS_REFCURSOR;
begin
  GENERATE_STATEMENT(P_SOURCE_SCHEMA);
  open V_CURSOR for SQL_STATEMENT;
  fetch V_CURSOR into V_JSON_DOCUMENT;
  close V_CURSOR;
  return V_JSON_DOCUMENT;
end;
--  
end;
/
show errors
--
spool off
--