--
create or replace package YADAMU_EXPORT_DDL
AUTHID CURRENT_USER
as

  TYPE T_DDL_STATEMENTS is TABLE of CLOB;

  TYPE T_RESULTS_CACHE is VARRAY(2147483647) of CLOB;
  RESULTS_CACHE        T_RESULTS_CACHE := T_RESULTS_CACHE();

  C_FATAL_ERROR      CONSTANT VARCHAR2(32) := 'FATAL';
  C_WARNING          CONSTANT VARCHAR2(32) := 'WARNING';
  C_IGNOREABLE       CONSTANT VARCHAR2(32) := 'IGNORE';
  C_DUPLICATE        CONSTANT VARCHAR2(32) := 'DUPLICATE';
  C_REFERENCE        CONSTANT VARCHAR2(32) := 'REFERENCE';
  C_AQ_ISSUE         CONSTANT VARCHAR2(32) := 'AQ RELATED';
  C_RECOMPILATION    CONSTANT VARCHAR2(32) := 'RECOMPILATION';
  C_XLARGE_SQL       CONSTANT VARCHAR2(32) := 'SQL_TOO_LARGE';

  function FETCH_DDL_STATEMENTS(P_SCHEMA VARCHAR2) return T_DDL_STATEMENTS pipelined;
  function APPLY_DDL_STATEMENT(P_DDL_STATEMENT IN OUT NOCOPY CLOB, P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) RETURN BOOLEAN;
  function IMPORT_DDL_STATEMENTS(P_DDL_STATEMENTS IN OUT NOCOPY BLOB, P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) RETURN BOOLEAN;
  function APPLY_DDL_STATEMENTS(P_DDL_STATEMENTS IN OUT NOCOPY BLOB, P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) RETURN CLOB;
  function GENERATE_LOG return CLOB;
  function FETCH_LOG_RECORDS return T_RESULTS_CACHE pipelined;
  PROCEDURE RENAME_INDEX(P_TABLE_NAME VARCHAR2, P_EXPORT_SELECT_LIST VARCHAR2, P_INDEX_NAME VARCHAR2);
  
end;
/
--
set TERMOUT on
--
show errors
--
@@SET_TERMOUT
--
create or replace package body YADAMU_EXPORT_DDL
as
--
  G_ABORT_PROCESSING BOOLEAN := FALSE;
--
FUNCTION FETCH_LOG_RECORDS
return T_RESULTS_CACHE PIPELINED
as
begin 
  for i in 1 .. RESULTS_CACHE.count loop
    pipe row( RESULTS_CACHE(i));
  end loop;
end;
--
PROCEDURE TRACE_OPERATION(P_SQL_STATEMENT CLOB)
as
begin
  RESULTS_CACHE.extend();
--
$IF JSON_FEATURE_DETECTION.GENERATION_SUPPORTED $THEN
--
  select JSON_OBJECT('trace' value JSON_OBJECT('sqlStatement' value P_SQL_STATEMENT
                     $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                     returning CLOB) returning CLOB)
                     $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                     returning  VARCHAR2(32767)) returning  VARCHAR2(32767))
                     $ELSE
                     returning  VARCHAR2(4000)) returning  VARCHAR2(4000))
                     $END
    into RESULTS_CACHE(RESULTS_CACHE.count)
    from DUAL;
--
$IF NOT JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
exception
  when YADAMU_UTILITIES.JSON_OVERFLOW1 or YADAMU_UTILITIES.JSON_OVERFLOW2 or YADAMU_UTILITIES.JSON_OVERFLOW3 or YADAMU_UTILITIES.BUFFER_OVERFLOW then
    RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                            YADAMU_UTILITIES.KVP_TABLE(
                                              YADAMU_UTILITIES.KVJ(
                                                'trace',
                                                YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                                   YADAMU_UTILITIES.KVP_TABLE(
                                                     YADAMU_UTILITIES.KVC('sqlStatement',P_SQL_STATEMENT)
                                                   )
                                                )
                                              )
                                            )
                                          );
  when others then
    RAISE;
--
$END
--
$ELSE
--
  RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                          YADAMU_UTILITIES.KVP_TABLE(
                                            YADAMU_UTILITIES.KVJ(
                                              'trace',
                                              YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                                 YADAMU_UTILITIES.KVP_TABLE(
                                                   YADAMU_UTILITIES.KVC('sqlStatement',P_SQL_STATEMENT)
                                                 )
                                              )
                                            )
                                          )
                                        );
$END
--
end;
--
procedure LOG_ERROR(P_SEVERITY VARCHAR2, P_SQL_STATEMENT CLOB,P_SQLCODE NUMBER, P_SQLERRM VARCHAR2, P_STACK CLOB)
as
begin
  RESULTS_CACHE.extend;
--
$IF JSON_FEATURE_DETECTION.GENERATION_SUPPORTED $THEN
--
  select JSON_OBJECT('error' value JSON_OBJECT('severity' value P_SEVERITY, 'sqlStatement' value P_SQL_STATEMENT, 'code' value P_SQLCODE, 'msg' value P_SQLERRM, 'details' value P_STACK
                     $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                     returning CLOB) returning CLOB)
                     $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                     returning  VARCHAR2(32767)) returning  VARCHAR2(32767))
                     $ELSE
                     returning  VARCHAR2(4000)) returning  VARCHAR2(4000))
                     $END
     into RESULTS_CACHE(RESULTS_CACHE.count)
     from DUAL;
--
$IF NOT JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
exception
  when YADAMU_UTILITIES.JSON_OVERFLOW1 or YADAMU_UTILITIES.JSON_OVERFLOW2 or YADAMU_UTILITIES.JSON_OVERFLOW3 or YADAMU_UTILITIES.BUFFER_OVERFLOW then
    RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                            YADAMU_UTILITIES.KVP_TABLE(
                                              YADAMU_UTILITIES.KVJ(
                                                'error',
                                                YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                                  YADAMU_UTILITIES.KVP_TABLE(
                                                    YADAMU_UTILITIES.KVC('sqlStatement',P_SQL_STATEMENT),
                                                    YADAMU_UTILITIES.KVS('severity',P_SEVERITY),
                                                    YADAMU_UTILITIES.KVN('code',P_SQLCODE),
                                                    YADAMU_UTILITIES.KVS('msg',P_SQLERRM),
                                                    YADAMU_UTILITIES.KVS('details',P_STACK)
                                                  )
                                                )
                                              )
                                            )
                                          );
  when others then
    RAISE;
--
$END
--
$ELSE
  RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                          YADAMU_UTILITIES.KVP_TABLE(
                                            YADAMU_UTILITIES.KVJ(
                                              'error',
                                              YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                                YADAMU_UTILITIES.KVP_TABLE(
                                                  YADAMU_UTILITIES.KVC('sqlStatement',P_SQL_STATEMENT),
                                                  YADAMU_UTILITIES.KVS('severity',P_SEVERITY),
                                                  YADAMU_UTILITIES.KVN('code',P_SQLCODE),
                                                  YADAMU_UTILITIES.KVS('msg',P_SQLERRM),
                                                  YADAMU_UTILITIES.KVS('details',P_STACK)
                                                )
                                              )
                                            )
                                          )
                                        );
$END
--
end;
--
function GENERATE_LOG
return CLOB
as
  V_RESULTS CLOB;
  --
  $IF NOT JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  --
  -- Make a copy of the results cache as the lob locators will be destroyed by the Table operator even if the operation fails.
  --
  V_RESULTS_CACHE_COPY T_RESULTS_CACHE := RESULTS_CACHE;
  --
  $END
  --
begin
--


$IF JSON_FEATURE_DETECTION.GENERATION_SUPPORTED $THEN
--
  select JSON_ARRAYAGG(
           $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
           TREAT (COLUMN_VALUE as JSON) returning CLOB)
           $ELSIF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
           JSON_QUERY (COLUMN_VALUE, '$' returning CLOB) returning CLOB)
           $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
           JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(32767)) returning VARCHAR2(32767))
           $ELSE   
           JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(4000)) returning VARCHAR2(4000))
           $END
      into V_RESULTS
      from table(RESULTS_CACHE);
  RESULTS_CACHE := T_RESULTS_CACHE();
  return V_RESULTS;
$IF NOT JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
exception
  when YADAMU_UTILITIES.JSON_OVERFLOW1 or YADAMU_UTILITIES.JSON_OVERFLOW2 or YADAMU_UTILITIES.JSON_OVERFLOW3 or YADAMU_UTILITIES.BUFFER_OVERFLOW then
    -- Restore the results cache from the copy.
    RESULTS_CACHE := V_RESULTS_CACHE_COPY;
    declare
      V_CURSOR SYS_REFCURSOR;
    begin
      OPEN V_CURSOR for 'select * from table(:1)' using RESULTS_CACHE;
      V_RESULTS := YADAMU_UTILITIES.JSON_ARRAYAGG_CLOB(V_CURSOR);
      RESULTS_CACHE := T_RESULTS_CACHE();
      return V_RESULTS;
    end;
  when others then
    RAISE;
--
$END
--
$ELSE
-- 
   declare
     V_JSON_ARRAY_TABLE YADAMU_UTILITIES.JSON_ARRAY_TABLE;
   begin
     /*
     ### ORA-21700: object does not exist or is marked for delete 
     select * 
       bulk collect into V_JSON_ARRAY_TABLE
       from table(RESULTS_CACHE);  
     */
     V_JSON_ARRAY_TABLE := YADAMU_UTILITIES.JSON_ARRAY_TABLE();
     if (RESULTS_CACHE.count > 0) then
       V_JSON_ARRAY_TABLE.extend(RESULTS_CACHE.count);
       for i in RESULTS_CACHE.first .. RESULTS_CACHE.last loop
         V_JSON_ARRAY_TABLE(i) := RESULTS_CACHE(i);
       end loop;
     end if;
     V_RESULTS := YADAMU_UTILITIES.JSON_ARRAYAGG_CLOB(V_JSON_ARRAY_TABLE);
     RESULTS_CACHE := T_RESULTS_CACHE();
     return V_RESULTS;
   end;
--
$END
--
end;
--
function FETCH_DDL_STATEMENTS(P_SCHEMA VARCHAR2)
return T_DDL_STATEMENTS
pipelined
/*
**
** Fails in 11.2 and 19.3 as shown below. Code must be run as an anonymous PL/SQL block in order for it work as expected.....
**
** SQL> select YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS('HR') from dual;
** select YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS('HR') from dual
**       *
** ERROR at line 1:
** ORA-31642: the following SQL statement fails:
** BEGIN
** "SYS"."DBMS_SCHED_EXPORT_CALLOUTS".SCHEMA_CALLOUT(:1,1,1,'11.02.00.00.00');
** END;
** ORA-06512: at "SYS.DBMS_SYS_ERROR", line 86
** ORA-06512: at "SYS.DBMS_METADATA", line 1225
** ORA-14552: cannot perform a DDL, commit or rollback inside a query or DML
** ORA-06512: at "SYSTEM.YADAMU_EXPORT_DDL", line 335
**
*/
as
  JOB_NOT_ATTACHED EXCEPTION;
  PRAGMA EXCEPTION_INIT( JOB_NOT_ATTACHED , -31623 );

  V_HDL_OPEN         NUMBER;
  V_HDL_TRANSFORM    NUMBER;

  V_DDL_STATEMENTS SYS.KU$_DDLS;
  V_DDL_STATEMENT  CLOB;

  cursor indexedColumnList(C_SCHEMA VARCHAR2)
  is
   select aic.TABLE_NAME, aic.INDEX_NAME, LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_POSITION) INDEXED_EXPORT_SELECT_LIST
     from ALL_IND_COLUMNS aic
     join ALL_ALL_TABLES aat
       on aic.TABLE_NAME = aat.TABLE_NAME and aic.TABLE_OWNER = aat.OWNER
    where aic.TABLE_OWNER = C_SCHEMA
    group by aic.TABLE_NAME, aic.INDEX_NAME;

  CURSOR heirachicalTableList(C_SCHEMA VARCHAR2)
  is
  select distinct TABLE_NAME
    from ALL_XML_TABLES axt
   where exists(
           select 1
             from ALL_TAB_COLS atc
            where axt.TABLE_NAME = atc.TABLE_NAME and axt.OWNER = atc.OWNER and atc.COLUMN_NAME = 'ACLOID' and atc.HIDDEN_COLUMN = 'YES'
         )
     and exists(
           select 1
             from ALL_TAB_COLS atc
            where axt.TABLE_NAME = atc.TABLE_NAME and axt.OWNER = atc.OWNER and atc.COLUMN_NAME = 'OWNERID' and atc.HIDDEN_COLUMN = 'YES'
        )
    and OWNER = C_SCHEMA;

begin

  -- Use DBMS_METADATA package to access the XMLSchemas registered in the target database schema
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',false);

  begin
    V_HDL_OPEN := DBMS_METADATA.OPEN('XMLSCHEMA');
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',P_SCHEMA);
    loop
      -- TO DO Switch to FETCH_DDL and process table of statements..
      V_DDL_STATEMENT := DBMS_METADATA.FETCH_CLOB(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENT IS NULL;
      -- Strip leading and trailing white space from DDL statement
      V_DDL_STATEMENT := TRIM(BOTH YADAMU_UTILITIES.C_NEWLINE FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(BOTH YADAMU_UTILITIES.C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(V_DDL_STATEMENT);
      if (TRIM(V_DDL_STATEMENT) <> '10 10') then
        PIPE ROW (V_DDL_STATEMENT);
      end if;
    end loop;

    DBMS_METADATA.CLOSE(V_HDL_OPEN);
  exception
    when JOB_NOT_ATTACHED then
      DBMS_METADATA.CLOSE(V_HDL_OPEN);
    when others then
      RAISE;
  end;

  -- Use DBMS_METADATA package to access the DDL statements used to create the database schema

  begin
    V_HDL_OPEN := DBMS_METADATA.OPEN('SCHEMA_EXPORT');
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',P_SCHEMA);

    V_HDL_TRANSFORM := DBMS_METADATA.ADD_TRANSFORM(V_HDL_OPEN,'DDL');

    -- Suppress Segement information for TABLES, INDEXES and CONSTRAINTS

    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'TABLE');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'INDEX');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'CONSTRAINT');

    -- Return constraints as 'ALTER TABLE' operations

    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'CONSTRAINTS_AS_ALTER',true,'TABLE');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'REF_CONSTRAINTS',false,'TABLE');

    -- Exclude XML Schema Info. XML Schemas need to come first and are handled in the previous section

    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'EXCLUDE_PATH_EXPR','=''XMLSCHEMA''');

    loop
      -- Get the next batch of DDL_STATEMENTS. Each batch may contain zero or more spaces.
      V_DDL_STATEMENTS := DBMS_METADATA.FETCH_DDL(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENTS IS NULL;

      for i in 1 .. V_DDL_STATEMENTS.count loop

        V_DDL_STATEMENT := V_DDL_STATEMENTS(i).DDLTEXT;

        -- Strip leading and trailing white space from DDL statement
        V_DDL_STATEMENT := TRIM(BOTH YADAMU_UTILITIES.C_NEWLINE FROM V_DDL_STATEMENT);
        V_DDL_STATEMENT := TRIM(BOTH YADAMU_UTILITIES.C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
        PIPE ROW (TRIM(V_DDL_STATEMENT));
      end loop;
    end loop;

    DBMS_METADATA.CLOSE(V_HDL_OPEN);

  exception
    when JOB_NOT_ATTACHED then
      DBMS_METADATA.CLOSE(V_HDL_OPEN);
    when others then
      RAISE;
  end;

  -- Renable the heirarchy for any heirachically enabled tables in the export file

  for t in heirachicalTableList(P_SCHEMA) loop
    pipe row ('begin DBMS_XDBZ.ENABLE_HIERARCHY(SYS_CONTEXT(''USERENV'',''CURRENT_SCHEMA''),''' || t.TABLE_NAME  || '''); END;');
  end loop;

  for i in indexedColumnList(P_SCHEMA) loop
    pipe row ('BEGIN YADAMU_EXPORT_DDL.RENAME_INDEX(''' || i.TABLE_NAME  || ''',''' || i.INDEXED_EXPORT_SELECT_LIST || ''',''' || i.INDEX_NAME || '''); END;');
  end loop;

end;
--
procedure HACK_MV_PERMISSIONS(P_CREATE_PROCEDURE VARCHAR2,P_TABLE_NAME VARCHAR2) 
as
  INHERIT_PRIVS_ISSUE    EXCEPTION; PRAGMA EXCEPTION_INIT( INHERIT_PRIVS_ISSUE,    -06598 );
  V_SQL_STATEMENT VARCHAR2(2048);
begin
  execute immediate P_CREATE_PROCEDURE;
  TRACE_OPERATION(P_CREATE_PROCEDURE);
  begin
    V_SQL_STATEMENT := 'begin FIX_MV_PERMSISSIONS_ISSUE(''' || P_TABLE_NAME || '''); end;';
    execute immediate V_SQL_STATEMENT;
    TRACE_OPERATION(V_SQL_STATEMENT);
  exception
    when INHERIT_PRIVS_ISSUE then
      begin
        V_SQL_STATEMENT := 'GRANT INHERIT PRIVILEGES ON USER "' || SYS_CONTEXT('USERENV','SESSION_USER') || '" TO "' || SYS_CONTEXT('USERENV','CURRENT_SCHEMA') ||'"';
        execute immediate V_SQL_STATEMENT;
        TRACE_OPERATION(V_SQL_STATEMENT);
        V_SQL_STATEMENT := 'begin FIX_MV_PERMSISSIONS_ISSUE(''' || P_TABLE_NAME || '''); end;';
        execute immediate V_SQL_STATEMENT;
        TRACE_OPERATION(V_SQL_STATEMENT);
      exception
        when OTHERS then
          LOG_ERROR(C_FATAL_ERROR,V_SQL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end;
      V_SQL_STATEMENT := 'REVOKE INHERIT PRIVILEGES ON USER "' || SYS_CONTEXT('USERENV','SESSION_USER') || '" FROM "' || SYS_CONTEXT('USERENV','CURRENT_SCHEMA') ||'"';
      execute immediate V_SQL_STATEMENT;
      TRACE_OPERATION(V_SQL_STATEMENT);
    when OTHERS then
      LOG_ERROR(C_FATAL_ERROR,V_SQL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
  end;
  V_SQL_STATEMENT := 'drop procedure FIX_MV_PERMSISSIONS_ISSUE';
  execute immediate V_SQL_STATEMENT;
  TRACE_OPERATION(V_SQL_STATEMENT);
end;
--  
procedure TRY_MATERIALIZED_VIEW_HACK(P_CREATE_MATERIALIZED_VIEW CLOB)
as
  DUPLICATE_MVIEW            EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_MVIEW,            -12006 );

  FUNCTION_GRANT_PERMSISSIONS CONSTANT VARCHAR2(512) := 'create or replace procedure FIX_MV_PERMSISSIONS_ISSUE(P_BASE_TABLE VARCHAR2)' || YADAMU_UTILITIES.C_NEWLINE
                                                     || 'as' || YADAMU_UTILITIES.C_NEWLINE
                                                     || 'begin' || YADAMU_UTILITIES.C_NEWLINE
                                                     || '  execute immediate ''GRANT ALL ON "'' || P_BASE_TABLE ||  ''" to "' || SYS_CONTEXT('USERENV','SESSION_USER')  || '" WITH GRANT OPTION''; ' || YADAMU_UTILITIES.C_NEWLINE
                                                     || 'end;';

  FUNCTION_REVOKE_PERMISSIONS CONSTANT VARCHAR2(512) := 'create or replace procedure FIX_MV_PERMSISSIONS_ISSUE(P_BASE_TABLE VARCHAR2)' || YADAMU_UTILITIES.C_NEWLINE
                                                     || 'as' || YADAMU_UTILITIES.C_NEWLINE   
                                                     || 'begin' || YADAMU_UTILITIES.C_NEWLINE
                                                     || '  execute immediate ''REVOKE ALL ON "'' || P_BASE_TABLE ||  ''" FROM "' || SYS_CONTEXT('USERENV','SESSION_USER') || '"'';' || YADAMU_UTILITIES.C_NEWLINE
                                                     || 'end;';
  V_SQL_STATEMENT CLOB;
  V_BASE_TABLE_NAME VARCHAR2(129);

begin
  V_BASE_TABLE_NAME := SUBSTR(P_CREATE_MATERIALIZED_VIEW,INSTR(P_CREATE_MATERIALIZED_VIEW,'"',1,3)+1,129);
  V_BASE_TABLE_NAME := SUBSTR(V_BASE_TABLE_NAME,1,INSTR(V_BASE_TABLE_NAME,'"')-1);

  HACK_MV_PERMISSIONS(FUNCTION_GRANT_PERMSISSIONS,V_BASE_TABLE_NAME);

  V_SQL_STATEMENT := P_CREATE_MATERIALIZED_VIEW;
  TRACE_OPERATION(V_SQL_STATEMENT);
  execute immediate V_SQL_STATEMENT;
  
  HACK_MV_PERMISSIONS(FUNCTION_REVOKE_PERMISSIONS,V_BASE_TABLE_NAME);  
exception
  when DUPLICATE_MVIEW then
    LOG_ERROR(C_DUPLICATE,V_SQL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
  when OTHERS then
    LOG_ERROR(C_FATAL_ERROR,V_SQL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
end;
--
procedure RENAME_INDEX(P_TABLE_NAME VARCHAR2, P_EXPORT_SELECT_LIST VARCHAR2, P_INDEX_NAME VARCHAR2)
as
  V_INDEX_NAME VARCHAR2(128);
  V_STATEMENT CLOB;
begin
  select INDEX_NAME
    into V_INDEX_NAME
    from (
      select aic.TABLE_NAME, aic.INDEX_NAME, LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_POSITION) INDEXED_EXPORT_SELECT_LIST
        from ALL_IND_COLUMNS aic
        join ALL_ALL_TABLES aat
          on aic.TABLE_NAME = aat.TABLE_NAME and aic.TABLE_OWNER = aat.OWNER
       where aic.TABLE_OWNER = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
         and aic.TABLE_NAME = P_TABLE_NAME
       group by aic.TABLE_NAME, aic.INDEX_NAME
    )
  where INDEXED_EXPORT_SELECT_LIST = P_EXPORT_SELECT_LIST;

  if (V_INDEX_NAME <> P_INDEX_NAME) then
    V_STATEMENT := 'ALTER INDEX "' || V_INDEX_NAME || '" RENAME TO "' || P_INDEX_NAME || '"';
    EXECUTE IMMEDIATE(V_STATEMENT);
  end if;
end;
--
procedure REGISTER_XML_SCHEMA(P_TARGET_SCHEMA VARCHAR2, P_XML_SCHEMA_DETAILS IN OUT NOCOPY XMLTYPE)
as
  cursor getXMLSchemas(C_XML_SCHEMA_DETAILS XMLTYPE)
  is
  select SCHEMA_URL, case when LOCAL = 1 then 'YES' else 'NO' end LOCAL, XMLTYPE(XML_SCHEMA) XML_SCHEMA
    from XMLTABLE(
           '/ROWSET/ROW'
           passing C_XML_SCHEMA_DETAILS
           COLUMNS
             SCHEMA_URL VARCHAR2(4000) PATH 'XMLSCHEMA_T/URL',
             LOCAL      NUMBER(1)      PATH 'XMLSCHEMA_T/LOCAL',
             XML_SCHEMA CLOB           PATH 'XMLSCHEMA_T/STRIPPED_VAL'
        );

  V_SCHEMA_COUNT PLS_INTEGER;
  V_REGISTER_SCHEMA VARCHAR2(4000);
  V_LOCAL VARCHAR2(5) := 'TRUE';
  
begin
  for x in getXMLSchemas(P_XML_SCHEMA_DETAILS) loop
 
   if (x.LOCAL = 'NO') then
     V_LOCAL := 'FALSE';
   end if;
   
   V_REGISTER_SCHEMA := 
'declare
  V_XML_SCHEMA XMLTYPE       := XMLTYPE(''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">...</xs:schema>'');
  V_SCHEMA_URL VARCHAR(2048) := ''' || x.SCHEMA_URL || ''';
  V_LOCAL      BOOLEAN       := ' || V_LOCAL || ';
  V_OWNER      VARCHAR2(128) := ''' || P_TARGET_SCHEMA || ''';
begin
  DBMS_XMLSCHEMA.registerSchema(
    SCHEMAURL => V_SCHEMA_URL
   ,SCHEMADOC => V_XML_SCHEMA
   ,LOCAL     => V_LOCAL
   ,GENTYPES  => FALSE
   ,GENTABLES => FALSE
   ,OWNER     => V_OWNER
  );
end;';
    select count(*)
      into V_SCHEMA_COUNT
      from ALL_XML_SCHEMAS axs
     where x.LOCAL = axs.LOCAL and x.SCHEMA_URL = axs.SCHEMA_URL;

    if (V_SCHEMA_COUNT = 0) then
      DBMS_XMLSCHEMA.registerSchema(
        SCHEMAURL => x.SCHEMA_URL
       ,SCHEMADOC  => x.XML_SCHEMA
       ,LOCAL => (x.LOCAL = 'YES')
       ,GENTYPES => FALSE
       ,GENTABLES => FALSE
       ,OWNER => P_TARGET_SCHEMA
      );
      TRACE_OPERATION(V_REGISTER_SCHEMA);
    end if;
  end loop;
exception
  when OTHERS then
    LOG_ERROR(C_FATAL_ERROR,V_REGISTER_SCHEMA,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
end;
--
procedure SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2)
as
  USER_NOT_FOUND EXCEPTION ; PRAGMA EXCEPTION_INIT( USER_NOT_FOUND , -01435 );
  V_SQL_STATEMENT CONSTANT VARCHAR2(4000) := 'ALTER SESSION SET CURRENT_SCHEMA = "' || P_TARGET_SCHEMA || '"';
begin
  if (SYS_CONTEXT('USERENV','CURRENT_SCHEMA') <> P_TARGET_SCHEMA) then
    TRACE_OPERATION(V_SQL_STATEMENT);
    execute immediate V_SQL_STATEMENT;
  end if;
exception
  when USER_NOT_FOUND then
    LOG_ERROR(C_FATAL_ERROR,V_SQL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
  when OTHERS then
    RAISE;
end;
--
function APPLY_DDL_STATEMENT(P_DDL_STATEMENT IN OUT NOCOPY CLOB ,P_SOURCE_SCHEMA VARCHAR2, P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return BOOLEAN
as
  MISSING_OBJECT             EXCEPTION; PRAGMA EXCEPTION_INIT( MISSING_OBJECT,             -00942 );

  DUPLICATE_OBJECT           EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_OBJECT,           -00955 );

  INSUFFICIENT_PRIVILEGES    EXCEPTION; PRAGMA EXCEPTION_INIT( INSUFFICIENT_PRIVILEGES,    -01031 );

  DUPLICATE_INDEX            EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_INDEX,            -01408 );

  MISSING_INDEX              EXCEPTION; PRAGMA EXCEPTION_INIT( MISSING_INDEX,              -01418 );

  NO_TABLESPACE_PRIVILEGES   EXCEPTION; PRAGMA EXCEPTION_INIT( NO_TABLESPACE_PRIVILEGES,   -01950 );
 
  MISSING_USER_OR_ROLE       EXCEPTION; PRAGMA EXCEPTION_INIT( MISSING_USER_OR_ROLE,       -01917 );

  DUPLICATE_PKEY             EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_PKEY,             -02260 );

  DUPLICATE_KEY              EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_KEY,              -02261 );

  DUPLICATE_NAME             EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_NAME,             -02264 );

  DUPLICATE_REF_CONSTRAINT   EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_REF_CONSTRAINT,   -02275 );

  DUPLICATE_MVIEW            EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_MVIEW,            -12006 );

  RULES_ENGINE_ISSUE         EXCEPTION; PRAGMA EXCEPTION_INIT( RULES_ENGINE_ISSUE,         -24171 );

  COMPILATION_ERROR          EXCEPTION; PRAGMA EXCEPTION_INIT( COMPILATION_ERROR,          -24344 );

  INVALID_AQ_OPERATION       EXCEPTION; PRAGMA EXCEPTION_INIT( INVALID_AQ_OPERATION,       -24005 );

  DUPLICATE_DIMENSION        EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_DIMENSION,        -30371 );
  
  DUPLICATE_JSON_CONSTRAINT  EXCEPTION; PRAGMA EXCEPTION_INIT( DUPLICATE_JSON_CONSTRAINT,  -40664 ); 
  
  HIERARCHICAL_FAILURE       EXCEPTION; PRAGMA EXCEPTION_INIT( HIERARCHICAL_FAILURE,       -31061 ); 
  
  V_SOURCE_SCHEMA_REFERENCE  CONSTANT VARCHAR2(131) := '"' || P_SOURCE_SCHEMA || '".';
  V_TARGET_SCHEMA_REFERENCE  CONSTANT VARCHAR2(131) := '"' || P_TARGET_SCHEMA || '".';

  V_XML_SCHEMA_INFORMATION XMLTYPE;

begin
  begin
    if (substr(P_DDL_STATEMENT,1,21) = '<?xml version="1.0"?>') then
      V_XML_SCHEMA_INFORMATION := XMLTYPE(P_DDL_STATEMENT);
      REGISTER_XML_SCHEMA(P_TARGET_SCHEMA,V_XML_SCHEMA_INFORMATION);
    else
      P_DDL_STATEMENT := replace(P_DDL_STATEMENT,V_SOURCE_SCHEMA_REFERENCE,V_TARGET_SCHEMA_REFERENCE);
      execute immediate P_DDL_STATEMENT;
      TRACE_OPERATION(P_DDL_STATEMENT);
    end if;
  exception
    when DUPLICATE_OBJECT or DUPLICATE_INDEX or DUPLICATE_PKEY or DUPLICATE_KEY or DUPLICATE_NAME or DUPLICATE_REF_CONSTRAINT OR DUPLICATE_MVIEW OR DUPLICATE_DIMENSION or DUPLICATE_JSON_CONSTRAINT or HIERARCHICAL_FAILURE then
      LOG_ERROR(C_DUPLICATE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    when COMPILATION_ERROR then
      LOG_ERROR(C_RECOMPILATION,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    when NO_TABLESPACE_PRIVILEGES then
      if (upper(P_DDL_STATEMENT) LIKE '%USAGE QUEUE') then
        LOG_ERROR(C_AQ_ISSUE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      else
        LOG_ERROR(C_FATAL_ERROR,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end if;
    when NO_DATA_FOUND then
      if (upper(P_DDL_STATEMENT) LIKE '%SYS.DBMS_AQ_IMP_INTERNAL.IMPORT_QUEUE%') then
        LOG_ERROR(C_AQ_ISSUE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      else
        LOG_ERROR(C_FATAL_ERROR,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end if;
    when RULES_ENGINE_ISSUE then
      if (upper(P_DDL_STATEMENT) LIKE upper('%dbms_rule_imp_obj%AQ$_%')) then
        LOG_ERROR(C_AQ_ISSUE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      else
        LOG_ERROR(C_FATAL_ERROR,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end if;
    when INVALID_AQ_OPERATION then
      LOG_ERROR(C_AQ_ISSUE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    when INSUFFICIENT_PRIVILEGES then
      if (upper(P_DDL_STATEMENT) like 'CREATE MATERIALIZED VIEW%ON PREBUILT TABLE%ENABLE QUERY REWRITE%') then
        TRY_MATERIALIZED_VIEW_HACK(P_DDL_STATEMENT);        
      else
        LOG_ERROR(C_FATAL_ERROR,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end if;
    when MISSING_INDEX then
      if (P_DDL_STATEMENT like 'ALTER INDEX "%"."SYS_IOT_TOP%" RENAME TO "SYS_IOT_TOP%"') then
        LOG_ERROR(C_DUPLICATE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      else
        LOG_ERROR(C_REFERENCE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end if;
    when MISSING_OBJECT then
      LOG_ERROR(C_REFERENCE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    when MISSING_USER_OR_ROLE then
      LOG_ERROR(C_REFERENCE,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    when OTHERS then
      LOG_ERROR(C_FATAL_ERROR,P_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
  end;
  return G_ABORT_PROCESSING;
end;
--
procedure APPLY_DDL_STATEMENTS(P_DDL_STATEMENTS IN OUT NOCOPY BLOB, P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
--
-- Use Case 1. Processing the ddl object in an entire Import File. The Object also contains a systemInformation Object.
--
-- Use Case 2. Processing a stand-alone ddl object extrats from from an Import File.
--
-- Use Case 3. Processing the DDL XML document generated when the target database does not support JSON processing.
--
as
  V_CURRENT_SCHEMA    VARCHAR2(128) := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');
--
$IF JSON_FEATURE_DETECTION.PARSING_SUPPORTED $THEN
--
  cursor getDDLStatements is
  select DDL_STATEMENT
    from JSON_TABLE(
           P_DDL_STATEMENTS,
           '$.ddl[*]'
           columns(
             $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
             DDL_STATEMENT            CLOB PATH '$'
             $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
             DDL_STATEMENT VARCHAR2(32767) PATH '$' ERROR ON ERROR
             $ELSE
             DDL_STATEMENT  VARCHAR2(4000) PATH '$'ERROR ON ERROR
             $END
           )
         );
--
$ELSE
--
  cursor getDDLStatements is
  select DDL_STATEMENT
    from XMLTABLE(
           '/ddlStatements/ddl'
           passing XMLTYPE(P_DDL_STATEMENTS,nls_charset_id('AL32UTF8'))
           columns
             DDL_STATEMENT            CLOB PATH 'text()'
         );
--
$END
--       
  V_DDL_STATEMENT        CLOB;
  V_ABORT_DDL_OPERATIONS BOOLEAN;
begin
  SET_CURRENT_SCHEMA(P_TARGET_SCHEMA);
  --
  $IF DBMS_DB_VERSION.VER_LE_11_2 $THEN
  --
  -- Force XMLType STORE AS CLOB.
  --
  V_DDL_STATEMENT := 'ALTER SESSION SET EVENTS = ''31050 trace name context forever,level 0x2000''';
  TRACE_OPERATION(V_DDL_STATEMENT);
  execute immediate V_DDL_STATEMENT;
  --
  $END
  --
  for s in getDDLStatements loop
    exit when G_ABORT_PROCESSING;
    V_DDL_STATEMENT := s.DDL_STATEMENT;
    V_ABORT_DDL_OPERATIONS := APPLY_DDL_STATEMENT(V_DDL_STATEMENT,P_SOURCE_SCHEMA,P_TARGET_SCHEMA);
  end loop;
  SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
exception
  when YADAMU_UTILITIES.JSON_OVERFLOW1 or YADAMU_UTILITIES.JSON_OVERFLOW2 or YADAMU_UTILITIES.JSON_OVERFLOW3 then
    LOG_ERROR(C_XLARGE_SQL,'Maximum suported SQL statement size by this database configuration is ' ||  JSON_FEATURE_DETECTION.C_MAX_STRING_SIZE || ' characters.',SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
  when OTHERS then
    LOG_ERROR(C_FATAL_ERROR,V_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
end;
--
function IMPORT_DDL_STATEMENTS(P_DDL_STATEMENTS IN OUT NOCOPY BLOB, P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return BOOLEAN
as
begin
  APPLY_DDL_STATEMENTS(P_DDL_STATEMENTS,P_SOURCE_SCHEMA,P_TARGET_SCHEMA);
  return G_ABORT_PROCESSING;
end;
--
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
function APPLY_DDL_STATEMENTS(P_DDL_STATEMENTS IN OUT NOCOPY BLOB, P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return CLOB
as
  V_RESULTS CLOB;
begin
  RESULTS_CACHE := T_RESULTS_CACHE();
  APPLY_DDL_STATEMENTS(P_DDL_STATEMENTS,P_SOURCE_SCHEMA,P_TARGET_SCHEMA);
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
    into V_RESULTS
    from table(RESULTS_CACHE);
  RESULTS_CACHE := T_RESULTS_CACHE();
  return V_RESULTS;
end;
--
$ELSE
--
function APPLY_DDL_STATEMENTS(P_DDL_STATEMENTS IN OUT NOCOPY BLOB, P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'), P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return CLOB
as
begin
  RESULTS_CACHE := T_RESULTS_CACHE();
  APPLY_DDL_STATEMENTS(P_DDL_STATEMENTS,P_SOURCE_SCHEMA,P_TARGET_SCHEMA);
  return GENERATE_LOG();
end;
--
$END
--
end;
/
--
set TERMOUT on
--
show errors
--
@@SET_TERMOUT
--