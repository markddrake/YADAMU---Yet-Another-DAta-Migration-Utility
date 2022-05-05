"use strict" 

import YadamuConstants from '../../../lib/yadamuConstants.js';
import DefaultStatmentLibrary from '../oracleStatementLibrary.js'

class OracleStatementLibrary extends DefaultStatmentLibrary{

  static get DATABASE_VERSION()                 { return 11.2 }
  static get SQL_GET_DLL_STATEMENTS()     { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_DROP_WRAPPERS()          { return _SQL_DROP_WRAPPERS } 

  constructor(dbi) {
    super(dbi)
  }

}

export {OracleStatementLibrary as default }

// SQL Statements

const _SQL_GET_DLL_STATEMENTS = `declare
  JOB_NOT_ATTACHED EXCEPTION;
  PRAGMA EXCEPTION_INIT( JOB_NOT_ATTACHED , -31623 );
  
  V_RESULT YADAMU_UTILITIES.KVP_TABLE := YADAMU_UTILITIES.KVP_TABLE();
  
  V_SCHEMA           VARCHAR2(128) := :V1;

  V_HDL_OPEN         NUMBER;
  V_HDL_TRANSFORM    NUMBER;

  V_DDL_STATEMENTS SYS.KU$_DDLS;
  V_DDL_STATEMENT  CLOB;
  
  C_NEWLINE          CONSTANT CHAR(1) := CHR(10);
  C_CARRIAGE_RETURN  CONSTANT CHAR(1) := CHR(13);
  C_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);
   
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

  V_RESULT.extend(1);
  V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,'{"jsonColumns":null}');

  -- Use DBMS_METADATA package to access the XMLSchemas registered in the target database schema

  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',false);

  begin
    V_HDL_OPEN := DBMS_METADATA.OPEN('XMLSCHEMA');
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',V_SCHEMA);
    loop
      -- TO DO Switch to FETCH_DDL and process table of statements..
      V_DDL_STATEMENT := DBMS_METADATA.FETCH_CLOB(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENT IS NULL;
      -- Strip leading and trailing white space from DDL statement
      V_DDL_STATEMENT := TRIM(BOTH C_NEWLINE FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(BOTH C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(V_DDL_STATEMENT);
      if (TRIM(V_DDL_STATEMENT) <> '10 10') then
        V_RESULT.extend(1);
        V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,V_DDL_STATEMENT);
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
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',V_SCHEMA);

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

    -- Exclude Statisticstype
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'EXCLUDE_PATH_EXPR','=''STATISTICS''');

    loop
      -- Get the next batch of DDL_STATEMENTS. Each batch may contain zero or more spaces.
      V_DDL_STATEMENTS := DBMS_METADATA.FETCH_DDL(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENTS IS NULL;
      for i in 1 .. V_DDL_STATEMENTS.count loop

        V_DDL_STATEMENT := V_DDL_STATEMENTS(i).DDLTEXT;

        -- Strip leading and trailing white space from DDL statement
        V_DDL_STATEMENT := TRIM(BOTH C_NEWLINE FROM V_DDL_STATEMENT);
        V_DDL_STATEMENT := TRIM(BOTH C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
        V_DDL_STATEMENT := TRIM(V_DDL_STATEMENT);
		if (INSTR(V_DDL_STATEMENT,'CREATE TABLE') = 1) then
		  YADAMU_EXPORT_DDL.PATCH_STORAGE_CLAUSES(V_DDL_STATEMENT);
		end if;
        if (DBMS_LOB.getLength(V_DDL_STATEMENT) > 0) then
          V_RESULT.extend(1);
          V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,V_DDL_STATEMENT);
        end if;
      end loop;
    end loop;

    DBMS_METADATA.CLOSE(V_HDL_OPEN);

/*
  exception
    when JOB_NOT_ATTACHED then
      DBMS_METADATA.CLOSE(V_HDL_OPEN);
    when others then
      RAISE;
*/
  end;

  -- Renable the heirarchy for any heirachically enabled tables in the export file

  for t in heirachicalTableList(V_SCHEMA) loop
    V_RESULT.extend(1);
    V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,'begin DBMS_XDBZ.ENABLE_HIERARCHY(SYS_CONTEXT(''USERENV'',''CURRENT_SCHEMA''),''' || t.TABLE_NAME  || '''); end;');
  end loop;

  for i in indexedColumnList(V_SCHEMA) loop
    V_RESULT.extend(1);
    V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,'begin YADAMU_EXPORT_DDL.RENAME_INDEX(''' || i.TABLE_NAME  || ''',''' || i.INDEXED_EXPORT_SELECT_LIST || ''',''' || i.INDEX_NAME || '''); end;');
  end loop;

  :V2 := YADAMU_UTILITIES.JSON_ARRAY_CLOB(V_RESULT);
  
end;`;

const _SQL_DROP_WRAPPERS = `declare
  OBJECT_NOT_FOUND EXCEPTION;
  PRAGMA EXCEPTION_INIT( OBJECT_NOT_FOUND , -4043 );
begin
  execute immediate 'DROP FUNCTION ":1:".":2:"';
exception
  when OBJECT_NOT_FOUND then
    NULL;
  when others then
    RAISE;
end;`
  
