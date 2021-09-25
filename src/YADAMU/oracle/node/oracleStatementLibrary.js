"use strict" 

const YadamuConstants = require('../../common/yadamuConstants.js');

class OracleStatementLibrary {

  static get DB_VERSION()                     { return 19 }

  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_SCHEMA_INFORMATION()         { return _SQL_SCHEMA_INFORMATION } 
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }

  static get SQL_SET_CURRENT_SCHEMA()         { return _SQL_SET_CURRENT_SCHEMA }
  static get SQL_DISABLE_CONSTRAINTS()        { return _SQL_DISABLE_CONSTRAINTS }
  static get SQL_ENABLE_CONSTRAINTS()         { return _SQL_ENABLE_CONSTRAINTS }
  static get SQL_REFRESH_MATERIALIZED_VIEWS() { return _SQL_REFRESH_MATERIALIZED_VIEWS }

  constructor(dbi) {
    this.dbi = dbi
  }

  get SQL_CREATE_EXTERNAL_TABLE() {
     this._SQL_CREATE_EXTERNAL_TABLE = this._SQL_CREATE_EXTERNAL_TABLE || (() => { 
       return 
    })();
    return this._SQL_CREATE_EXTERNAL_TABLE
  }   
}
 
module.exports = OracleStatementLibrary

// SQL Statements

const _SQL_SET_CURRENT_SCHEMA         = `begin :log := YADAMU_IMPORT.SET_CURRENT_SCHEMA(:schema); end;`;

const _SQL_DISABLE_CONSTRAINTS        = `begin :log := YADAMU_IMPORT.DISABLE_CONSTRAINTS(:schema); end;`;

const _SQL_ENABLE_CONSTRAINTS         = `begin :log := YADAMU_IMPORT.ENABLE_CONSTRAINTS(:schema); end;`;

const _SQL_REFRESH_MATERIALIZED_VIEWS = `begin :log := YADAMU_IMPORT.REFRESH_MATERIALIZED_VIEWS(:schema); end;`;

const _SQL_CONFIGURE_CONNECTION = 
`begin 
   :DB_VERSION := YADAMU_EXPORT.DATABASE_RELEASE(); 
   :MAX_STRING_SIZE := YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE; 
   if YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED then :EXTENDED_STRING := 'TRUE'; else :EXTENDED_STRING := 'FALSE'; end if;
   :JSON_STORAGE_MODEL := YADAMU_IMPORT.C_JSON_DATA_TYPE; 
   :XML_STORAGE_MODEL := YADAMU_IMPORT.C_XML_STORAGE_MODEL; 
   if YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED then :JSON_PARSING := 'TRUE'; else :JSON_PARSING := 'FALSE'; end if;
   if YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED then :NATIVE_JSON_TYPE := 'TRUE'; else :NATIVE_JSON_TYPE := 'FALSE'; end if;
 end;`;

const _SQL_SYSTEM_INFORMATION   = `begin :sysInfo := YADAMU_EXPORT.GET_SYSTEM_INFORMATION(); end;`;

const _SQL_GET_DLL_STATEMENTS   = `declare
  JOB_NOT_ATTACHED EXCEPTION;
  PRAGMA EXCEPTION_INIT( JOB_NOT_ATTACHED , -31623 );
  
  V_SCHEMA           VARCHAR2(128) := :V1;

  V_HDL_OPEN         NUMBER;
  V_HDL_TRANSFORM    NUMBER;

  V_DDL_STATEMENTS SYS.KU$_DDLS;
  V_DDL_STATEMENT  CLOB;
  
  C_NEWLINE          CONSTANT CHAR(1) := CHR(10);
  C_CARRIAGE_RETURN  CONSTANT CHAR(1) := CHR(13);
  C_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);
  
  V_RESULT JSON_ARRAY_T := new JSON_ARRAY_T();
  
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


  V_JSON_COLUMNS   CLOB;
begin
--
  select JSON_OBJECT('jsonColumns' value 
           JSON_ARRAYAGG(
		     JSON_OBJECT(
			  'owner' value OWNER, 'tableName' value TABLE_NAME, 'columnName' value COLUMN_NAME, 'jsonFormat' value FORMAT, 'dataType' value  DATA_TYPE
		     )
         $IF YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
             returning CLOB
		   )
           returning CLOB
         $ELSIF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
             returning VARCHAR2(32767)
		   )
           returning VARCHAR2(32767)
         )
         $ELSE   
             returning VARCHAR2(4000)
		   ) 
		   returning VARCHAR2(4000)
		 $END
       )
	into V_JSON_COLUMNS
    from ALL_JSON_COLUMNS
   where OBJECT_TYPE = 'TABLE'
     and OWNER = V_SCHEMA;
		 
  V_RESULT.APPEND(V_JSON_COLUMNS);

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
        V_RESULT.APPEND(V_DDL_STATEMENT);
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
          V_RESULT.APPEND(V_DDL_STATEMENT);
        end if;
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

  for t in heirachicalTableList(V_SCHEMA) loop
    V_RESULT.APPEND('begin DBMS_XDBZ.ENABLE_HIERARCHY(SYS_CONTEXT(''USERENV'',''CURRENT_SCHEMA''),''' || t.TABLE_NAME  || '''); end;');
  end loop;

  for i in indexedColumnList(V_SCHEMA) loop
    V_RESULT.APPEND('begin YADAMU_EXPORT_DDL.RENAME_INDEX(''' || i.TABLE_NAME  || ''',''' || i.INDEXED_EXPORT_SELECT_LIST || ''',''' || i.INDEX_NAME || '''); end;');
  end loop;

  :V2 :=  V_RESULT.to_CLOB();
  
end;`;

const _SQL_SCHEMA_INFORMATION = `select * from table(YADAMU_EXPORT.GET_DML_STATEMENTS(:schema,:spatialFormat,:objectsAsJSON,:raw1AsBoolean))`;

const _SQL_CREATE_SAVE_POINT  = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT = `ROLLBACK TO ${YadamuConstants.SAVE_POINT_NAME}`;

  
