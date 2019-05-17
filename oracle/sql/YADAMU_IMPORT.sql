--
/*
** De-serialize serialized data
*/
declare
  TABLE_NOT_FOUND EXCEPTION;
  PRAGMA EXCEPTION_INIT( TABLE_NOT_FOUND , -00942 );
begin
  execute immediate 'drop table "SCHEMA_COMPARE_RESULTS"';
exception
  when TABLE_NOT_FOUND then
    null;
  when others then  
    RAISE;
end;
/
create global temporary table SCHEMA_COMPARE_RESULTS (
  SOURCE_SCHEMA    VARCHAR2(128)
 ,TARGET_SCHEMA    VARCHAR2(128)
 ,TABLE_NAME       VARCHAR2(128)
 ,SOURCE_ROW_COUNT NUMBER
 ,TARGET_ROW_COUNT NUMBER
 ,MISSING_ROWS     NUMBER
 ,EXTRA_ROWS       NUMBER
 ,SQLERRM          VARCHAR2(4000)
) 
ON COMMIT PRESERVE  ROWS
/
create or replace package YADAMU_IMPORT
AUTHID CURRENT_USER
as
  C_VERSION_NUMBER constant NUMBER(4,2) := 1.0;

  C_SUCCESS          CONSTANT VARCHAR2(32) := 'SUCCESS';
  C_FATAL_ERROR      CONSTANT VARCHAR2(32) := 'FATAL';
  C_WARNING          CONSTANT VARCHAR2(32) := 'WARNING';
  C_IGNOREABLE       CONSTANT VARCHAR2(32) := 'IGNORE';
  C_XLARGE_CONTENT   CONSTANT VARCHAR2(32) := 'CONTENT_TOO_LARGE';

  TYPE T_RESULTS_CACHE is VARRAY(2147483647) of CLOB;
  RESULTS_CACHE        T_RESULTS_CACHE := T_RESULTS_CACHE();
  
  TYPE TABLE_INFO_RECORD is RECORD (
    DDL                CLOB
   ,DML                CLOB
   ,TARGET_DATA_TYPES  CLOB
  );
  
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  $ELSE
  TYPE KVP_RECORD is RECORD (
    KEY                VARCHAR2(4000)
   ,VALUE              CLOB
  );
  
  TYPE KEY_VALUE_PAIR_TABLE is TABLE of KVP_RECORD;
  $END  

  procedure DATA_ONLY_MODE(P_DATA_ONLY_MODE BOOLEAN);
  procedure DDL_ONLY_MODE(P_DDL_ONLY_MODE BOOLEAN);

  function IMPORT_VERSION return NUMBER deterministic;
  procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'));

  function IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) return CLOB;
  function GENERATE_SQL(P_SOURCE_VENROR VARCHAR2,P_TARGET_SCHEMA VARCHAR2, P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2, P_COLUMN_LIST CLOB, P_DATA_TYPE_ARRAY CLOB, P_SIZE_CONSTRAINTS CLOB, P_XMLTYPE_STORAGE_MODEL VARCHAR2 DEFAULT 'CLOB') return TABLE_INFO_RECORD;
  function GENERATE_STATEMENTS(P_METADATA IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) return CLOB;
  
  function SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2) return CLOB;
  
  function DISABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2) return CLOB;
  function ENABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2) return CLOB;
  function REFRESH_MATERIALIZED_VIEWS(P_TARGET_SCHEMA VARCHAR2) return CLOB;

  function MAP_FOREIGN_DATATYPE(P_SOURCE_VENDOR VARCHAR2,P_DATA_TYPE VARCHAR2, P_DATA_TYPE_LENGTH NUMBER, P_DATA_TYPE_SCALE NUMBER) return VARCHAR2;
  function GET_MILLISECONDS(P_START_TIME TIMESTAMP, P_END_TIME TIMESTAMP) return NUMBER;
  function SERIALIZE_TABLE(P_TABLE T_VC4000_TABLE,P_DELIMITER VARCHAR2 DEFAULT ',')  return CLOB;
  procedure COMPARE_SCHEMAS(P_SOURCE_SCHEMA VARCHAR2, P_TARGET_SCHEMA VARCHAR2);

end;
/
--
set TERMOUT on
--
show errors
--
@@SET_TERMOUT
--
create or replace package body YADAMU_IMPORT
as
--
  G_INCLUDE_DATA    BOOLEAN := TRUE;
  G_INCLUDE_DDL     BOOLEAN := FALSE;

function GET_MILLISECONDS(P_START_TIME TIMESTAMP, P_END_TIME TIMESTAMP)
return NUMBER
as
  V_INTERVAL INTERVAL DAY TO SECOND := P_END_TIME - P_START_TIME;
begin
  return (((((((extract(DAY from V_INTERVAL) * 24)  + extract(HOUR from  V_INTERVAL)) * 60 ) + extract(MINUTE from V_INTERVAL)) * 60 ) + extract(SECOND from  V_INTERVAL)) * 1000);
end;
--
function SERIALIZE_TABLE(P_TABLE T_VC4000_TABLE,P_DELIMITER VARCHAR2 DEFAULT ',')
return CLOB
as
  V_LIST CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_LIST,TRUE,DBMS_LOB.CALL);
  if ((P_TABLE is not NULL) and (P_TABLE.count > 0)) then
    for i in P_TABLE.first .. P_TABLE.last loop
      if (i > 1) then
        DBMS_LOB.WRITEAPPEND(V_LIST,length(P_DELIMITER),P_DELIMITER);
      end if;
      DBMS_LOB.WRITEAPPEND(V_LIST,length(P_TABLE(i)),P_TABLE(i));
    end loop;
  end if;
  return V_LIST;
end;
--
function DESERIALIZE_TABLE(P_LIST CLOB)
return T_VC4000_TABLE
as
  V_TABLE T_VC4000_TABLE;
begin
  select cast(collect(x.COLUMN_VALUE.getStringVal()) as T_VC4000_TABLE) IMPORT_COLUMN_LIST
   into V_TABLE
   from XMLTABLE(P_LIST) x;
   return V_TABLE;
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
function IMPORT_VERSION return NUMBER deterministic
as
begin
  return C_VERSION_NUMBER;
end;
--
PROCEDURE TRACE_OPERATION(P_SQL_STATEMENT CLOB)
as
begin
  RESULTS_CACHE.extend();
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
exception
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  $ELSE
  when YADAMU_UTILITIES.JSON_OVERFLOW then
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
  when others then
    RAISE;
end;
--
procedure LOG_DDL_OPERATION(P_TABLE_NAME VARCHAR2, P_DDL_OPERATION CLOB)
as
begin
  RESULTS_CACHE.extend;
  select JSON_OBJECT('ddl' value JSON_OBJECT('tableName' value P_TABLE_NAME, 'sqlStatement' value P_DDL_OPERATION
                     $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                     returning CLOB) returning CLOB)
                     $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                     returning  VARCHAR2(32767)) returning  VARCHAR2(32767))
                     $ELSE
                     returning  VARCHAR2(4000)) returning  VARCHAR2(4000))
                     $END
    into RESULTS_CACHE(RESULTS_CACHE.count)
    from DUAL;
exception
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  $ELSE
  when YADAMU_UTILITIES.JSON_OVERFLOW then
    RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                            YADAMU_UTILITIES.KVP_TABLE(
                                              YADAMU_UTILITIES.KVJ(
                                                'dll',
                                                YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                                  YADAMU_UTILITIES.KVP_TABLE(
                                                    YADAMU_UTILITIES.KVC('sqlStatement',P_DDL_OPERATION),
                                                    YADAMU_UTILITIES.KVS('tableName',P_TABLE_NAME)
                                                  )
                                                )
                                              )
                                            )
                                          );
  $END
  when others then
    RAISE;
end;
--
procedure LOG_DML_OPERATION(P_TABLE_NAME VARCHAR2, P_DML_OPERATION CLOB, P_ROW_COUNT NUMBER, P_ELAPSED_TIME NUMBER)
as
begin
  RESULTS_CACHE.extend;
  select JSON_OBJECT('dml' value JSON_OBJECT('tableName' value P_TABLE_NAME, 'sqlStatement' value P_DML_OPERATION, 'rowCount' value P_ROW_COUNT, 'elapsedTime' value P_ELAPSED_TIME
               $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
               returning CLOB) returning CLOB)
               $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
               returning  VARCHAR2(32767)) returning  VARCHAR2(32767))
               $ELSE
               returning  VARCHAR2(4000)) returning  VARCHAR2(4000))
               $END
          into RESULTS_CACHE(RESULTS_CACHE.count)
          from DUAL;
exception
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  $ELSE
  when YADAMU_UTILITIES.JSON_OVERFLOW then
    RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                            YADAMU_UTILITIES.KVP_TABLE(
                                              YADAMU_UTILITIES.KVJ(
                                                'dml',
                                                YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                                  YADAMU_UTILITIES.KVP_TABLE(
                                                    YADAMU_UTILITIES.KVC('sqlStatement',P_DML_OPERATION),
                                                    YADAMU_UTILITIES.KVS('tableName',P_TABLE_NAME)
                                                  )
                                                )
                                              )
                                            )
                                          );
  $END
  when others then
    RAISE;
end;
--
procedure LOG_ERROR(P_SEVERITY VARCHAR2, P_TABLE_NAME VARCHAR2,P_SQL_STATEMENT CLOB,P_SQLCODE NUMBER, P_SQLERRM VARCHAR2, P_STACK CLOB)
as
begin
  RESULTS_CACHE.extend;
  select JSON_OBJECT('error' value JSON_OBJECT('severity' value P_SEVERITY, 'tableName' value P_TABLE_NAME, 'sqlStatement' value P_SQL_STATEMENT, 'code' value P_SQLCODE, 'msg' value P_SQLERRM, 'details' value P_STACK
                     $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                     returning CLOB) returning CLOB)
                     $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                     returning  VARCHAR2(32767)) returning  VARCHAR2(32767))
                     $ELSE
                     returning  VARCHAR2(4000)) returning  VARCHAR2(4000))
                     $END
     into RESULTS_CACHE(RESULTS_CACHE.count)
     from DUAL;
exception
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  $ELSE
  when YADAMU_UTILITIES.JSON_OVERFLOW then
    RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                            YADAMU_UTILITIES.KVP_TABLE(
                                              YADAMU_UTILITIES.KVJ(
                                                'error',
                                                YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                                  YADAMU_UTILITIES.KVP_TABLE(
                                                    YADAMU_UTILITIES.KVC('sqlStatement',P_SQL_STATEMENT),
                                                    YADAMU_UTILITIES.KVS('severity',P_SEVERITY),
                                                    YADAMU_UTILITIES.KVS('tableName',P_TABLE_NAME),
                                                    YADAMU_UTILITIES.KVN('code',P_SQLCODE),
                                                    YADAMU_UTILITIES.KVS('msg',P_SQLERRM),
                                                    YADAMU_UTILITIES.KVS('details',P_STACK)
                                                  )
                                                )
                                              )
                                            )
                                          );
  $END
  when others then
    RAISE;
end;
--
procedure LOG_INFO(P_PAYLOAD CLOB)
as
--
-- ### Issue with Large Payloads (>32k or >4k)
--
begin
  RESULTS_CACHE.extend;
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  select JSON_OBJECT('info' value TREAT(P_PAYLOAD as JSON) returning CLOB)
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  select JSON_OBJECT('info' value JSON_QUERY(P_PAYLOAD,'$' returning VARCHAR2(32767))  returning VARCHAR2(32767))
  $ELSE
  select JSON_OBJECT('info' value JSON_QUERY(P_PAYLOAD,'$' returning VARCHAR2(4000)) returning VARCHAR2(4000))
  $END
    into RESULTS_CACHE(RESULTS_CACHE.count)
    from DUAL;
exception
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  $ELSE
  when YADAMU_UTILITIES.JSON_OVERFLOW then
    RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                            YADAMU_UTILITIES.KVP_TABLE(
                                              YADAMU_UTILITIES.KVJ(
                                                'info',
                                                P_PAYLOAD
                                              )
                                            )
                                          );
  $END
  when others then
    RAISE;
end;
--
procedure LOG_MESSAGE(P_PAYLOAD CLOB)
as
begin
  RESULTS_CACHE.extend;
  select JSON_OBJECT('message' value P_PAYLOAD
                     $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                     returning CLOB)
                     $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                     returning  VARCHAR2(32767))
                     $ELSE
                     returning  VARCHAR2(4000))
                     $END
     into RESULTS_CACHE(RESULTS_CACHE.count)
     from DUAL;
exception
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  $ELSE
  when YADAMU_UTILITIES.JSON_OVERFLOW then
    RESULTS_CACHE(RESULTS_CACHE.count) := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                                            YADAMU_UTILITIES.KVP_TABLE(
                                              YADAMU_UTILITIES.KVJ(
                                                'message',
                                                P_PAYLOAD
                                              )
                                            )
                                          );
  $END
  when others then
    RAISE;
end;
--
function MAP_FOREIGN_DATATYPE(P_SOURCE_VENDOR VARCHAR2, P_DATA_TYPE VARCHAR2, P_DATA_TYPE_LENGTH NUMBER, P_DATA_TYPE_SCALE NUMBER)
return VARCHAR2
as
begin
  case 
    when P_SOURCE_VENDOR = 'Oracle' then
       return UPPER(P_DATA_TYPE);
    when P_SOURCE_VENDOR = 'MSSQLSERVER' then
      case 
        -- exact numbers
        when P_DATA_TYPE = 'tinyint' then
           return 'NUMBER(3,0)';
        when P_DATA_TYPE = 'smallint' then
           return 'NUMBER(5,0)';
        when P_DATA_TYPE = 'mediumint' then
           return 'NUMBER(7,0)';
        when P_DATA_TYPE = 'int' then
           return 'NUMBER(10,0)';
        when P_DATA_TYPE = 'bigint' then
           return 'NUMBER(19,0)';
        when P_DATA_TYPE = 'money' then
           return 'NUMBER(19,4)';
        when P_DATA_TYPE = 'decimal' then
           return 'NUMBER';
        when P_DATA_TYPE = 'numeric' then
           return 'NUMBER';
        when P_DATA_TYPE = 'smallmoney' then
           return 'NUMBER(10,4)';
        -- binary numbers
        when P_DATA_TYPE = 'real' then
           return 'BINARY_FLOAT';
        when P_DATA_TYPE = 'float' then
           return 'BINARY_DOUBLE';
        -- date /time data types
        when P_DATA_TYPE = 'date' then
           return 'DATE';
        when P_DATA_TYPE = 'time' then
           case 
             when P_DATA_TYPE_LENGTH = 0 then
               return 'DATETIME';
             else  
               return 'TIMESTAMP';
            end case;
        when P_DATA_TYPE = 'datetime' then
           return 'TIMESTAMP(3)';
        when P_DATA_TYPE = 'datetime2' then
           return 'TIMESTAMP';
        when P_DATA_TYPE = 'datetimeoffset' then
           return 'TIMESTAMP(' || P_DATA_TYPE_LENGTH || ') WITH TIME ZONE';
        -- text data Types
        when P_DATA_TYPE = 'ntext' then
           return 'NCLOB';
        when P_DATA_TYPE = 'nvarchar'and (P_DATA_TYPE_LENGTH = -1) then
           return 'NCLOB';
        when P_DATA_TYPE = 'nvarchar'and (P_DATA_TYPE_LENGTH > 2000) then
          -- Cannot create NVARCHAR2(2001) at least with AL32UTF8 Database Character Set
           return 'NCLOB';
        when P_DATA_TYPE = 'nvarchar' then
           return 'NVARCHAR2';
        when P_DATA_TYPE = 'text' then
           return 'CLOB';
        when P_DATA_TYPE = 'varchar'  and (P_DATA_TYPE_LENGTH = -1) then
           return 'CLOB';
        when P_DATA_TYPE = 'varchar' then
           return 'VARCHAR2';
        -- binary data types 
        when P_DATA_TYPE = 'binary' and (P_DATA_TYPE_LENGTH = -1) then
           return 'BLOB';
        when P_DATA_TYPE = 'bit' then
         -- Map to Virtual Data Type BOOLEAN so we can process content correclty later
           return 'BOOLEAN';
        when P_DATA_TYPE = 'binary' then
           return 'RAW';
        when P_DATA_TYPE = 'image' then
           return 'BLOB';
        when P_DATA_TYPE = 'varbinary' and (P_DATA_TYPE_LENGTH = -1) then
           return 'BLOB';
        when P_DATA_TYPE = 'varbinary' then
           return 'RAW';
        -- specialist data types
        when P_DATA_TYPE = 'xml' then
          return 'XMLTYPE';
        when P_DATA_TYPE = 'hierarchyid' then
           return 'VARCHAR2(4000)';
        when P_DATA_TYPE = 'uniqueidentifier' then
           return 'VARCHAR2(36)';
        when P_DATA_TYPE = 'geography' then
           return 'GEOMETRY';
        when P_DATA_TYPE = 'geometry' then
           return 'GEOMETRY';
        else
          return UPPER(P_DATA_TYPE);
      end case;
    when P_SOURCE_VENDOR = 'Postgres' then
      case
        when P_DATA_TYPE = 'character' then
           return 'VARCHAR2';
        when P_DATA_TYPE = 'character varying' then
          case 
            when P_DATA_TYPE_LENGTH is NULL then
              return 'CLOB';
            when P_DATA_TYPE_LENGTH < 2001 then
              return 'VARCHAR2';
            else
              return 'CLOB';
          end case;
        when P_DATA_TYPE = 'smallint' then
          return 'NUMBER(5,0)';
        when P_DATA_TYPE = 'integer' then
          return 'NUMBER(10,0)';
        when P_DATA_TYPE = 'bigint' then
          return 'NUMBER(19,0)';
        when P_DATA_TYPE = 'real' then
           return 'BINARY_FLOAT';
        when P_DATA_TYPE = 'double precision' then
           return 'BINARY_DOUBLE';
        when P_DATA_TYPE = 'timestamp without time zone' then
           return 'TIMESTAMP(6)';
        when P_DATA_TYPE = 'time without time zone' then
           return 'TIMESTAMP(6)';
        when P_DATA_TYPE like 'interval%day%' then
           return 'INTERVAL DAY TO SECOND';
        when P_DATA_TYPE like 'interval%YEAR%' then
           return 'INTERVAL YEAR TO MONTH';
        when P_DATA_TYPE = 'text' then
           return 'CLOB';
        when P_DATA_TYPE = 'bytea' then
          case 
            when P_DATA_TYPE_LENGTH < 2001 then
              return 'RAW';
            else
              return 'BLOB';
          end case;
        when P_DATA_TYPE = 'xml' then
           return 'XMLTYPE';
        when P_DATA_TYPE = 'geometry' then
           return 'GEOMETRY';
        when P_DATA_TYPE = 'numeric' then
           return 'NUMBER';
        else
          return UPPER(P_DATA_TYPE);  
      end case;
    when ((P_SOURCE_VENDOR = 'MySQL') or (P_SOURCE_VENDOR = 'MariaDB')) then
      case
        -- ExactNumbers
        when P_DATA_TYPE = 'tinyint' then
          return 'NUMBER(3,0)';
        when P_DATA_TYPE = 'smallint' then
          return 'NUMBER(5,0)';
        when P_DATA_TYPE = 'mediumint' then
          return 'NUMBER(7,0)';
        when P_DATA_TYPE = 'int' then
          return 'NUMBER(10,0)';
        when P_DATA_TYPE = 'bigint' then
          return 'NUMBER(19,0)';
        when P_DATA_TYPE = 'decimal'  then
          return 'NUMBER';
        when P_DATA_TYPE = 'numeric' then
          return 'NUMBER';
        -- Binary Numbers
        when P_DATA_TYPE = 'float' then
          return 'BINARY_FLOAT';
        when P_DATA_TYPE = 'double' then
          return 'BINARY_DOUBLE';
        -- Text Data Types           
        when P_DATA_TYPE = 'varchar' then
          return 'VARCHAR2';
        when P_DATA_TYPE = 'tinytext' then
          return 'VARCHAR2';
         when P_DATA_TYPE = 'mediumtext' then
          return 'CLOB';
        when P_DATA_TYPE = 'text' then
          return 'CLOB';
        when P_DATA_TYPE = 'longtext' then
          return 'CLOB';
        -- Binary Data types
        when P_DATA_TYPE = 'bit' then
          return 'RAW(1)';
        when P_DATA_TYPE = 'binary' then
          -- return 'RAW(' || CEIL(P_DATA_TYPE_LENGTH/8) || ')';
          return 'RAW';
        when P_DATA_TYPE = 'varbinary' then
          return 'RAW';
        when P_DATA_TYPE = 'tinyblob' then
           return 'RAW';
        when P_DATA_TYPE = 'mediumblob' then
           return 'BLOB';
        when P_DATA_TYPE = 'longblob' then
           return 'BLOB';
        -- Date/TIme Data Types
        when P_DATA_TYPE = 'date' then
           return 'DATE';
        when P_DATA_TYPE = 'time' then
           return 'DATE';
        when P_DATA_TYPE = 'datetime' then
           return 'TIMESTAMP';
        -- Special Data Types   
        when P_DATA_TYPE = 'enum' then
           return 'VARCHAR2(512)';
        when P_DATA_TYPE = 'geography' then
           return 'GEOMETRY';
        when P_DATA_TYPE = 'geometry' then
           return 'GEOMETRY';
        when P_DATA_TYPE = 'set' then
           return 'VARCHAR2(512)';
        when P_DATA_TYPE = 'year' then
           return 'NUMBER(4)';
        else
          return UPPER(P_DATA_TYPE);
      end case;
    when P_SOURCE_VENDOR = 'MariaDB' then
      -- MariaDB and MySQL currentyl share the same mappings
      return UPPER(P_DATA_TYPE);
    else
      return UPPER(P_DATA_TYPE);
  end case;
end;
--
procedure APPEND_DESERIALIZATION_FUNCTIONS(P_DESERIALIZATION_FUNCTIONS T_VC4000_TABLE, P_SQL_STATEMENT IN OUT CLOB)
as
  V_IDX   PLS_INTEGER;
begin
  if (P_DESERIALIZATION_FUNCTIONS.count > 0) then
    DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB('WITH' || YADAMU_UTILITIES.C_NEWLINE));
    for V_IDX in 1.. P_DESERIALIZATION_FUNCTIONS.count loop
      DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(P_DESERIALIZATION_FUNCTIONS(V_IDX)));
    end loop;
  end if;
end;
--
function GENERATE_SQL(P_SOURCE_VENROR VARCHAR2,P_TARGET_SCHEMA VARCHAR2, P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2, P_COLUMN_LIST CLOB, P_DATA_TYPE_ARRAY CLOB, P_SIZE_CONSTRAINTS CLOB, P_XMLTYPE_STORAGE_MODEL VARCHAR2 DEFAULT 'CLOB')
return TABLE_INFO_RECORD
as
  CURSOR generateStatementComponents
  is
  with 
  "SOURCE_TABLE_DEFINITIONS" 
  as (
    select c."KEY" IDX
          ,c.VALUE "COLUMN_NAME"
          ,t.VALUE "DATA_TYPE"
          ,case
             when s.VALUE = '' then
                NULL
             when INSTR(s.VALUE,',') > 0 then
                SUBSTR(s.VALUE,1,INSTR(s.VALUE,',')-1)
             else
               s.VALUE
           end "DATA_TYPE_LENGTH"
          ,case
             when INSTR(s.VALUE,',') > 0 then
                SUBSTR(s.VALUE, INSTR(s.VALUE,',')+1)
             else
               NULL
           end "DATA_TYPE_SCALE"
          ,case
             when (t.VALUE LIKE '"%"."%"')  then
               -- Data Type is a schema qualified object type
               case 
                 -- Remap types defined by the source schema to the target schema.
                 when (t.VALUE LIKE '"' || P_TABLE_OWNER || '"."%"') then
                   P_TARGET_SCHEMA
                 else
                   SUBSTR(t.VALUE,2,INSTR(t.VALUE,'.')-3)
               end
             else 
               NULL
           end "TYPE_OWNER"     
          ,case
             when (t.VALUE LIKE '"%"."%"') then
                RTRIM(SUBSTR(t.VALUE,INSTR(t.VALUE,'.')+2),'"')
             else 
               NULL 
           end "TYPE_NAME"
         from JSON_TABLE('[' || P_COLUMN_LIST || ']','$[*]' COLUMNS "KEY" FOR ORDINALITY, VALUE PATH '$') c
             ,JSON_TABLE(P_DATA_TYPE_ARRAY,'$[*]' COLUMNS "KEY" FOR ORDINALITY, VALUE PATH '$') t
             ,JSON_TABLE(P_SIZE_CONSTRAINTS,'$[*]' COLUMNS "KEY" FOR ORDINALITY, VALUE PATH '$') s
        where (c."KEY" = t."KEY") and (c."KEY" = s."KEY")
  ),
  "TARGET_TABLE_DEFINITIONS" 
  as (
    select st.*
          ,MAP_FOREIGN_DATATYPE(P_SOURCE_VENROR,"DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE") TARGET_DATA_TYPE
          ,case
             -- Probe rather than Join since most rows are not objects.
             when (TYPE_NAME is not null) then
                case 
                      when (TYPE_OWNER = P_TABLE_OWNER) then
                        -- Original Type belonged to the same schema as the table. Resolve the type in the target Schema
                        (select 1 from ALL_TYPES at where OWNER = P_TARGET_SCHEMA and at.TYPE_NAME = st.TYPE_NAME)
                      else
                        -- Original Type belonged to different schema as the table. Resolve the type in the original Schema
                        (select 1 from ALL_TYPES at where at.OWNER = st.TYPE_OWNER and at.TYPE_NAME = st.TYPE_NAME)
                    end
             else
               NULL
           end "TYPE_EXISTS"
      from "SOURCE_TABLE_DEFINITIONS" st
  ),
  "EXTENDED_TABLE_DEFINITIONS"
  as (
  select tt.*,
         case 
           when TYPE_EXISTS = 1 then 
             OBJECT_SERIALIZATION.DESERIALIZE_TYPE(TYPE_OWNER,TYPE_NAME) 
          else 
            NULL 
          end  "DESERIALIZATION_FUNCTION"
    from "TARGET_TABLE_DEFINITIONS" tt
  )
--
  $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
--
  select SERIALIZE_TABLE(
           cast(collect(
                        '"' || COLUMN_NAME || '" ' ||
                        case
                          when TYPE_EXISTS = 1 then
                            '"' || TYPE_OWNER || '"."' || TYPE_NAME || '"'
                          when TYPE_NAME is not NULL then
                            'CLOB'
                          -- Type Exist is NULL.
                          when TARGET_DATA_TYPE = 'GEOMETRY' then
                            '"MDSYS"."SDO_GEOMETRY"'
                          when TARGET_DATA_TYPE = 'JSON' then
                            -- BLOB results in Error: ORA-40479: internal JSON serializer error during export operations.
                            'CLOB CHECK ("' || COLUMN_NAME || '" IS JSON)'
                          when TARGET_DATA_TYPE = 'BOOLEAN' then
                            'RAW(1)'
                          when TARGET_DATA_TYPE in ('DATE','DATETIME','CLOB','NCLOB','BLOB','XMLTYPE','ROWID','UROWID','BINARY_FLOAT','BINARY_DOUBLE') or (TARGET_DATA_TYPE LIKE 'INTERVAL%') or (TARGET_DATA_TYPE like '% TIME ZONE') or (TARGET_DATA_TYPE LIKE '%(%)') then
                            TARGET_DATA_TYPE
                          when DATA_TYPE_SCALE is not NULL then
                            TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
                          when DATA_TYPE_LENGTH  is not NULL then
                            TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH|| ')'
                          else
                            TARGET_DATA_TYPE
                        end || YADAMU_UTILITIES.C_NEWLINE
                        order by IDX
                )
                as T_VC4000_TABLE
           )
         ) COLUMNS_CLAUSE
        ,SERIALIZE_TABLE(
           cast(collect(
                        -- Cast JSON representation back into SQL data type where implicit coversion does happen or results in incorrect results
                        case
                          when ((TARGET_DATA_TYPE = 'GEOMETRY') or (TARGET_DATA_TYPE = '"MDSYS"."SDO_GEOMETRY"')) then
                            'case when "' || COLUMN_NAME || '" is NULL then NULL else SDO_UTIL.FROM_WKTGEOMETRY("' || COLUMN_NAME || '") end'
                          when TYPE_EXISTS = 1 then
                            '"#' || TYPE_NAME || '"("' || COLUMN_NAME || '")'
                          when TARGET_DATA_TYPE = 'BOOLEAN' then
                            'HEXTORAW(case when "' || COLUMN_NAME || '" = ''true'' then ''1'' else ''0'' end)'
                          when TARGET_DATA_TYPE = 'BFILE' then
                            'OBJECT_SERIALIZATION.DESERIALIZE_BFILE("' || COLUMN_NAME || '")' 
                          when (TARGET_DATA_TYPE = 'XMLTYPE') then
                            -- Cannot map directly to XMLTYPE constructor as we need to test for NULL. 
                            'case when "' || COLUMN_NAME || '" is NULL then NULL else XMLTYPE("' || COLUMN_NAME || '") end'
                          when (TARGET_DATA_TYPE = 'ANYDATA') then
                            -- ### TODO - Better deserialization of ANYDATA.
                            'case when "' || COLUMN_NAME || '" is NULL then NULL else ANYDATA.convertVARCHAR2("' || COLUMN_NAME || '") end' 
                          when TARGET_DATA_TYPE = 'BLOB' then
                            $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                            'OBJECT_SERIALIZATION.DESERIALIZE_HEX_BLOB("' || COLUMN_NAME || '")'
                            $ELSE
                            'case when "' || COLUMN_NAME || '" is NULL then NULL else HEXTORAW("' || COLUMN_NAME || '") end'
                            $END
                          else
                            '"' || COLUMN_NAME || '"'
                        end
                        order by IDX
               )
               as T_VC4000_TABLE
           )
         ) INSERT_SELECT_LIST
        ,SERIALIZE_TABLE(
           cast(collect('"' ||
                        case
                          when TYPE_EXISTS = 1 then
                            '\"' || TYPE_OWNER || '\".\"' || TYPE_NAME || '\"'
                          when TYPE_NAME is not NULL then
                            'CLOB'
                          -- Type Exist is NULL.
                          else
                            replace(TARGET_DATA_TYPE,'"','\"')
                        end ||
                        '"'
                        order by IDX
               )
               as T_VC4000_TABLE
           )
         ) TARGET_DATA_TYPES
        ,SERIALIZE_TABLE(
           cast(collect(case
                          when (TARGET_DATA_TYPE = 'XMLTYPE') then
                            'XMLTYPE "' || COLUMN_NAME || '" STORE AS CLOB'
                          else
                            NULL
                        end
                        order by IDX
               )
               as T_VC4000_TABLE
           )
           ,YADAMU_UTILITIES.C_NEWLINE
         ) XMLTYPE_STORAGE_CLAUSES
        ,SERIALIZE_TABLE(
           cast(collect(
                        '"' || COLUMN_NAME || '" ' ||
                        case
                          when TYPE_EXISTS = 1 then
                            'CLOB'
                          when TARGET_DATA_TYPE  = 'GEOMETRY' then
                            'CLOB'
                          when TARGET_DATA_TYPE  = 'BOOLEAN' then
                            'VARCHAR2(5)'
                          when TARGET_DATA_TYPE = 'JSON' then
                            JSON_FEATURE_DETECTION.C_RETURN_TYPE || ' FORMAT JSON'
                          when TARGET_DATA_TYPE  = 'FLOAT' then
                            'NUMBER'
                          when TARGET_DATA_TYPE = 'BINARY_FLOAT' then
                            'VARCHAR2(29)'
                          when TARGET_DATA_TYPE = 'BINARY_DOUBLE' then
                            'VARCHAR2(53)'
                          when TARGET_DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE' then
                            'TIMESTAMP WITH TIME ZONE'
                          when TARGET_DATA_TYPE like 'RAW(%)' then
                            'VARCHAR2'
                          when TARGET_DATA_TYPE in ('CHAR','NCHAR','NVARCHAR2','RAW','BFILE','ROWID','UROWID') or (TARGET_DATA_TYPE like 'INTERVAL%') then
                            'VARCHAR2'
                          when TARGET_DATA_TYPE in ('XMLTYPE','ANYDATA','CLOB','NCLOB','BLOB','LONG','LONG RAW') or (TYPE_NAME is not NULL) then
                            JSON_FEATURE_DETECTION.C_RETURN_TYPE
                          when "TARGET_DATA_TYPE" in ('DATE','DATETIME') then
                            "TARGET_DATA_TYPE"
                          when "TARGET_DATA_TYPE" like 'TIMESTAMP%TIME ZONE' then
                            "TARGET_DATA_TYPE"
                          when "TARGET_DATA_TYPE"  LIKE '%(%)' then
                            "TARGET_DATA_TYPE"
                          when "DATA_TYPE_SCALE" is not NULL then
                            "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ',' || "DATA_TYPE_SCALE" || ')'
                          when "DATA_TYPE_LENGTH"  is not NULL then
                            "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ')'
                          else
                            "TARGET_DATA_TYPE"
                        end
                        || ' PATH ''$[' || (IDX - 1) || ']'' ERROR ON ERROR' || YADAMU_UTILITIES.C_NEWLINE
                        order by IDX
                )
                as T_VC4000_TABLE
           )
         ) COLUMN_PATTERNS
        , cast(collect( DESERIALIZATION_FUNCTION) as T_VC4000_TABLE) "DESERIALIZATION_FUNCTIONS"
    from "EXTENDED_TABLE_DEFINITIONS";
--
  $ELSE
--
  --
  -- cast(collect(...) causes ORA-22814: attribute or element value is larger than specified in type in 12.2
  --
  select '"' || COLUMN_NAME || '" ' ||
         case
           when TYPE_EXISTS = 1 then 
             '"' || TYPE_OWNER || '"."' || TYPE_NAME || '"'
           when TYPE_NAME is not NULL then
             'CLOB'  
           -- Type Exist is NULL.
           when TARGET_DATA_TYPE = 'GEOMETRY' then
             '"MDSYS"."SDO_GEOMETRY"'
           when TARGET_DATA_TYPE = 'JSON' then 
             -- BLOB results in Error: ORA-40479: internaly JSON serializer error during export operations.
             'CLOB CHECK ("' || COLUMN_NAME || '" IS JSON)'
           when TARGET_DATA_TYPE = 'BOOLEAN' then 
             'RAW(1)'
           when TARGET_DATA_TYPE in ('DATE','DATETIME','CLOB','NCLOB','BLOB','XMLTYPE','ROWID','UROWID','BINARY_FLOAT','BINARY_DOUBLE') or (TARGET_DATA_TYPE LIKE 'INTERVAL%') or (TARGET_DATA_TYPE like '% TIME ZONE') or (TARGET_DATA_TYPE LIKE '%(%)') then 
             TARGET_DATA_TYPE
           when DATA_TYPE_SCALE is not NULL then 
             TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
           when DATA_TYPE_LENGTH  is not NULL then 
             TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH|| ')'
           else
             TARGET_DATA_TYPE
         end || YADAMU_UTILITIES.C_NEWLINE 
         COLUMNS_CLAUSE
         /* Cast JSON representation back into SQL data type where implicit coversion does happen or results in incorrect results */
        ,case
           when ((TARGET_DATA_TYPE = 'GEOMETRY') or TARGET_DATA_TYPE = '"MDSYS"."SDO_GEOMETRY"')then
             'case when "' || COLUMN_NAME || '" is NULL then NULL else SDO_UTIL.FROM_WKTGEOMETRY("' || COLUMN_NAME || '") end'
           when TYPE_EXISTS = 1 then 
             '"#' || TYPE_NAME || '"("' || COLUMN_NAME || '")'
           when TARGET_DATA_TYPE = 'BOOLEAN' then 
             'HEXTORAW(case when "' || COLUMN_NAME || '" = ''true'' then ''1'' else ''0'' end)'
           when TARGET_DATA_TYPE = 'BFILE' then 
             'OBJECT_SERIALIZATION.DESERIALIZE_BFILE("' || COLUMN_NAME || '")'
           when (TARGET_DATA_TYPE = 'XMLTYPE') then 
              -- Cannot map directly to XMLTYPE constructor as we need to test for NULL. 
             'case when "' || COLUMN_NAME || '" is NULL then NULL else XMLTYPE("' || COLUMN_NAME || '") end' 
           when (TARGET_DATA_TYPE = 'ANYDATA') then 
             -- ### TODO - Better deserialization of ANYDATA.
             'case when "' || COLUMN_NAME || '" is NULL then NULL else ANYDATA.convertVARCHAR2("' || COLUMN_NAME || '") end'
           $IF NOT JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
           when TARGET_DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE' then
             -- Problems with ORA-1881
              'TO_TIMESTAMP_TZ("' || COLUMN_NAME || '",''YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM'')'
           $END
           when TARGET_DATA_TYPE = 'BLOB' then
             $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
             'OBJECT_SERIALIZATION.DESERIALIZE_HEX_BLOB("' || COLUMN_NAME || '")'
             $ELSE
             'case when "' || COLUMN_NAME || '" is NULL then NULL when substr("' || COLUMN_NAME || '",1,15) = ''SERIALIZE_BLOB_HEX:'' then NULL else HEXTORAW("' || COLUMN_NAME || '") end'
             $END
           else
             '"' || COLUMN_NAME || '"'
         end 
         INSERT_SELECT_LIST
        ,'"' ||
         case
           when TYPE_EXISTS = 1 then 
             '\"' || TYPE_OWNER || '\".\"' || TYPE_NAME || '\"'
           when TYPE_NAME is not NULL then
             -- Type Exist is NULL.
             'CLOB'
           else
             TARGET_DATA_TYPE
         end || '"' 
         TARGET_DATA_TYPES
        ,case 
           when TARGET_DATA_TYPE = 'XMLTYPE' then
             'XMLTYPE "' || COLUMN_NAME || '" STORE AS CLOB'
           else
             NULL
         end                
         XMLTYPE_STORAGE_CLAUSES
        ,'"' || COLUMN_NAME || '" ' ||
         case
           when TYPE_EXISTS = 1 then 
             JSON_FEATURE_DETECTION.C_RETURN_TYPE
           when TARGET_DATA_TYPE  = 'BOOLEAN' then
             'VARCHAR2(5)'
           when TARGET_DATA_TYPE  = 'GEOMETRY' then
             JSON_FEATURE_DETECTION.C_RETURN_TYPE
           when TARGET_DATA_TYPE = 'JSON' then 
             JSON_FEATURE_DETECTION.C_RETURN_TYPE || ' FORMAT JSON'
           when TARGET_DATA_TYPE  = 'FLOAT' then 
             'NUMBER'
           when TARGET_DATA_TYPE = 'BINARY_FLOAT' then 
             'VARCHAR2(29)'
           when TARGET_DATA_TYPE = 'BINARY_DOUBLE' then 
             'VARCHAR2(53)'
           $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
           when TARGET_DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE' then
             'TIMESTAMP WITH TIME ZONE'
           $ELSE
           when TARGET_DATA_TYPE like 'TIMESTAMP%' then
             -- Avoid problems with ORA-1881 and ORA-01866 when JSON_TABLE returns TIMESTAMP and TIMESTAMP_WITH_TZ. Use SQL to convert from String to Native data types
             'VARCHAR2(48)'
           $END
           when TARGET_DATA_TYPE like 'RAW(%)' then 
             'VARCHAR2'
           when TARGET_DATA_TYPE in ('CHAR','NCHAR','NVARCHAR2','RAW','BFILE','ROWID','UROWID') or (TARGET_DATA_TYPE like 'INTERVAL%') then 
             'VARCHAR2'
           when TARGET_DATA_TYPE in ('XMLTYPE','ANYDATA','CLOB','NCLOB','BLOB','LONG','LONG RAW') or (TYPE_NAME is not NULL) then 
             JSON_FEATURE_DETECTION.C_RETURN_TYPE
           when "TARGET_DATA_TYPE" in ('DATE','DATETIME') then 
             "TARGET_DATA_TYPE"
           when "TARGET_DATA_TYPE" like 'TIMESTAMP%TIME ZONE' then
             "TARGET_DATA_TYPE"
           when "TARGET_DATA_TYPE"  LIKE '%(%)' then 
             "TARGET_DATA_TYPE"
           when "DATA_TYPE_SCALE" is not NULL then 
             "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ',' || "DATA_TYPE_SCALE" || ')'
           when "DATA_TYPE_LENGTH"  is not NULL then 
             "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ')'
           else
             "TARGET_DATA_TYPE"
         end  || ' PATH ''$[' || (IDX - 1) || ']'' ERROR ON ERROR' || YADAMU_UTILITIES.C_NEWLINE 
         COLUMN_PATTERNS
        ,DESERIALIZATION_FUNCTION
    from "EXTENDED_TABLE_DEFINITIONS"
   order by IDX;

   V_COLUMNS_CLAUSE_TABLE      T_VC4000_TABLE;
   V_INSERT_SELECT_TABLE       T_VC4000_TABLE;
   V_COLUMN_PATTERNS_TABLE     T_VC4000_TABLE;
   V_TARGET_DATA_TYPES_TABLE   T_VC4000_TABLE;
   V_DESERIALIZATION_FUNCTIONS T_VC4000_TABLE;
   V_XML_STORAGE_TEMP          T_VC4000_TABLE;
   V_XML_STORAGE_TABLE         T_VC4000_TABLE;
--
   $END
--

  V_COLUMNS_CLAUSE            CLOB;
  V_INSERT_SELECT_LIST        CLOB;
  V_COLUMN_PATTERNS           CLOB;
  V_XML_STORAGE_CLAUSE        CLOB;
   
  V_DESERIALIZATIONS          T_VC4000_TABLE;

  V_SQL_FRAGMENT VARCHAR2(32767);
  V_INSERT_HINT  VARCHAR2(128) := '';

  C_CREATE_TABLE_BLOCK1 CONSTANT VARCHAR2(2048) :=
'declare
  TABLE_EXISTS EXCEPTION;
  PRAGMA EXCEPTION_INIT( TABLE_EXISTS , -00955 );
  V_STATEMENT CLOB := ''create table "';

   C_CREATE_TABLE_BLOCK2 CONSTANT VARCHAR2(2048) := ''';
begin
  execute immediate V_STATEMENT;
exception
  when TABLE_EXISTS then
    null;
  when others then  
    RAISE;
end;';

  V_DDL_STATEMENT     CLOB := NULL;
  V_DML_STATEMENT     CLOB;
  V_TARGET_DATA_TYPES CLOB;
  V_RESULTS           TABLE_INFO_RECORD;
  
  V_EXISTING_TABLE    PLS_INTEGER := 0; 
begin

  DBMS_LOB.CREATETEMPORARY(V_DML_STATEMENT,TRUE,DBMS_LOB.SESSION);
--
  $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
--
   -- Cursor only generates one row (Aggregration Operation),
  for o in generateStatementComponents loop
    V_COLUMNS_CLAUSE         := o.COLUMNS_CLAUSE;
    V_INSERT_SELECT_LIST     := o.INSERT_SELECT_LIST;
    V_TARGET_DATA_TYPES      := o.TARGET_DATA_TYPES;
    V_COLUMN_PATTERNS        := o.COLUMN_PATTERNS;
    V_XML_STORAGE_CLAUSE     := o.XMLTYPE_STORAGE_CLAUSES;
    
    select distinct COLUMN_VALUE 
      bulk collect into V_DESERIALIZATIONS
      from table(o.DESERIALIZATION_FUNCTIONS)
     where COLUMN_VALUE is not null;
    
  end loop;
--
  $ELSE
--
  open generateStatementComponents;
  fetch generateStatementComponents
        bulk collect into V_COLUMNS_CLAUSE_TABLE, V_INSERT_SELECT_TABLE, V_TARGET_DATA_TYPES_TABLE, V_XML_STORAGE_TEMP, V_COLUMN_PATTERNS_TABLE, V_DESERIALIZATION_FUNCTIONS;

  V_COLUMNS_CLAUSE := SERIALIZE_TABLE(V_COLUMNS_CLAUSE_TABLE);
  V_INSERT_SELECT_LIST := SERIALIZE_TABLE(V_INSERT_SELECT_TABLE);
  V_TARGET_DATA_TYPES := SERIALIZE_TABLE(V_TARGET_DATA_TYPES_TABLE);
  V_COLUMN_PATTERNS := SERIALIZE_TABLE(V_COLUMN_PATTERNS_TABLE);
  
  select COLUMN_VALUE
    bulk collect into V_XML_STORAGE_TABLE
    from table (V_XML_STORAGE_TEMP)
   where COLUMN_VALUE is not NULL;

  V_XML_STORAGE_CLAUSE := SERIALIZE_TABLE(V_XML_STORAGE_TABLE,YADAMU_UTILITIES.C_NEWLINE);
  
  select distinct COLUMN_VALUE
    bulk collect into V_DESERIALIZATIONS
    from table(V_DESERIALIZATION_FUNCTIONS)
   where COLUMN_VALUE is not null;

--
  $END
--
  begin 
    select 1
      into V_EXISTING_TABLE
      from ALL_ALL_TABLES
     where TABLE_NAME = P_TABLE_NAME
       and OWNER = P_TARGET_SCHEMA;
  exception
    when NO_DATA_FOUND then
      V_EXISTING_TABLE := 0;
    when OTHERS then
      RAISE;
  end;
  
  if (V_EXISTING_TABLE = 0) then 
    -- Table does not exist: Generate DDL Statement
    DBMS_LOB.CREATETEMPORARY(V_DDL_STATEMENT,TRUE,DBMS_LOB.SESSION);
    V_SQL_FRAGMENT := C_CREATE_TABLE_BLOCK1 || P_TARGET_SCHEMA || '"."' || P_TABLE_NAME || '" (' || YADAMU_UTILITIES.C_NEWLINE || ' ';
    DBMS_LOB.WRITEAPPEND(V_DDL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    DBMS_LOB.APPEND(V_DDL_STATEMENT,V_COLUMNS_CLAUSE);
    V_SQL_FRAGMENT := ')';
    DBMS_LOB.WRITEAPPEND(V_DDL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    if (P_XMLTYPE_STORAGE_MODEL = 'CLOB') then      
    V_SQL_FRAGMENT := ')';
      DBMS_LOB.WRITEAPPEND(V_DDL_STATEMENT,LENGTH(YADAMU_UTILITIES.C_NEWLINE),YADAMU_UTILITIES.C_NEWLINE);
      DBMS_LOB.APPEND(V_DDL_STATEMENT,V_XML_STORAGE_CLAUSE);
    end if;
    V_SQL_FRAGMENT := C_CREATE_TABLE_BLOCK2;
    DBMS_LOB.WRITEAPPEND(V_DDL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  end if;
  
  if ( V_DESERIALIZATIONS.count > 0) then
    V_INSERT_HINT := ' /*+ WITH_PLSQL */';
  end if;
  
  V_SQL_FRAGMENT := 'insert' || V_INSERT_HINT || ' into "' || P_TARGET_SCHEMA || '"."' || P_TABLE_NAME || '" (';
  DBMS_LOB.WRITEAPPEND(V_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  DBMS_LOB.APPEND(V_DML_STATEMENT,P_COLUMN_LIST);
  V_SQL_FRAGMENT :=  ')' || YADAMU_UTILITIES.C_NEWLINE;
  DBMS_LOB.WRITEAPPEND(V_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  
  APPEND_DESERIALIZATION_FUNCTIONS(V_DESERIALIZATIONS,V_DML_STATEMENT);
   
  V_SQL_FRAGMENT := 'select ';
  DBMS_LOB.WRITEAPPEND(V_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  DBMS_LOB.APPEND(V_DML_STATEMENT,V_INSERT_SELECT_LIST );
  V_SQL_FRAGMENT := YADAMU_UTILITIES.C_NEWLINE || '  from JSON_TABLE(:JSON,''$.data."' || P_TABLE_NAME || '"[*]''' || YADAMU_UTILITIES.C_NEWLINE || '         COLUMNS(' || YADAMU_UTILITIES.C_NEWLINE || ' ';
  DBMS_LOB.WRITEAPPEND(V_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  DBMS_LOB.APPEND(V_DML_STATEMENT,V_COLUMN_PATTERNS);
  DBMS_LOB.WRITEAPPEND(V_DML_STATEMENT,7,')) data');
  
  V_TARGET_DATA_TYPES := '[' || V_TARGET_DATA_TYPES || ']';
  
  V_RESULTS.DML := V_DML_STATEMENT;
  V_RESULTS.DDL := V_DDL_STATEMENT;
  V_RESULTS.TARGET_DATA_TYPES := V_TARGET_DATA_TYPES;
  
  return V_RESULTS;
  
exception
  when OTHERS then 
    LOG_INFO('[' || P_COLUMN_LIST || ']');
    LOG_INFO('[' || REPLACE(P_DATA_TYPE_ARRAY,'"."','"."') || ']');
    LOG_INFO('[' || P_SIZE_CONSTRAINTS || ']');
    LOG_ERROR(C_FATAL_ERROR,'YADAMU_IMPORT.GENERATE_STATEMENTS()',NULL,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    raise;
end;
--
procedure SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2)
as
  USER_NOT_FOUND EXCEPTION ; PRAGMA EXCEPTION_INIT( USER_NOT_FOUND , -01435 );
  V_SQL_STATEMENT CONSTANT VARCHAR2(4000) := 'ALTER SESSION SET CURRENT_SCHEMA = "' || P_TARGET_SCHEMA ||'"';
begin
  if (SYS_CONTEXT('USERENV','CURRENT_SCHEMA') <> P_TARGET_SCHEMA) then
    TRACE_OPERATION(V_SQL_STATEMENT);
    execute immediate V_SQL_STATEMENT;
  end if;
exception
  when others then
    LOG_ERROR(C_WARNING,'SET_CURRENT_SCHEMA()',V_SQL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
end;
--
function SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2)
return CLOB
as
  V_RESULTS CLOB;
begin
  RESULTS_CACHE := T_RESULTS_CACHE();
  SET_CURRENT_SCHEMA(P_TARGET_SCHEMA);
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(32767)) returning VARCHAR2(32767))
  $ELSE
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(4000)) returning VARCHAR2(4000))
  $END
    into V_RESULTS
    from table(RESULTS_CACHE);
  RESULTS_CACHE := T_RESULTS_CACHE();
  return V_RESULTS;
end;
--
procedure DISABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2)
as
  cursor getConstraints
  is
  select TABLE_NAME
        ,'ALTER TABLE "' || P_TARGET_SCHEMA || '"."' || TABLE_NAME  || '" DISABLE CONSTRAINT "' || CONSTRAINT_NAME || '"' DDL_OPERATION
    from ALL_CONSTRAINTS
   where OWNER = P_TARGET_SCHEMA
     AND constraint_type = 'R';
begin
  for c in getConstraints loop
    begin
      execute immediate c.DDL_OPERATION;
      LOG_DDL_OPERATION(c.TABLE_NAME,c.DDL_OPERATION);
    exception
      when others then
        LOG_ERROR(C_WARNING,c.TABLE_NAME,c.DDL_OPERATION,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    end;
  end loop;
end;
--
function DISABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2)
return CLOB
as
  V_RESULTS CLOB;
begin
  RESULTS_CACHE := T_RESULTS_CACHE();
  DISABLE_CONSTRAINTS(P_TARGET_SCHEMA);
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(32767)) returning VARCHAR2(32767))
  $ELSE
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(4000)) returning VARCHAR2(4000))
  $END
    into V_RESULTS
    from table(RESULTS_CACHE);
  RESULTS_CACHE := T_RESULTS_CACHE();
  return V_RESULTS;
end;
--
procedure ENABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2)
as
  cursor getConstraints
  is
  select TABLE_NAME
        ,'ALTER TABLE "' || P_TARGET_SCHEMA || '"."' || TABLE_NAME  || '" ENABLE CONSTRAINT "' || CONSTRAINT_NAME || '"' DDL_OPERATION
    from ALL_CONSTRAINTS
   where OWNER = P_TARGET_SCHEMA
     AND constraint_type = 'R';
begin
  for c in getConstraints loop
    begin
      execute immediate c.DDL_OPERATION;
      LOG_DDL_OPERATION(c.TABLE_NAME,c.DDL_OPERATION);
    exception
      when others then
        LOG_ERROR(C_WARNING,c.TABLE_NAME,c.DDL_OPERATION,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    end;
  end loop;
end;
--
function ENABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2)
return CLOB
as
  V_RESULTS CLOB;
begin
  RESULTS_CACHE := T_RESULTS_CACHE();
  ENABLE_CONSTRAINTS(P_TARGET_SCHEMA);
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(32767)) returning VARCHAR2(32767))
  $ELSE
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(4000)) returning VARCHAR2(4000))
  $END
    into V_RESULTS
    from table(RESULTS_CACHE);
  RESULTS_CACHE := T_RESULTS_CACHE();
  return V_RESULTS;
end;
--
procedure REFRESH_MATERIALIZED_VIEWS(P_TARGET_SCHEMA VARCHAR2)
as
  V_MVIEW_COUNT NUMBER;
  V_MVIEW_LIST  VARCHAR2(32767);
begin
  select COUNT(*), LISTAGG('"' || MVIEW_NAME || '"',',') WITHIN GROUP (order by MVIEW_NAME)
    into V_MVIEW_COUNT, V_MVIEW_LIST
    from ALL_MVIEWS
   where OWNER = P_TARGET_SCHEMA;

  if (V_MVIEW_COUNT > 0) then
    begin
      TRACE_OPERATION('DBMS_MVIEW.REFRESH('''|| V_MVIEW_LIST || ''')');
      DBMS_MVIEW.REFRESH(V_MVIEW_LIST);
    exception
      when others then
        LOG_ERROR(C_WARNING,'REFRESH_MATERIALIZED_VIEWS(''' || P_TARGET_SCHEMA || ''')',NULL,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    end;
  end if;
end;
--
function REFRESH_MATERIALIZED_VIEWS(P_TARGET_SCHEMA VARCHAR2)
return CLOB
as
  V_RESULTS CLOB;
begin
  RESULTS_CACHE := T_RESULTS_CACHE();
  REFRESH_MATERIALIZED_VIEWS(P_TARGET_SCHEMA);
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(32767)) returning VARCHAR2(32767))
  $ELSE
  select JSON_ARRAYAGG(JSON_QUERY (COLUMN_VALUE, '$' returning VARCHAR2(4000)) returning VARCHAR2(4000))
  $END
    into V_RESULTS
    from table(RESULTS_CACHE);
  RESULTS_CACHE := T_RESULTS_CACHE();
  return V_RESULTS;
end;
--
procedure MANAGE_MUTATING_TABLE(P_TABLE_NAME VARCHAR2, P_DML_STATEMENT IN OUT NOCOPY CLOB)
as
  V_SQL_STATEMENT         CLOB;
  V_SQL_FRAGMENT          VARCHAR2(1024);
  V_JSON_TABLE_OFFSET     NUMBER;

  V_START_TIME   TIMESTAMP(6);
  V_END_TIME     TIMESTAMP(6);
  V_ROW_COUNT    NUMBER;
begin
   V_SQL_FRAGMENT := 'declare' || YADAMU_UTILITIES.C_NEWLINE
                  || '  cursor JSON_TO_RELATIONAL' || YADAMU_UTILITIES.C_NEWLINE
                  || '  is' || YADAMU_UTILITIES.C_NEWLINE
                  || '  select *' || YADAMU_UTILITIES.C_NEWLINE
                  || '    from ';

   DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
   DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
   V_JSON_TABLE_OFFSET := DBMS_LOB.INSTR(P_DML_STATEMENT,' JSON_TABLE(');
   DBMS_LOB.COPY(V_SQL_STATEMENT,P_DML_STATEMENT,((DBMS_LOB.GETLENGTH(P_DML_STATEMENT)-V_JSON_TABLE_OFFSET)+1),DBMS_LOB.GETLENGTH(V_SQL_STATEMENT)+1,V_JSON_TABLE_OFFSET);

   V_SQL_FRAGMENT := ';' || YADAMU_UTILITIES.C_NEWLINE
                  || '  type T_JSON_TABLE_ROW_TAB is TABLE of JSON_TO_RELATIONAL%ROWTYPE index by PLS_INTEGER;' || YADAMU_UTILITIES.C_NEWLINE
                  || '  V_ROW_BUFFER T_JSON_TABLE_ROW_TAB;' || YADAMU_UTILITIES.C_NEWLINE
                  || '  V_ROW_COUNT PLS_INTEGER := 0;' || YADAMU_UTILITIES.C_NEWLINE
                  || 'begin' || YADAMU_UTILITIES.C_NEWLINE
                  || '  open JSON_TO_RELATIONAL;' || YADAMU_UTILITIES.C_NEWLINE
                  || '  loop' || YADAMU_UTILITIES.C_NEWLINE
                  || '    fetch JSON_TO_RELATIONAL' || YADAMU_UTILITIES.C_NEWLINE
                  || '    bulk collect into V_ROW_BUFFER LIMIT 25000;' || YADAMU_UTILITIES.C_NEWLINE
                  || '    exit when V_ROW_BUFFER.count = 0;' || YADAMU_UTILITIES.C_NEWLINE
                  || '    V_ROW_COUNT := V_ROW_COUNT + V_ROW_BUFFER.count;' || YADAMU_UTILITIES.C_NEWLINE
                  -- || '    forall i in 1 .. V_ROW_BUFFER.count' || YADAMU_UTILITIES.C_NEWLINE
                  || '    for i in 1 .. V_ROW_BUFFER.count loop' || YADAMU_UTILITIES.C_NEWLINE
                  || '      insert into "' || P_TABLE_NAME || '"' || YADAMU_UTILITIES.C_NEWLINE
                  || '      values V_ROW_BUFFER(i);'|| YADAMU_UTILITIES.C_NEWLINE
                  || '    end loop;'|| YADAMU_UTILITIES.C_NEWLINE
                  || '    commit;' || YADAMU_UTILITIES.C_NEWLINE
                  || '  end loop;' || YADAMU_UTILITIES.C_NEWLINE
                  || '  :2 := V_ROW_COUNT;' || YADAMU_UTILITIES.C_NEWLINE
                  || 'end;';

   DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
   P_DML_STATEMENT := V_SQL_STATEMENT;
end;
--
procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
as
  MUTATING_TABLE      EXCEPTION ; PRAGMA EXCEPTION_INIT( MUTATING_TABLE , -04091 );
  INVALID_IMPORT_FILE EXCEPTION ; PRAGMA EXCEPTION_INIT( INVALID_IMPORT_FILE , -40441 );
  XLARGE_CONTENT      EXCEPTION ; PRAGMA EXCEPTION_INIT( XLARGE_CONTENT , -40478 );

  V_CURRENT_SCHEMA    CONSTANT VARCHAR2(128) := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');

  V_START_TIME        TIMESTAMP(6);
  V_END_TIME          TIMESTAMP(6);
  V_ROWCOUNT          NUMBER;

  V_STATEMENT         CLOB;
  V_TARGET_DATA_TYPES CLOB;
  
  V_VALID_JSON        BOOLEAN := true;
  V_SQLCODE           NUMBER;
  V_SQLERRM           VARCHAR2(4000);
  V_DETAILS           VARCHAR2(4000);

  V_TABLE_INFO      TABLE_INFO_RECORD;

  CURSOR operationsList
  is
  select ROWNUM, TABLE_NAME, SOURCE_VENDOR, OWNER, COLUMN_LIST, DATA_TYPE_ARRAY, SIZE_CONSTRAINTS
      from JSON_TABLE(
           P_JSON_DUMP_FILE,
           '$'           
           COLUMNS(
             SOURCE_VENDOR           VARCHAR2(128)                      PATH '$.systemInformation.vendor',
             NESTED                                                     PATH '$.metadata.*'
               COLUMNS (
                 OWNER                        VARCHAR2(128)             PATH '$.owner'
               , TABLE_NAME                   VARCHAR2(128)             PATH '$.tableName'
               $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
               ,  COLUMN_LIST                          CLOB             PATH '$.columns'
               ,  DATA_TYPE_ARRAY                      CLOB FORMAT JSON PATH '$.dataTypes' 
               ,  SIZE_CONSTRAINTS                     CLOB FORMAT JSON PATH '$.sizeConstraints'
               $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
               ,  COLUMN_LIST               VARCHAR2(32767)             PATH '$.columns'
               ,  DATA_TYPE_ARRAY           VARCHAR2(32767) FORMAT JSON PATH '$.dataTypes'
               ,  SIZE_CONSTRAINTS          VARCHAR2(32767) FORMAT JSON PATH '$.sizeConstraints'
               $ELSE
               ,  COLUMN_LIST                VARCHAR2(4000)             PATH '$.columns'
               ,  DATA_TYPE_ARRAY            VARCHAR2(4000) FORMAT JSON PATH '$.dataTypes'
               ,  SIZE_CONSTRAINTS           VARCHAR2(4000) FORMAT JSON PATH '$.sizeConstraints'
               $END
             )
           )
         )
   where TABLE_NAME is not NULL;
   
   V_NOTHING_DONE BOOLEAN := TRUE;
begin
  -- LOG_INFO(JSON_OBJECT('startTime' value SYSTIMESTAMP, 'includeData' value G_INCLUDE_DATA, 'includeDDL' value G_INCLUDE_DDL));
  
  SET_CURRENT_SCHEMA(P_TARGET_SCHEMA);

  if (G_INCLUDE_DDL) then
    YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS(P_JSON_DUMP_FILE,P_TARGET_SCHEMA);
  end if;


  if (G_INCLUDE_DATA) then

  DISABLE_CONSTRAINTS(P_TARGET_SCHEMA) ;

    for o in operationsList loop
      V_NOTHING_DONE := FALSE;
      V_TABLE_INFO := GENERATE_SQL(o.SOURCE_VENDOR,P_TARGET_SCHEMA, o.OWNER, o.TABLE_NAME, o.COLUMN_LIST, o.DATA_TYPE_ARRAY, o.SIZE_CONSTRAINTS); 
      V_STATEMENT := V_TABLE_INFO.DDL;
      if (V_STATEMENT is not NULL) then
        begin
          execute immediate V_STATEMENT;
          LOG_DDL_OPERATION(o.TABLE_NAME,V_STATEMENT);
        exception
          when others then
            LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,V_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
        end;
      end if;
      if (V_VALID_JSON) then
        begin
          V_STATEMENT := V_TABLE_INFO.DML;
          V_START_TIME := SYSTIMESTAMP;
          execute immediate V_STATEMENT using P_JSON_DUMP_FILE;
          V_ROWCOUNT := SQL%ROWCOUNT;
          V_END_TIME := SYSTIMESTAMP;
          commit;
          LOG_DML_OPERATION(o.TABLE_NAME,V_STATEMENT,V_ROWCOUNT,GET_MILLISECONDS(V_START_TIME,V_END_TIME));
        exception
          when INVALID_IMPORT_FILE then
            -- If the Import File is not valid JSON do not process any further tables
            V_VALID_JSON := false;
            V_SQLCODE := SQLCODE;
            V_SQLERRM := SQLERRM;
            V_DETAILS := DBMS_UTILITY.FORMAT_ERROR_STACK();
            LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,V_STATEMENT,V_SQLCODE,V_SQLERRM,V_DETAILS);
          when MUTATING_TABLE then
            begin
              LOG_ERROR(C_WARNING,o.TABLE_NAME,V_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
              MANAGE_MUTATING_TABLE(o.TABLE_NAME,V_STATEMENT);
              V_START_TIME := SYSTIMESTAMP;
              execute immediate V_STATEMENT using P_JSON_DUMP_FILE, out V_ROWCOUNT;
              V_END_TIME := SYSTIMESTAMP;
              commit;
              LOG_DML_OPERATION(o.TABLE_NAME,V_STATEMENT,V_ROWCOUNT,GET_MILLISECONDS(V_START_TIME,V_END_TIME));
            exception
              when others then
                LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,V_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
             end;
          when XLARGE_CONTENT then
            LOG_ERROR(C_XLARGE_CONTENT,o.TABLE_NAME,V_STATEMENT,V_SQLCODE,V_SQLERRM,V_DETAILS);
          when others then
            LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,V_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
        end;
      else 
        LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,'JSON_TABLE() operation skipped.',V_SQLCODE,V_SQLERRM,'Possible corrupt or truncated import file.');
      end if;
    end loop;
    
    if (V_NOTHING_DONE) then
      LOG_MESSAGE('Warning: No data imported');
    end if;

    ENABLE_CONSTRAINTS(P_TARGET_SCHEMA);
    REFRESH_MATERIALIZED_VIEWS(P_TARGET_SCHEMA);

    end if;
  SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
exception
  when OTHERS then
    SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
    RAISE;
end;
--
function GENERATE_STATEMENTS(P_METADATA IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return CLOB
as
  V_RESULTS         CLOB;

  V_TABLE_INFO      TABLE_INFO_RECORD;
  V_TABLE_INFO_JSON CLOB;

  V_FRAGMENT VARCHAR2(4000);
  
  cursor getStatements
  is
  /*
  select JSON_OBJECTAGG(
           TABLE_NAME,
           TREAT(GENERATE_STATEMENTS(SOURCE_VENDOR,P_TARGET_SCHEMA, OWNER, TABLE_NAME, COLUMN_LIST, DATA_TYPE_ARRAY, SIZE_CONSTRAINTS) as JSON) 
           returning CLOB
         )
    into V_RESULTS
  */
  select ROWNUM, TABLE_NAME, VENDOR, OWNER, COLUMN_LIST, DATA_TYPE_ARRAY, SIZE_CONSTRAINTS
    from JSON_TABLE(
           P_METADATA,
           '$'           
           COLUMNS(
             NESTED                                                     PATH '$.metadata.*'
               COLUMNS (
                 VENDOR                       VARCHAR2(128)             PATH '$.vendor'
               , OWNER                        VARCHAR2(128)             PATH '$.owner'
               , TABLE_NAME                   VARCHAR2(128)             PATH '$.tableName'
               $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
               ,  COLUMN_LIST                          CLOB             PATH '$.columns'
               ,  DATA_TYPE_ARRAY                      CLOB FORMAT JSON PATH '$.dataTypes' 
               ,  SIZE_CONSTRAINTS                     CLOB FORMAT JSON PATH '$.sizeConstraints'
               $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
               ,  COLUMN_LIST               VARCHAR2(32767)             PATH '$.columns'
               ,  DATA_TYPE_ARRAY           VARCHAR2(32767) FORMAT JSON PATH '$.dataTypes'
               ,  SIZE_CONSTRAINTS          VARCHAR2(32767) FORMAT JSON PATH '$.sizeConstraints'
               $ELSE
               ,  COLUMN_LIST                VARCHAR2(4000)             PATH '$.columns'
               ,  DATA_TYPE_ARRAY            VARCHAR2(4000) FORMAT JSON PATH '$.dataTypes'
               ,  SIZE_CONSTRAINTS           VARCHAR2(4000) FORMAT JSON PATH '$.sizeConstraints'
               $END
             )
           )
         )
   where TABLE_NAME is not NULL;

begin
   DBMS_LOB.CREATETEMPORARY(V_RESULTS,TRUE,DBMS_LOB.SESSION);
   DBMS_LOB.WRITEAPPEND(V_RESULTS,1,'{');

  for x in getStatements() loop
    if (x.ROWNUM > 1) then
      DBMS_LOB.WRITEAPPEND(V_RESULTS,1,',');
    end if;
    V_TABLE_INFO := GENERATE_SQL(x.VENDOR,P_TARGET_SCHEMA, x.OWNER, x.TABLE_NAME, x.COLUMN_LIST, x.DATA_TYPE_ARRAY, x.SIZE_CONSTRAINTS);
    V_FRAGMENT := '"' || x.TABLE_NAME || '" : ';
    DBMS_LOB.WRITEAPPEND(V_RESULTS,LENGTH(V_FRAGMENT),V_FRAGMENT);
    $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
       select JSON_OBJECT('ddl' value V_TABLE_INFO.DDL,
                          'dml' value V_TABLE_INFO.DML,
                          'targetDataTypes' value TREAT(V_TABLE_INFO.TARGET_DATA_TYPES AS JSON)
                          returning CLOB)
         into V_TABLE_INFO_JSON
         from DUAL;    
    $ELSE
    if (LENGTH(V_TABLE_INFO.DDL) + LENGTH(V_TABLE_INFO.DML) + LENGTH(V_TABLE_INFO.TARGET_DATA_TYPES) + 512 < JSON_FEATURE_DETECTION.C_MAX_STRING_SIZE) then
      select JSON_OBJECT('ddl' value V_TABLE_INFO.DDL, 
                         'dml' value V_TABLE_INFO.DML,
                         'targetDataTypes'
                          $IF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                          value JSON_QUERY(V_TABLE_INFO.TARGET_DATA_TYPES,'$' returning  VARCHAR2(32767))
                          returning  VARCHAR2(32767))
                          $ELSE
                          value JSON_QUERY(V_TABLE_INFO.TARGET_DATA_TYPES,'$' returning  VARCHAR2(4000) ERROR ON ERROR)
                          returning  VARCHAR2(4000))
                          $END
        into V_TABLE_INFO_JSON
        from DUAL;    
    else
      V_TABLE_INFO_JSON := YADAMU_UTILITIES.JSON_OBJECT_CLOB(
                             YADAMU_UTILITIES.KVP_TABLE(
                               YADAMU_UTILITIES.KVC('ddl',V_TABLE_INFO.DDL),
                               YADAMU_UTILITIES.KVC('dml',V_TABLE_INFO.DML),
                               YADAMU_UTILITIES.KVJ('targetDataTypes',V_TABLE_INFO.TARGET_DATA_TYPES)
                             )
                           );
    end if;      
    $END    
    DBMS_LOB.APPEND(V_RESULTS,V_TABLE_INFO_JSON);
   end loop;
   DBMS_LOB.WRITEAPPEND(V_RESULTS,1,'}');
   return V_RESULTS;  
end;
--
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
function GENERATE_IMPORT_LOG
return CLOB
as
   V_IMPORT_LOG CLOB;
begin
  select JSON_ARRAYAGG(TREAT (LOGENTRY as JSON) returning CLOB)
    into V_IMPORT_LOG
    from (
           select COLUMN_VALUE LOGENTRY
             from table(YADAMU_EXPORT_DDL.RESULTS_CACHE)
            union all
           select COLUMN_VALUE LOGENTRY
             from table(RESULTS_CACHE)
         );
  return V_IMPORT_LOG;
end;
--
$ELSE
--
function GENERATE_IMPORT_LOG
return CLOB
as
  V_IMPORT_LOG CLOB;
  V_FIRST_ITEM    BOOLEAN := TRUE;

  cursor getLogRecords
  is
  select COLUMN_VALUE LOGENTRY
    from table(YADAMU_EXPORT_DDL.RESULTS_CACHE)
   union all
  select COLUMN_VALUE LOGENTRY
        from table(RESULTS_CACHE);

begin
  DBMS_LOB.CREATETEMPORARY(V_IMPORT_LOG,TRUE,DBMS_LOB.CALL);

  DBMS_LOB.WRITEAPPEND(V_IMPORT_LOG,1,'[');

  for i in getLogRecords loop
    if (not V_FIRST_ITEM) then
      DBMS_LOB.WRITEAPPEND(V_IMPORT_LOG,1,',');
    end if;
    V_FIRST_ITEM := FALSE;
    DBMS_LOB.APPEND(V_IMPORT_LOG,i.LOGENTRY);
  end loop;

  DBMS_LOB.WRITEAPPEND(V_IMPORT_LOG,1,']');
  return V_IMPORT_LOG;
end;
--
$END
--
function IMPORT_JSON(P_JSON_DUMP_FILE IN OUT BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return CLOB
as
begin
  IMPORT_JSON(P_JSON_DUMP_FILE, P_TARGET_SCHEMA);
  return GENERATE_IMPORT_LOG();
exception
  when others then
    LOG_ERROR(C_FATAL_ERROR,'PROCEUDRE IMPORT_JSON',NULL,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    return GENERATE_IMPORT_LOG();
end;
--
procedure COMPARE_SCHEMAS(P_SOURCE_SCHEMA VARCHAR2, P_TARGET_SCHEMA VARCHAR2)
as
  TABLE_NOT_FOUND EXCEPTION;
  PRAGMA EXCEPTION_INIT( TABLE_NOT_FOUND , -00942 );

  cursor getTableList
  is
  select aat.TABLE_NAME
        ,LISTAGG(
		   case 
             when (DATA_TYPE = '"MDSYS"."SDO_GEOMETRY"') then
               'case when "' || COLUMN_NAME || '" is NULL then NULL else dbms_crypto.HASH(SDO_UTIL.FROMWKTGEOMETRY("' || COLUMN_NAME || '"),getCyptoHash) end'
		     when DATA_TYPE = 'BFILE' then
			   'case when "' || COLUMN_NAME || '" is NULL then NULL else OBJECT_SERIALIZATION.SERIALIZE_BFILE("' || COLUMN_NAME || '") end' 
		     when DATA_TYPE = 'XMLTYPE' then
		       'case when "' || COLUMN_NAME || '" is NULL then NULL else dbms_crypto.HASH(XMLSERIALIZE(CONTENT "' || COLUMN_NAME || '" as  BLOB ENCODING ''UTF-8''),getCyptoHash) end' 
			 when DATA_TYPE in ('BLOB')  then
		       'case when "' || COLUMN_NAME || '" is NULL then NULL else dbms_crypto.HASH("' || COLUMN_NAME || '",getCyptoHash) end'
			 when DATA_TYPE in ('CLOB','NCLOB')  then
		       'case when "' || COLUMN_NAME || '" is NULL then NULL when DBMS_LOB.GETLENGTH("' || COLUMN_NAME || '") = 0 then NULL else dbms_crypto.HASH("' || COLUMN_NAME || '",getCyptoHash) end'
             else
			   '"' || COLUMN_NAME || '"'
		   end,
		',') 
		 WITHIN GROUP (ORDER BY INTERNAL_COLUMN_ID, COLUMN_NAME) COLUMN_LIST
  from ALL_ALL_TABLES aat
       inner join ALL_TAB_COLS atc
	         on atc.OWNER = aat.OWNER
	        and atc.TABLE_NAME = aat.TABLE_NAME
       left outer join ALL_TYPES at
	                on at.TYPE_NAME = atc.DATA_TYPE
                   and at.OWNER = atc.DATA_TYPE_OWNER
	   left outer join ALL_MVIEWS amv
		            on amv.OWNER = aat.OWNER
		           and amv.MVIEW_NAME = aat.TABLE_NAME    
 where aat.STATUS = 'VALID'
   and aat.DROPPED = 'NO'
   and aat.TEMPORARY = 'N'
   and aat.EXTERNAL = 'NO'
   and aat.NESTED = 'NO'
   and aat.SECONDARY = 'N'
   and (aat.IOT_TYPE is NULL or aat.IOT_TYPE = 'IOT')
   and (
	    ((aat.TABLE_TYPE is NULL) and ((atc.HIDDEN_COLUMN = 'NO') and ((atc.VIRTUAL_COLUMN = 'NO') or ((atc.VIRTUAL_COLUMN = 'YES') and (atc.DATA_TYPE = 'XMLTYPE')))))
        or
	    ((aat.TABLE_TYPE is not NULL) and (COLUMN_NAME in ('SYS_NC_OID$','SYS_NC_ROWINFO$')))
	    or
		((aat.TABLE_TYPE = 'XMLTYPE') and (COLUMN_NAME in ('ACLOID', 'OWNERID')))
       )
	and aat.OWNER = P_SOURCE_SCHEMA
    and ((TYPECODE is NULL) or (TYPE_NAME = 'XMLTYPE'))
  group by aat.TABLE_NAME;
  
  V_SQL_STATEMENT     CLOB;
  P_SOURCE_COUNT      NUMBER := 0;
  P_TARGET_COUNT      NUMBER := 0;
  V_SQLERRM           VARCHAR2(4000);
begin
  
  begin
    execute immediate 'truncate table "SCHEMA_COMPARE_RESULTS"';
  exception
    when TABLE_NOT_FOUND then
      null;
    when others then  
      RAISE;
  end;
   
  for t in getTableList loop
    V_SQL_STATEMENT := 'insert /*+ WITH_PLSQL */ into SCHEMA_COMPARE_RESULTS ' || YADAMU_UTILITIES.C_NEWLINE
                    || 'with'|| YADAMU_UTILITIES.C_NEWLINE
					|| 'function getCyptoHash return number as begin return DBMS_CRYPTO.hash_sh256; end;'|| YADAMU_UTILITIES.C_NEWLINE
                    || ' select ''' || P_SOURCE_SCHEMA  || ''' ' || YADAMU_UTILITIES.C_NEWLINE
                    || '       ,''' || P_TARGET_SCHEMA  || ''' ' || YADAMU_UTILITIES.C_NEWLINE
                    || '       ,'''  || t.TABLE_NAME || ''' ' || YADAMU_UTILITIES.C_NEWLINE
                    || '       ,(select count(*) from "' || P_SOURCE_SCHEMA  || '"."' || t.TABLE_NAME || '")'  || YADAMU_UTILITIES.C_NEWLINE
                    || '       ,(select count(*) from "' || P_TARGET_SCHEMA  || '"."' || t.TABLE_NAME || '")'  || YADAMU_UTILITIES.C_NEWLINE
                    || '       ,(select count(*) from (SELECT ' || t.COLUMN_LIST || ' from "' || P_SOURCE_SCHEMA  || '"."' || t.TABLE_NAME || '" MINUS SELECT ' || t.COLUMN_LIST || ' from  "' || P_TARGET_SCHEMA  || '"."' || t.TABLE_NAME || '")) '  || YADAMU_UTILITIES.C_NEWLINE
                    || '       ,(select count(*) from (SELECT ' || t.COLUMN_LIST || ' from "' || P_TARGET_SCHEMA  || '"."' || t.TABLE_NAME || '" MINUS SELECT ' || t.COLUMN_LIST || ' from  "' || P_SOURCE_SCHEMA  || '"."' || t.TABLE_NAME || '")) '  || YADAMU_UTILITIES.C_NEWLINE
                    || '       ,null' || YADAMU_UTILITIES.C_NEWLINE
					|| '  from dual';
	begin
	  EXECUTE IMMEDIATE V_SQL_STATEMENT;
    exception 
      when OTHERS then
        V_SQLERRM := SQLERRM;					  
        begin 
          V_SQL_STATEMENT := 'select count(*) from "' || P_SOURCE_SCHEMA  || '"."' || t.TABLE_NAME || '"';
          execute immediate V_SQL_STATEMENT into P_SOURCE_COUNT;
        exception
          when others then
            V_SQLERRM := SQLERRM;					  
            P_SOURCE_COUNT := -1;
        end;
        begin 
          V_SQL_STATEMENT := 'select count(*) from "' || P_TARGET_SCHEMA  || '"."' || t.TABLE_NAME || '"';
          execute immediate V_SQL_STATEMENT into P_TARGET_COUNT;
        exception
          when others then
            V_SQLERRM := SQLERRM;					  
            P_TARGET_COUNT := -1;
        end;
        V_SQL_STATEMENT := 'insert into SCHEMA_COMPARE_RESULTS values (:1,:2,:3,:4,:5,:6,:7,:8)';
        execute immediate V_SQL_STATEMENT using P_SOURCE_SCHEMA, P_TARGET_SCHEMA, t.TABLE_NAME, P_SOURCE_COUNT, P_TARGET_COUNT, -1, -1, V_SQLERRM;
    end;
  end loop;
exception
  when OTHERS then 
	RAISE;
end;
--
end;
/
set TERMOUT on
--
show errors
--
@@SET_TERMOUT
--
