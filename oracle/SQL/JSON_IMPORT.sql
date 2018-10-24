--
/*
** De-serialize serialized data
*/
create or replace package JSON_IMPORT
AUTHID CURRENT_USER
as
  C_VERSION_NUMBER constant NUMBER(4,2) := 1.0;

  C_SUCCESS          CONSTANT VARCHAR2(32) := 'SUCCESS';
  C_FATAL_ERROR      CONSTANT VARCHAR2(32) := 'FATAL';
  C_WARNING          CONSTANT VARCHAR2(32) := 'WARNING';
  C_IGNOREABLE       CONSTANT VARCHAR2(32) := 'IGNORE';
--
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  C_RETURN_TYPE     CONSTANT VARCHAR2(32) := 'CLOB';
  C_MAX_OUTPUT_SIZE CONSTANT NUMBER       := DBMS_LOB.LOBMAXSIZE;
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  C_RETURN_TYPE     CONSTANT VARCHAR2(32):= 'VARCHAR2(32767)';
  C_MAX_OUTPUT_SIZE CONSTANT NUMBER      := 32767;
  $ELSE
  C_RETURN_TYPE     CONSTANT VARCHAR2(32):= 'VARCHAR2(4000)';
  C_MAX_OUTPUT_SIZE CONSTANT NUMBER      := 4000;
  $END
--
  TYPE T_RESULTS_CACHE is VARRAY(2147483647) of CLOB;
  RESULTS_CACHE        T_RESULTS_CACHE := T_RESULTS_CACHE();

  procedure DATA_ONLY_MODE(P_DATA_ONLY_MODE BOOLEAN);
  procedure DDL_ONLY_MODE(P_DDL_ONLY_MODE BOOLEAN);

  function IMPORT_VERSION return NUMBER deterministic;

  procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'));
  function IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) return CLOB;
  function GENERATE_STATEMENTS(P_METADATA IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) return CLOB;
  function GENERATE_STATEMENTS(P_TARGET_SCHEMA VARCHAR2, P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2, P_COLUMN_LIST CLOB, P_DATA_TYPE_LIST CLOB, P_SIZE_CONSTRAINTS CLOB) return CLOB;
  function SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2) return CLOB;
  function DISABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2) return CLOB;
  function ENABLE_CONSTRAINTS(P_TARGET_SCHEMA VARCHAR2) return CLOB;
  function MAP_FOREIGN_DATATYPE(P_DATA_TYPE VARCHAR2, P_DATA_TYPE_LENGTH NUMBER, P_DATA_TYPE_SCALE NUMBER) return VARCHAR2;
  function GET_MILLISECONDS(P_START_TIME TIMESTAMP, P_END_TIME TIMESTAMP) return NUMBER;
  function SERIALIZE_TABLE(P_TABLE T_VC4000_TABLE,P_DELIMITER VARCHAR2 DEFAULT ',')  return CLOB;

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
          into RESULTS_CACHE(RESULTS_CACHE.count) from DUAL;
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
end;
--
procedure LOG_INFO(P_PAYLOAD CLOB)
as
begin
  RESULTS_CACHE.extend;
  $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
  select JSON_OBJECT('info' value TREAT(P_PAYLOAD as JSON) returning CLOB)
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  select JSON_OBJECT('info' value JSON_QUERY(P_PAYLOAD,'$' returning VARCHAR2(32767)) returning VARCHAR2(32767))
  $ELSE
  select JSON_OBJECT('info' value JSON_QUERY(P_PAYLOAD,'$' returning VARCHAR2(4000)) returning VARCHAR2(4000))
  $END
     into RESULTS_CACHE(RESULTS_CACHE.count)
     from DUAL;
end;
--
procedure LOG_MESSAGE(V_PAYLOAD CLOB)
as
begin
  RESULTS_CACHE.extend;
  $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
  select JSON_OBJECT('message' value V_PAYLOAD returning CLOB)
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  select JSON_OBJECT('message' value V_PAYLOAD returning VARCHAR2(32767))
  $ELSE
  select JSON_OBJECT('message' value V_PAYLOAD returning VARCHAR2(4000))
  $END
     into RESULTS_CACHE(RESULTS_CACHE.count)
     from DUAL;
end;
--
function MAP_FOREIGN_DATATYPE(P_DATA_TYPE VARCHAR2, P_DATA_TYPE_LENGTH NUMBER, P_DATA_TYPE_SCALE NUMBER)
return VARCHAR2
as
begin
  case
    --
    -- SQLSERVER, MYSQL Conversions
    --
    when P_DATA_TYPE = 'bigint'
      then return 'NUMBER(19)';
    when P_DATA_TYPE = 'binary' and (P_DATA_TYPE_LENGTH = -1)
      then return 'BLOB';
    when P_DATA_TYPE = 'binary'
      then return 'RAW';
    when P_DATA_TYPE = 'bit'
      then return 'RAW(1)';
    when P_DATA_TYPE = 'datetime'
      then return 'TIMESTAMP';
    when P_DATA_TYPE = 'decimal'
      then return 'NUMBER';
    when P_DATA_TYPE = 'double'
      then return 'FLOAT(24)';
    when P_DATA_TYPE = 'double precision'
      then return 'FLOAT(24)';
    when P_DATA_TYPE = 'enum'
      then return 'VARCHAR2';
    when P_DATA_TYPE = 'float'
      then return 'FLOAT(49)';
    when P_DATA_TYPE = 'geography'
      -- TODO : Add IS JSON Constraint. Mag Geography --> GeoJSON --> "MDSYS.SPATIAL"
      then return 'VARCHAR2(4000)';
    when P_DATA_TYPE = 'geometry'
      -- TODO : Add IS JSON Constraint. Mag Geography --> GeoJSON --> "MDSYS.SPATIAL"
      then return 'VARCHAR2(4000)';
    when P_DATA_TYPE = 'hierarchyid'
      -- Assume DATA_TYPE_LENGTH is characters required to represent value as HEXBINARY (Default appears to 892)
      then return 'RAW(' || (P_DATA_TYPE_LENGTH / 2) ||')';
    when P_DATA_TYPE = 'image'
      then return 'BLOB';
    when P_DATA_TYPE = 'int'
      then return 'NUMBER(10)';
    when P_DATA_TYPE = 'longblob'
      then return 'BLOB';
    when P_DATA_TYPE = 'longtext'
      then return 'CLOB';
    when P_DATA_TYPE = 'mediumblob'
      then return 'BLOB';
    when P_DATA_TYPE = 'mediumint'
      then return 'NUMBER(7,0)';
    when P_DATA_TYPE = 'mediumblob'
      then return 'BLOB';
    when P_DATA_TYPE = 'money'
      then return 'NUMBER(19,4)';
    when P_DATA_TYPE = 'ntext'
      then return 'NCLOB';
    when P_DATA_TYPE = 'nvarchar'and (P_DATA_TYPE_LENGTH = -1)
      then return 'NCLOB';
    when P_DATA_TYPE = 'nvarchar'and (P_DATA_TYPE_LENGTH > 2000)
      -- Cannot create NVARCHAR2(2001) at least with AL32UTF8 Database Character Set
      then return 'NCLOB';
    when P_DATA_TYPE = 'nvarchar'
      then return 'NVARCHAR2';
    when P_DATA_TYPE = 'numeric'
      then return 'NUMBER';
    when P_DATA_TYPE = 'set'
      then return 'VARCHAR2';
    when P_DATA_TYPE = 'real'
      then return 'FLOAT(24)';
    when P_DATA_TYPE = 'smalldatetime'
      then return 'DATE';
    when P_DATA_TYPE = 'smallmoney'
      then return 'NUMBER(10,4)';
    when P_DATA_TYPE = 'smallint'
      then return 'NUMBER(5)';
    when P_DATA_TYPE = 'text'
      then return 'CLOB';
    when P_DATA_TYPE = 'time'
      then return 'TIMESTAMP';
    when P_DATA_TYPE = 'tinyblob'
      then return 'RAW';
    when P_DATA_TYPE = 'tinyint'
      then return 'NUMBER(3)';
    when P_DATA_TYPE = 'tinytext'
      then return 'VARCHAR2';
    when P_DATA_TYPE = 'uniqueidentifier'
      then return 'CHAR(36)';
    when P_DATA_TYPE = 'varbinary' and (P_DATA_TYPE_LENGTH = -1)
      then return 'BLOB';
    when P_DATA_TYPE = 'varbinary'
      then return 'RAW';
    when P_DATA_TYPE = 'varchar'  and (P_DATA_TYPE_LENGTH = -1)
      then return 'CLOB';
    when P_DATA_TYPE = 'varchar'
      then return 'VARCHAR2';
    when P_DATA_TYPE = 'xml'
      then return 'XMLTYPE';
    when P_DATA_TYPE = 'year'
      then return 'NUMBER(4)';
    else
      return UPPER(P_DATA_TYPE);
  end case;
end;
--
procedure APPEND_DESERIALIZATION_FUNCTIONS(P_DESERIALIZATION_FUNCTIONS T_VC4000_TABLE, P_BLOB_COUNT NUMBER, P_BFILE_COUNT NUMBER, P_ANYDATA_COUNT NUMBER, P_SQL_STATEMENT IN OUT CLOB)
as
  V_IDX   PLS_INTEGER;
begin
  if ((P_DESERIALIZATION_FUNCTIONS.count > 0) or ((P_BFILE_COUNT + P_BLOB_COUNT + P_ANYDATA_COUNT) > 0)) then
    DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB('WITH' || C_NEWLINE));
    if ((P_BFILE_COUNT + P_ANYDATA_COUNT + P_DESERIALIZATION_FUNCTIONS.count) > 0) then
      DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(OBJECT_SERIALIZATION.CODE_CHAR2BFILE));
    end if;
    if ((P_BLOB_COUNT  + P_ANYDATA_COUNT + P_DESERIALIZATION_FUNCTIONS.count) > 0) then
      DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(OBJECT_SERIALIZATION.CODE_HEXBINARY2BLOB));
    end if;
    if (P_DESERIALIZATION_FUNCTIONS.count > 0) then
      for V_IDX in 1.. P_DESERIALIZATION_FUNCTIONS.count loop
        DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(P_DESERIALIZATION_FUNCTIONS(V_IDX)));
      end loop;
    end if;
  end if;
end;
--
procedure GENERATE_STATEMENTS(P_JSON_TABLE BOOLEAN, P_TARGET_SCHEMA VARCHAR2, P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2, P_COLUMN_LIST CLOB, P_DATA_TYPE_LIST CLOB, P_DATA_SIZE_LIST CLOB, P_DDL_STATEMENT IN OUT NOCOPY CLOB, P_DML_STATEMENT  IN OUT NOCOPY CLOB, P_TARGET_DATA_TYPES OUT CLOB)
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
             when s.VALUE = ''
               then NULL
             when INSTR(s.VALUE,',') > 0
               then SUBSTR(s.VALUE,1,INSTR(s.VALUE,',')-1)
             else
               s.VALUE
           end "DATA_TYPE_LENGTH"
          ,case
             when INSTR(s.VALUE,',') > 0
               then SUBSTR(s.VALUE, INSTR(s.VALUE,',')+1)
             else
               NULL
           end "DATA_TYPE_SCALE"
          ,case
             when (INSTR(t.VALUE,'"."') > 0) 
               -- Data Type is a schema qualified object type
               then case 
                      -- Remap types defined by the source schema to the target schema.
                      when SUBSTR(t.VALUE,1,INSTR(t.VALUE,'.')-2)  = P_TABLE_OWNER
                        then P_TARGET_SCHEMA
                      else
                        SUBSTR(t.VALUE,1,INSTR(t.VALUE,'.')-2)
                      end
             else 
               NULL
           end "TYPE_OWNER"     
          ,case
             when (INSTR(t.VALUE,'"."') > 0)
               then SUBSTR(t.VALUE,INSTR(t.VALUE,'.')+2)
             else 
               NULL 
           end "TYPE_NAME"
         from JSON_TABLE('[' || P_COLUMN_LIST || ']','$[*]' COLUMNS "KEY" FOR ORDINALITY, VALUE PATH '$') c
             ,JSON_TABLE('[' || REPLACE(P_DATA_TYPE_LIST,'"."','\".\"') || ']','$[*]' COLUMNS "KEY" FOR ORDINALITY, VALUE PATH '$') t
             ,JSON_TABLE('[' || P_DATA_SIZE_LIST || ']','$[*]' COLUMNS "KEY" FOR ORDINALITY, VALUE PATH '$') s
        where (c."KEY" = t."KEY") and (c."KEY" = s."KEY")
  ),
  "TARGET_TABLE_DEFINITIONS" 
  as (
    select std.*
          , MAP_FOREIGN_DATATYPE("DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE") TARGET_DATA_TYPE
          ,case
             -- Probe rather than Join since most rows are not objects.
             when (TYPE_NAME is not null)
               then case 
                      when (TYPE_OWNER = P_TABLE_OWNER)
                        -- Original Type belonged to the same schema as the table. Resolve the type in the target Schema
                        then (select 1 from ALL_TYPES at where OWNER = P_TARGET_SCHEMA and at.TYPE_NAME = std.TYPE_NAME)
                      else
                        -- Original Type belonged to different schema as the table. Resolve the type in the original Schema
                        (select 1 from ALL_TYPES at where at.OWNER = std.TYPE_OWNER and at.TYPE_NAME = std.TYPE_NAME)
                    end
             else
               NULL
           end "TYPE_EXISTS"
      from "SOURCE_TABLE_DEFINITIONS" std
  ),
  "EXTENDED_TABLE_DEFINITIONS"
  as (
  select ttd.*,
         case when TYPE_EXISTS = 1 then OBJECT_SERIALIZATION.DESERIALIZE_TYPE(TYPE_OWNER,TYPE_NAME) else NULL end  "DESERIALIZATION_FUNCTION"
    from "TARGET_TABLE_DEFINITIONS" ttd
  )
--
  $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
--
  select SERIALIZE_TABLE(
           cast(collect(
                        '"' || COLUMN_NAME || '" ' ||
                        case
                          when TYPE_EXISTS = 1 
                            then '"' || TYPE_OWNER || '"."' || TYPE_NAME || '"'
                          when TYPE_NAME is not NULL
                            then 'CLOB'
                          -- Type Exist is NULL.
                          when TARGET_DATA_TYPE in ('DATE','DATETIME','CLOB','NCLOB','BLOB','XMLTYPE','ROWID','UROWID') or (TARGET_DATA_TYPE LIKE 'INTERVAL%') or (TARGET_DATA_TYPE LIKE '%(%)')
                            then TARGET_DATA_TYPE
                          when DATA_TYPE_SCALE is not NULL
                            then TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
                          when DATA_TYPE_LENGTH  is not NULL
                            then TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH|| ')'
                          else
                            TARGET_DATA_TYPE
                        end || C_NEWLINE
                        order by IDX
                )
                as T_VC4000_TABLE
           )
         ) COLUMNS_CLAUSE
        ,SERIALIZE_TABLE(
           cast(collect(
                        -- Cast JSON representation back into SQL data type where implicit coversion does happen or results in incorrect results
                        case
                          when DATA_TYPE = 'real'
                            then 'cast( "' || COLUMN_NAME || '" as FLOAT)'
                          when DATA_TYPE = 'bit'
                            then 'HEXTORAW(case when "' || COLUMN_NAME || '" = ''true'' then ''1'' else ''0'' end)'
                          when DATA_TYPE = 'hierarchyid'
                            then 'HEXTORAW("' || COLUMN_NAME || '")'
                          -- when DATA_TYPE = 'date'
                            -- then 'TO_DATE("' || COLUMN_NAME || '",''YYYY-MM-DDT"HH24:MI:SS.FFFTZHTZM'')'
                          when TARGET_DATA_TYPE = 'BFILE'
                            then 'OBJECT_SERIALIZATION.CHAR2BFILE("' || COLUMN_NAME || '")'
                          when (TARGET_DATA_TYPE = 'XMLTYPE')
                            then 'OBJECT_SERIALIZATION.CLOB2XMLTYPE("' || COLUMN_NAME || '")'
                          when (TARGET_DATA_TYPE = 'ANYDATA')
                            -- ### TODO - Better deserialization of ANYDATA.
                            then 'case when "' || COLUMN_NAME || '" is NULL then NULL else ANYDATA.convertVARCHAR2("' || COLUMN_NAME || '") end'
                          when TYPE_EXISTS = 1
                            then '"#' || TYPE_NAME || '"("' || COLUMN_NAME || '")'
                          when TARGET_DATA_TYPE = 'BLOB'
                            $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                            then 'OBJECT_SERIALIZATION.HEXBINARY2BLOB("' || COLUMN_NAME || '")'
                            $ELSE
                            then 'case when "' || COLUMN_NAME || '" is NULL then NULL when substr("' || COLUMN_NAME || '",1,15) = ''BLOB2HEXBINARY:'' then NULL else HEXTORAW("' || COLUMN_NAME || '") end'
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
           cast(collect(
                        -- Cast JSON representation back into SQL data type where implicit coversion does happen or results in incorrect results
                        case
                          when DATA_TYPE = 'date' -- Microsoft 
                            then 'cast(to_timestamp(:' || ROWNUM || ',''YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM'') as DATE)'
                          when DATA_TYPE = 'time' -- Microsoft 
                            then 'to_timestamp(:' || ROWNUM || ',''YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM'')'
                          when DATA_TYPE = 'datetime' -- Microsoft 
                            then 'cast(to_timestamp(:' || ROWNUM || ',''YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM'') as DATE)'
                          when DATA_TYPE = 'DATE' -- Oracle
                            then 'to_date(:' || ROWNUM || ',''YYYY-MM-DD"T"HH24:MI:SS'')'
                          when TARGET_DATA_TYPE like 'TIMESTAMP(%)'
                            then 'to_timestamp(:' || ROWNUM || ',''YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM'')'
                          when DATA_TYPE like 'TIMESTAMP%TIME ZONE'
                            then 'to_timestamp_tz(:' || ROWNUM || ',''YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM'')'
                          when DATA_TYPE = 'real'
                            then 'cast(:' || ROWNUM || ' as FLOAT)'
                          when DATA_TYPE = 'bit'
                            then 'HEXTORAW(case when :' || ROWNUM || ' = ''true'' then ''1'' else ''0'' end)'
                          when DATA_TYPE = 'hierarchyid'
                            then 'HEXTORAW(:' || ROWNUM || ')'
                          -- when DATA_TYPE = 'date'
                            -- then 'TO_DATE("' || COLUMN_NAME || '",''YYYY-MM-DDT"HH24:MI:SS.FFFTZHTZM'')'
                          when TARGET_DATA_TYPE = 'BFILE'
                            then 'OBJECT_SERIALIZATION.CHAR2BFILE(:' || ROWNUM || ')'
                          when (TARGET_DATA_TYPE = 'XMLTYPE')
                            then 'OBJECT_SERIALIZATION.CLOB2XMLTYPE(:' || ROWNUM || ')'
                          when (TARGET_DATA_TYPE = 'ANYDATA')
                            -- ### TODO - Better deserialization of ANYDATA.
                            then 'case when "' || COLUMN_NAME || '" is NULL then NULL else ANYDATA.convertVARCHAR2("' || COLUMN_NAME || '") end'
                          when TYPE_EXISTS = 1
                            then '"#' || TYPE_NAME || '"(:' || ROWNUM || ')'
                          when TARGET_DATA_TYPE = 'BLOB'
                            $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                            then 'OBJECT_SERIALIZATION.HEXBINARY2BLOB(:' || ROWNUM || ')'
                            $ELSE
                            then 'case when "' || COLUMN_NAME || '" is NULL then NULL when substr("' || COLUMN_NAME || '",1,15) = ''BLOB2HEXBINARY:'' then NULL else HEXTORAW("' || COLUMN_NAME || '") end'
                            $END
                          else
                            ':' || ROWNUM
                        end
                        order by IDX
               )
               as T_VC4000_TABLE
           )
         ) DUAL_SELECT_LIST
        ,SERIALIZE_TABLE(
           cast(collect('"' || TARGET_DATA_TYPE || '"' order by IDX) as T_VC4000_TABLE)
         ) TARGET_DATA_TYPES
        ,SERIALIZE_TABLE(
           cast(collect(
                        '"' || COLUMN_NAME || '" ' ||
                        case
                          when TARGET_DATA_TYPE in ('CHAR','NCHAR','NVARCHAR2','RAW','BFILE','ROWID','UROWID') or (TARGET_DATA_TYPE like 'INTERVAL%')
                            then 'VARCHAR2'
                          when TARGET_DATA_TYPE in ('XMLTYPE','ANYDATA','CLOB','NCLOB','BLOB','LONG','LONG RAW') or (TYPE_NAME is not NULL)
                            then C_RETURN_TYPE
                          when "TARGET_DATA_TYPE" in ('DATE','DATETIME')
                            then "TARGET_DATA_TYPE"
                          when TARGET_DATA_TYPE  = 'FLOAT'
                            then 'NUMBER'
                          when TARGET_DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE'
                            then 'TIMESTAMP WITH TIME ZONE'
                          when DATA_TYPE = 'uniqueidentifier'
                            then 'VARCHAR2(36)'
                          when DATA_TYPE = 'geography'
                            then 'VARCHAR2(4000) FORMAT JSON'
                          when DATA_TYPE = 'geometry'
                            then 'VARCHAR2(4000) FORMAT JSON'
                          when DATA_TYPE = 'hierarchyid'
                            then 'VARCHAR2'
                          when DATA_TYPE = 'float'
                            then 'VARCHAR2(49)'
                          when DATA_TYPE = 'real'
                            then 'VARCHAR2(23)'
                          when DATA_TYPE = 'datetime'
                            then 'DATE'
                          when DATA_TYPE = 'bit'
                            then 'VARCHAR2(5)'
                          when "TARGET_DATA_TYPE"  LIKE '%(%)'
                            then "TARGET_DATA_TYPE"
                          when "DATA_TYPE_SCALE" is not NULL
                            then "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ',' || "DATA_TYPE_SCALE" || ')'
                          when "DATA_TYPE_LENGTH"  is not NULL
                            then "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ')'
                          else
                            "TARGET_DATA_TYPE"
                        end
                        || ' PATH ''$[' || (IDX - 1) || ']'' ERROR ON ERROR' || C_NEWLINE
                        order by IDX
                )
                as T_VC4000_TABLE
           )
         ) COLUMN_PATTERNS
        , cast(collect( DESERIALIZATION_FUNCTION) as T_VC4000_TABLE) "DESERIALIZATION_FUNCTIONS"
        , SUM(CASE WHEN TARGET_DATA_TYPE = 'BLOB' THEN 1 ELSE 0 END) BLOB_COUNT
        , SUM(CASE WHEN TARGET_DATA_TYPE = 'BFILE' THEN 1 ELSE 0 END) BFILE_COUNT
        , SUM(CASE WHEN TARGET_DATA_TYPE = 'ANYDATA' THEN 1 ELSE 0 END) ANYDATA_COUNT
        , SUM(CASE WHEN TYPE_EXISTS = 1 THEN 1 ELSE 0 END) OBJECT_COUNT
    from "EXTENDED_TABLE_DEFINITIONS";
--
  $ELSE
--
  -- cast(collect(...) causes ORA-22814: attribute or element value is larger than specified in type in 12.2
  select '"' || COLUMN_NAME || '" ' ||
         case
           when (INSTR(TARGET_DATA_TYPE,'"."') > 0)
             then case
                    when SUBSTR(TARGET_DATA_TYPE,1,INSTR(TARGET_DATA_TYPE,'"."')-1) = P_TABLE_OWNER
                      then '"' || P_TARGET_SCHEMA || '"."' || SUBSTR(TARGET_DATA_TYPE,INSTR(TARGET_DATA_TYPE,'"."')+3) || '"'
                      else '"' || TARGET_DATA_TYPE || '"'
                  end
           when TARGET_DATA_TYPE in ('DATE','DATETIME','CLOB','NCLOB','BLOB','XMLTYPE','ROWID','UROWID') or (TARGET_DATA_TYPE LIKE 'INTERVAL%')
             then TARGET_DATA_TYPE
           when DATA_TYPE_SCALE is not NULL
             then TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
           when DATA_TYPE_LENGTH  is not NULL
             then TARGET_DATA_TYPE  || '(' || DATA_TYPE_LENGTH|| ')'
           else
             TARGET_DATA_TYPE
         end || C_NEWLINE COLUMNS_CLAUSE
         /* Cast JSON representation back into SQL data type where implicit coversion does happen or results in incorrect results */
        ,case
           when TARGET_DATA_TYPE = 'BFILE'
             then 'OBJECT_SERIALIZATION.CHAR2BFILE("' || COLUMN_NAME || '")'
           when (TARGET_DATA_TYPE = 'XMLTYPE') or (SUBSTR("TARGET_DATA_TYPE",INSTR("TARGET_DATA_TYPE",'"."')+3) = 'XMLTYPE')
             then 'case when "' || COLUMN_NAME || '" is NULL then NULL else XMLTYPE("' || COLUMN_NAME || '") end'
           when (TARGET_DATA_TYPE = 'ANYDATA') or (SUBSTR("TARGET_DATA_TYPE",INSTR("TARGET_DATA_TYPE",'"."')+3) = 'ANYDATA')
             -- ### TODO - Better deserialization of ANYDATA.
             then 'case when "' || COLUMN_NAME || '" is NULL then NULL else ANYDATA.convertVARCHAR2("' || COLUMN_NAME || '") end'
           when TARGET_DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE'
             -- Problems with ORA-1881
             then 'TO_TIMESTAMP_TZ("' || COLUMN_NAME || '",''YYYY-MM-DD"T"HH24:MI:SS.FFTZHTZM'')'
           when INSTR(TARGET_DATA_TYPE,'"."') > 0
             then '"#' || SUBSTR(TARGET_DATA_TYPE,INSTR(TARGET_DATA_TYPE,'"."')+3) || '"("' || COLUMN_NAME || '")'
           when TARGET_DATA_TYPE = 'BLOB'
             $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
             then 'OBJECT_SERIALIZATION.HEXBINARY2BLOB("' || COLUMN_NAME || '")'
             $ELSE
             then 'case when "' || COLUMN_NAME || '" is NULL then NULL when substr("' || COLUMN_NAME || '",1,15) = ''BLOB2HEXBINARY:'' then NULL else HEXTORAW("' || COLUMN_NAME || '") end'
             $END
           else
             '"' || COLUMN_NAME || '"'
         end INSERT_SELECT_LIST
        ,'"' || COLUMN_NAME || '" ' ||
         case
           when TARGET_DATA_TYPE in ('CHAR','NCHAR','NVARCHAR2','RAW','BFILE','ROWID','UROWID') or (TARGET_DATA_TYPE like 'INTERVAL%')
             then 'VARCHAR2'
           when TARGET_DATA_TYPE in ('XMLTYPE','CLOB','NCLOB','BLOB','LONG','LONG RAW') or (INSTR(TARGET_DATA_TYPE,'"."') > 0)
             then C_RETURN_TYPE
           when "TARGET_DATA_TYPE" in ('DATE','DATETIME')
             then "TARGET_DATA_TYPE"
           when TARGET_DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE'
             -- Problems with ORA-1881
             -- then 'TIMESTAMP WITH TIME ZONE'
             then 'VARCHAR2'
            when "DATA_TYPE_SCALE" is not NULL
             then "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ',' || "DATA_TYPE_SCALE" || ')'
           when "DATA_TYPE_LENGTH"  is not NULL
             then "TARGET_DATA_TYPE"  || '(' || "DATA_TYPE_LENGTH" || ')'
           else
             "TARGET_DATA_TYPE"
         end
         || ' PATH ''$[' || (st.IDX - 1) || ']'' ERROR ON ERROR' || C_NEWLINE COLUMN_PATTERNS
    from "EXTENDED_TABLE_DEFINITIONS"
   order by IDX;

   V_COLUMNS_CLAUSE_TABLE      T_VC4000_TABLE;
   V_INSERT_SELECT_TABLE       T_VC4000_TABLE;
   V_COLUMN_PATTERNS_TABLE     T_VC4000_TABLE;
   $END
--
   V_COLUMNS_CLAUSE            CLOB;
   V_INSERT_SELECT_LIST        CLOB;
   V_COLUMN_PATTERNS           CLOB;
   V_DUAL_SELECT_LIST          CLOB;
   
   V_DESERIALIZATIONS          T_VC4000_TABLE;

   V_OBJECT_COUNT              NUMBER;
   V_BLOB_COUNT                NUMBER;
   V_ANYDATA_COUNT             NUMBER;
   V_BFILE_COUNT               NUMBER;
   V_INLINE_PLSQL_REQUIRED     BOOLEAN := FALSE;

   V_SQL_FRAGMENT VARCHAR2(32767);
   V_INSERT_HINT  VARCHAR2(128) := '';

   C_CREATE_TABLE_BLOCK1 CONSTANT VARCHAR2(2048) :=
'declare
  TABLE_EXISTS EXCEPTION;
  PRAGMA EXCEPTION_INIT( TABLE_EXISTS , -00955 );
  V_STATEMENT CLOB := ''create table "';

   C_CREATE_TABLE_BLOCK2 CONSTANT VARCHAR2(2048) :=
')'';
begin
  execute immediate V_STATEMENT;
exception
  when TABLE_EXISTS then
    null;
  when others then  
    RAISE;
end;';
begin

  DBMS_LOB.CREATETEMPORARY(P_DDL_STATEMENT,TRUE,DBMS_LOB.SESSION);
  DBMS_LOB.CREATETEMPORARY(P_DML_STATEMENT,TRUE,DBMS_LOB.SESSION);
--
  $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
--
   -- Cursor only generates one row (Aggregration Operation),
  for o in generateStatementComponents loop
    V_COLUMNS_CLAUSE         := o.COLUMNS_CLAUSE;
    V_INSERT_SELECT_LIST     := o.INSERT_SELECT_LIST;
    V_DUAL_SELECT_LIST       := o.DUAL_SELECT_LIST;
    P_TARGET_DATA_TYPES      := o.TARGET_DATA_TYPES;
    V_COLUMN_PATTERNS        := o.COLUMN_PATTERNS;
    V_OBJECT_COUNT           := o.OBJECT_COUNT;
    V_BFILE_COUNT            := o.BFILE_COUNT;
    V_BLOB_COUNT             := o.BLOB_COUNT;
    V_ANYDATA_COUNT          := o.ANYDATA_COUNT;
    
    select distinct COLUMN_VALUE 
      bulk collect into V_DESERIALIZATIONS
      from table(o.DESERIALIZATION_FUNCTIONS);
    
    V_INLINE_PLSQL_REQUIRED := ((V_OBJECT_COUNT + V_BLOB_COUNT + V_BFILE_COUNT + V_ANYDATA_COUNT) > 0);
     
  end loop;
--
  $ELSE
--
  open generateStatementComponents;
  fetch generateStatementComponents
        bulk collect into V_COLUMNS_CLAUSE_TABLE, V_INSERT_SELECT_TABLE, V_COLUMN_PATTERNS_TABLE;

  V_COLUMNS_CLAUSE := SERIALIZE_TABLE(V_COLUMNS_CLAUSE_TABLE);
  V_INSERT_SELECT_LIST := SERIALIZE_TABLE(V_INSERT_SELECT_TABLE);
  V_COLUMN_PATTERNS := SERIALIZE_TABLE(V_COLUMN_PATTERNS_TABLE);
--
  $END
--
  V_SQL_FRAGMENT := C_CREATE_TABLE_BLOCK1 || P_TARGET_SCHEMA || '"."' || P_TABLE_NAME || '" (' || C_NEWLINE || ' ';
  DBMS_LOB.WRITEAPPEND(P_DDL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  DBMS_LOB.APPEND(P_DDL_STATEMENT,V_COLUMNS_CLAUSE);
  V_SQL_FRAGMENT := C_NEWLINE || C_CREATE_TABLE_BLOCK2;
  DBMS_LOB.WRITEAPPEND(P_DDL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  
  
  if ( V_INLINE_PLSQL_REQUIRED) then
    V_INSERT_HINT := ' /*+ WITH_PLSQL */';
  end if;
  
  /*
  **
  ** Generate one of three possibile INSERT operations
  **
  ** (1) INSERT INTO TABLE SELECT ... FROM JSON_TABLE()
  **
  ** (2) INSERT INTO TABLE VALUES (...)
  **     Used when there no inline PL/SQL is required for conversions
  **
  ** (3) INSERT INTO TABLE SELECT ... FROM DUAL
  **     Used When in-line PL/SQL functions are required to perform conversions (OBJECTS, BLOBS, BFILES, ANYDATA)
  **
  **/ 

  V_SQL_FRAGMENT := 'insert' || V_INSERT_HINT || ' into "' || P_TARGET_SCHEMA || '"."' || P_TABLE_NAME || '" (';
  DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  DBMS_LOB.APPEND(P_DML_STATEMENT,P_COLUMN_LIST);
  V_SQL_FRAGMENT :=  ')' || C_NEWLINE;
  DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  
  APPEND_DESERIALIZATION_FUNCTIONS(V_DESERIALIZATIONS,V_BLOB_COUNT,V_BFILE_COUNT,V_ANYDATA_COUNT,P_DML_STATEMENT);
   
  if (P_JSON_TABLE) then
    V_SQL_FRAGMENT := 'select ';
    DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    DBMS_LOB.APPEND(P_DML_STATEMENT,V_INSERT_SELECT_LIST );
    V_SQL_FRAGMENT := C_NEWLINE || '  from JSON_TABLE(:JSON,''$.data."' || P_TABLE_NAME || '"[*]''' || C_NEWLINE || '         COLUMNS(' || C_NEWLINE || ' ';
    DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    DBMS_LOB.APPEND(P_DML_STATEMENT,V_COLUMN_PATTERNS);
    DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,2,'))');
  else
    -- V_INLINE_PLSQL_REQUIRED := TRUE;
    if (V_INLINE_PLSQL_REQUIRED) then
      V_SQL_FRAGMENT := 'select ';
      DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
      DBMS_LOB.APPEND(P_DML_STATEMENT,V_DUAL_SELECT_LIST);    
      V_SQL_FRAGMENT := C_NEWLINE || '  from DUAL';
      DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    else
      V_SQL_FRAGMENT := '       values (';
      DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
      DBMS_LOB.APPEND(P_DML_STATEMENT,V_DUAL_SELECT_LIST);    
      DBMS_LOB.WRITEAPPEND(P_DML_STATEMENT,1,')');
    end if;
  end if;
  
exception
  when OTHERS then 
    LOG_INFO('[' || P_COLUMN_LIST || ']');
    LOG_INFO('[' || REPLACE(P_DATA_TYPE_LIST,'"."','"."') || ']');
    LOG_INFO('[' || P_DATA_SIZE_LIST || ']');
    LOG_ERROR(C_FATAL_ERROR,'JSON_IMPORT.GENERATE_STATEMENTS()',NULL,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
    raise;
end;
--
procedure SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2)
as
  USER_NOT_FOUND EXCEPTION ; PRAGMA EXCEPTION_INIT( USER_NOT_FOUND , -01435 );
  V_SQL_STATEMENT CONSTANT VARCHAR2(4000) := 'ALTER SESSION SET CURRENT_SCHEMA = ' || P_TARGET_SCHEMA;
begin
  if (SYS_CONTEXT('USERENV','CURRENT_SCHEMA') <> P_TARGET_SCHEMA) then
    execute immediate V_SQL_STATEMENT;
    TRACE_OPERATION(V_SQL_STATEMENT);
  end if;
end;
--
function SET_CURRENT_SCHEMA(P_TARGET_SCHEMA VARCHAR2)
return CLOB
as
  V_RESULTS CLOB;
begin
  RESULTS_CACHE := T_RESULTS_CACHE();
  SET_CURRENT_SCHEMA(P_TARGET_SCHEMA);
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
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
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
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
  select JSON_ARRAYAGG(TREAT (COLUMN_VALUE as JSON) returning CLOB)
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
   V_SQL_FRAGMENT := 'declare' || C_NEWLINE
                  || '  cursor JSON_TO_RELATIONAL' || C_NEWLINE
                  || '  is' || C_NEWLINE
                  || '  select *' || C_NEWLINE
                  || '    from ';

   DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
   DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
   V_JSON_TABLE_OFFSET := DBMS_LOB.INSTR(P_DML_STATEMENT,' JSON_TABLE(');
   DBMS_LOB.COPY(V_SQL_STATEMENT,P_DML_STATEMENT,((DBMS_LOB.GETLENGTH(P_DML_STATEMENT)-V_JSON_TABLE_OFFSET)+1),DBMS_LOB.GETLENGTH(V_SQL_STATEMENT)+1,V_JSON_TABLE_OFFSET);

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
                  || '      insert into "' || P_TABLE_NAME || '"' || C_NEWLINE
                  || '      values V_ROW_BUFFER(i);'|| C_NEWLINE
                  || '    end loop;'|| C_NEWLINE
                  || '    commit;' || C_NEWLINE
                  || '  end loop;' || C_NEWLINE
                  || '  :2 := V_ROW_COUNT;' || C_NEWLINE
                  || 'end;';

   DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
   P_DML_STATEMENT := V_SQL_STATEMENT;
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
    DBMS_MVIEW.REFRESH(V_MVIEW_LIST);
  end if;
end;
--
procedure IMPORT_JSON(P_JSON_DUMP_FILE IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
as
  MUTATING_TABLE EXCEPTION ; PRAGMA EXCEPTION_INIT( MUTATING_TABLE , -04091 );

  V_CURRENT_SCHEMA           CONSTANT VARCHAR2(128) := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');

  V_START_TIME        TIMESTAMP(6);
  V_END_TIME          TIMESTAMP(6);
  V_ROWCOUNT          NUMBER;

  V_DDL_STATEMENT     CLOB;
  V_DML_STATEMENT     CLOB;
  V_TARGET_DATA_TYPES CLOB;

  CURSOR operationsList
  is
  select OWNER
        ,TABLE_NAME
        ,COLUMN_LIST
        ,DATA_TYPE_LIST
        ,SIZE_CONSTRAINTS
    from JSON_TABLE(
           P_JSON_DUMP_FILE,
           '$.metadata.*'
           COLUMNS (
             OWNER                        VARCHAR2(128) PATH '$.owner'
           , TABLE_NAME                   VARCHAR2(128) PATH '$.tableName'
           $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
           ,  COLUMN_LIST                          CLOB PATH '$.columns'
           ,  DATA_TYPE_LIST                       CLOB PATH '$.dataTypes'
           ,  SIZE_CONSTRAINTS                     CLOB PATH '$.dataTypeSizing'
           ,  INSERT_COLUMN_LIST                   CLOB PATH '$.insertSelectList'
           ,  COLUMN_PATTERNS                      CLOB PATH '$.columnPatterns'
           $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
           ,  COLUMN_LIST               VARCHAR2(32767) PATH '$.columns'
           ,  DATA_TYPE_LIST            VARCHAR2(32767) PATH '$.dataTypes'
           ,  SIZE_CONSTRAINTS          VARCHAR2(32767) PATH '$.dataTypeSizing'
           ,  INSERT_COLUMN_LIST        VARCHAR2(32767) PATH '$.insertSelectList'
           ,  COLUMN_PATTERNS           VARCHAR2(32767) PATH '$.columnPatterns'
           $ELSE
           ,  COLUMN_LIST                VARCHAR2(4000) PATH '$.columns'
           ,  DATA_TYPE_LIST             VARCHAR2(4000) PATH '$.dataTypes'
           ,  SIZE_CONSTRAINTS           VARCHAR2(4000) PATH '$.dataTypeSizing'
           ,  INSERT_COLUMN_LIST         VARCHAR2(4000) PATH '$.insertSelectList'
           ,  COLUMN_PATTERNS            VARCHAR2(4000) PATH '$.columnPatterns'
           $END
           )
         );
   
   V_NOTHING_DONE BOOLEAN := TRUE;
begin
  -- LOG_INFO(JSON_OBJECT('startTime' value SYSTIMESTAMP, 'includeData' value G_INCLUDE_DATA, 'includeDDL' value G_INCLUDE_DDL));

  SET_CURRENT_SCHEMA(P_TARGET_SCHEMA);

  if (G_INCLUDE_DDL) then
    JSON_EXPORT_DDL.APPLY_DDL_STATEMENTS(P_JSON_DUMP_FILE,P_TARGET_SCHEMA);
  end if;


  if (G_INCLUDE_DATA) then

  DISABLE_CONSTRAINTS(P_TARGET_SCHEMA) ;

    for o in operationsList loop
      V_NOTHING_DONE := FALSE;
      GENERATE_STATEMENTS(TRUE,P_TARGET_SCHEMA, o.OWNER, o.TABLE_NAME, o.COLUMN_LIST, o.DATA_TYPE_LIST, o.SIZE_CONSTRAINTS, V_DDL_STATEMENT, V_DML_STATEMENT,V_TARGET_DATA_TYPES);
      begin
        execute immediate V_DDL_STATEMENT;
        LOG_DDL_OPERATION(o.TABLE_NAME,V_DDL_STATEMENT);
      exception
        when others then
          LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,V_DDL_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end;
      begin
        V_START_TIME := SYSTIMESTAMP;
        execute immediate V_DML_STATEMENT using P_JSON_DUMP_FILE;
        V_ROWCOUNT := SQL%ROWCOUNT;
        V_END_TIME := SYSTIMESTAMP;
        commit;
        LOG_DML_OPERATION(o.TABLE_NAME,V_DML_STATEMENT,V_ROWCOUNT,GET_MILLISECONDS(V_START_TIME,V_END_TIME));
      exception
        when MUTATING_TABLE then
          begin
            LOG_ERROR(C_WARNING,o.TABLE_NAME,V_DML_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
            MANAGE_MUTATING_TABLE(o.TABLE_NAME,V_DML_STATEMENT);
            V_START_TIME := SYSTIMESTAMP;
            execute immediate V_DML_STATEMENT using P_JSON_DUMP_FILE, out V_ROWCOUNT;
            V_END_TIME := SYSTIMESTAMP;
            commit;
            LOG_DML_OPERATION(o.TABLE_NAME,V_DML_STATEMENT,V_ROWCOUNT,GET_MILLISECONDS(V_START_TIME,V_END_TIME));
          exception
            when others then
              LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,V_DML_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
           end;
        when others then
          LOG_ERROR(C_FATAL_ERROR,o.TABLE_NAME,V_DML_STATEMENT,SQLCODE,SQLERRM,DBMS_UTILITY.FORMAT_ERROR_STACK());
      end;
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
function GENERATE_STATEMENTS(P_TARGET_SCHEMA VARCHAR2, P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2, P_COLUMN_LIST CLOB, P_DATA_TYPE_LIST CLOB, P_SIZE_CONSTRAINTS CLOB)
return CLOB
as
  V_RESULTS           CLOB;
  V_DDL_STATEMENT     CLOB;
  V_DML_STATEMENT     CLOB;
  
  V_TARGET_DATA_TYPES CLOB;
begin
  GENERATE_STATEMENTS(FALSE,P_TARGET_SCHEMA, P_TABLE_OWNER, P_TABLE_NAME, P_COLUMN_LIST, P_DATA_TYPE_LIST, P_SIZE_CONSTRAINTS, V_DDL_STATEMENT, V_DML_STATEMENT, V_TARGET_DATA_TYPES);
  
  select JSON_OBJECT('ddl' value V_DDL_STATEMENT, 'dml' value V_DML_STATEMENT, 'targetDataTypes' value V_TARGET_DATA_TYPES returning CLOB)
    into V_RESULTS 
    from dual;
 
  return V_RESULTS;
end;
--
function GENERATE_STATEMENTS(P_METADATA IN OUT NOCOPY BLOB,P_TARGET_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return CLOB
as
  V_RESULTS CLOB;
  V_FRAGMENT VARCHAR2(4000);
  
  cursor getStatements
  is
  /*
  select JSON_OBJECTAGG(
           TABLE_NAME,
           TREAT(GENERATE_STATEMENTS(P_TARGET_SCHEMA, OWNER, TABLE_NAME, COLUMN_LIST, DATA_TYPE_LIST, SIZE_CONSTRAINTS) as JSON) 
           returning CLOB
         )
    into V_RESULTS
  */
  select ROWNUM,
         TABLE_NAME,
         GENERATE_STATEMENTS(P_TARGET_SCHEMA, OWNER, TABLE_NAME, COLUMN_LIST, DATA_TYPE_LIST, SIZE_CONSTRAINTS) TABLE_INFO
    from JSON_TABLE(
           P_METADATA,
           '$.metadata.*'
           COLUMNS (
             OWNER                        VARCHAR2(128) PATH '$.owner'
           , TABLE_NAME                   VARCHAR2(128) PATH '$.tableName'
           $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
           ,  COLUMN_LIST                          CLOB PATH '$.columns'
           ,  DATA_TYPE_LIST                       CLOB PATH '$.dataTypes'
           ,  SIZE_CONSTRAINTS                     CLOB PATH '$.dataTypeSizing'
           ,  INSERT_COLUMN_LIST                   CLOB PATH '$.insertSelectList'
           ,  COLUMN_PATTERNS                      CLOB PATH '$.columnPatterns'
           $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
           ,  COLUMN_LIST               VARCHAR2(32767) PATH '$.columns'
           ,  DATA_TYPE_LIST            VARCHAR2(32767) PATH '$.dataTypes'
           ,  SIZE_CONSTRAINTS          VARCHAR2(32767) PATH '$.dataTypeSizing'
           ,  INSERT_COLUMN_LIST        VARCHAR2(32767) PATH '$.insertSelectList'
           ,  COLUMN_PATTERNS           VARCHAR2(32767) PATH '$.columnPatterns'
           $ELSE
           ,  COLUMN_LIST                VARCHAR2(4000) PATH '$.columns'
           ,  DATA_TYPE_LIST             VARCHAR2(4000) PATH '$.dataTypes'
           ,  SIZE_CONSTRAINTS           VARCHAR2(4000) PATH '$.dataTypeSizing'
           ,  INSERT_COLUMN_LIST         VARCHAR2(4000) PATH '$.insertSelectList'
           ,  COLUMN_PATTERNS            VARCHAR2(4000) PATH '$.columnPatterns'
           $END
           )
         );
begin
   DBMS_LOB.CREATETEMPORARY(V_RESULTS,TRUE,DBMS_LOB.SESSION);
   DBMS_LOB.WRITEAPPEND(V_RESULTS,1,'{');

  for x in getStatements() loop
    if (x.ROWNUM > 1) then
      DBMS_LOB.WRITEAPPEND(V_RESULTS,1,',');
    end if;
    V_FRAGMENT := '"' || x.TABLE_NAME || '" : ';
    DBMS_LOB.WRITEAPPEND(V_RESULTS,LENGTH(V_FRAGMENT),V_FRAGMENT);
    DBMS_LOB.APPEND(V_RESULTS,x.TABLE_INFO);
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
             from table(JSON_EXPORT_DDL.RESULTS_CACHE)
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
    from table(JSON_EXPORT_DDL.RESULTS_CACHE)
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
end;
/
show errors
--