create or replace package JSON_EXPORT
authid CURRENT_USER
as
  TYPE EXPORT_METADATA_RECORD is RECORD (
    OWNER                VARCHAR2(128)
   ,TABLE_NAME           VARCHAR2(128)
   ,COLUMN_LIST          CLOB
   ,DATA_TYPE_LIST       CLOB
   ,SIZE_CONSTRAINTS     CLOB
   ,EXPORT_SELECT_LIST   CLOB
   ,NODE_SELECT_LIST     CLOB
   ,IMPORT_SELECT_LIST   CLOB
   ,COLUMN_PATTERN_LIST  CLOB
   ,WITH_CLAUSE          CLOB
   ,SQL_STATEMENT        CLOB
  );
  
  TYPE EXPORT_METADATA_TABLE IS TABLE OF EXPORT_METADATA_RECORD;
  
  function GET_DML_STATEMENTS(P_OWNER_LIST VARCHAR2,P_TABLE_NAME VARCHAR2 DEFAULT NULL) return EXPORT_METADATA_TABLE PIPELINED;
  function JSON_FEATURES return VARCHAR2 deterministic;
  function DATABASE_RELEASE return NUMBER deterministic;
--
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
$ELSE
  procedure JSON_ARRAYAGG(P_JSON_DOCUMENT IN OUT CLOB, P_CURSOR SYS_REFCURSOR);
  function JSON_ARRAYAGG(P_CURSOR SYS_REFCURSOR) return CLOB;
$END
--
END;
/
--
set TERMOUT on
--
show errors
--
@@SET_TERMOUT
--
create or replace package BODY JSON_EXPORT
as
  C_NEWLINE         CONSTANT CHAR(1) := CHR(10);
  C_SINGLE_QUOTE    CONSTANT CHAR(1) := CHR(39);
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
function TABLE_TO_LIST(P_TABLE T_VC4000_TABLE,P_DELIMITER VARCHAR2 DEFAULT ',') 
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
function DATABASE_RELEASE return NUMBER deterministic
as
begin
  return DBMS_DB_VERSION.VERSION || '.' || DBMS_DB_VERSION.RELEASE;
end;
--
function JSON_FEATURES return VARCHAR2 deterministic
as
begin
  return JSON_OBJECT(
          'treatAsJSON'    value JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED
	 	 ,'CLOB'           value JSON_FEATURE_DETECTION.CLOB_SUPPORTED
		 ,'extendedString' value JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED
		 );
end;
--
procedure APPEND_SERIALIZATION_FUNCTIONS(P_OBJECT_SERAIALIZATION CLOB,P_SQL_STATEMENT IN OUT CLOB)
as
begin
  if (P_OBJECT_SERAIALIZATION is not NULL) then
    DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB('WITH' || C_NEWLINE));
    DBMS_LOB.APPEND(P_SQL_STATEMENT,P_OBJECT_SERAIALIZATION);
  end if;
end;
--
function GET_DML_STATEMENTS(P_OWNER_LIST VARCHAR2,P_TABLE_NAME VARCHAR2 DEFAULT NULL) 
return EXPORT_METADATA_TABLE 
PIPELINED
/*
** Generate the SQL Statements to create a JSON document from the contents of the supplied schemas
*/
as
  V_SQL_FRAGMENT  VARCHAR2(32767);
  
  V_SCHEMA_LIST T_VC4000_TABLE;

   -- ### Do not use JSON_ARRAYAGG for DATE_TYPE_LIST, SIZE_CONSTRAINTS dir to lack of CLOB support prior to release 18c.
  
  cursor getTableMetadata
  is
  select aat.owner
        ,aat.table_name
		,sum(case when ((TYPECODE in ('COLLECTION', 'OBJECT')) and (atc.DATA_TYPE not in ('XMLTYPE','ANYDATA','RAW'))) then 1 else 0 end) OBJECT_COUNT
        ,cast(collect('"' || atc.COLUMN_NAME || '"' ORDER BY INTERNAL_COLUMN_ID) as T_VC4000_TABLE) COLUMN_LIST
		,cast(collect(
               case 
                 when (jc.FORMAT is not NULL) then
                   -- Does not attempt to preserve json storage details
                   -- If storage model fidelity is required then set specify MODE=DDL_AND_DATA on the export command line to include DDL statements to the file.
                   -- If DDL is not included in the file import operations will default to CLOB storage in Oracle 12.1 thru 18c.
                   '"JSON"'
                 when (atc.DATA_TYPE like 'TIMESTAMP(%)') then
                   '"TIMESTAMP"'
                 when (DATA_TYPE_OWNER is null) then
                   '"' || atc.DATA_TYPE || '"' 
                 when (atc.DATA_TYPE in ('XMLTYPE','ANYDATA','RAW')) then
                   '"' || atc.DATA_TYPE || '"' 
                 else 
                    '"\"' || atc.DATA_TYPE_OWNER || '\".\"' || atc.DATA_TYPE || '\""' 
               end 
               ORDER BY INTERNAL_COLUMN_ID) as T_VC4000_TABLE) DATA_TYPE_LIST
	    ,cast(collect( 
		       case
			     when atc.DATA_TYPE in ('VARCHAR2', 'CHAR') then
                   case 
                     when (CHAR_LENGTH < DATA_LENGTH) then
                       '"' || CHAR_LENGTH || '"'
                     else 
                       '"' || DATA_LENGTH || '"'
                   end
			     when (atc.DATA_TYPE = 'TIMESTAMP') or (atc.DATA_TYPE LIKE  'TIMESTAMP(%)') or (atc.DATA_TYPE LIKE '%TIME ZONE')  then
                   '"' || DATA_SCALE || '"'
                 when atc.DATA_TYPE in ('NVARCHAR2', 'NCHAR') then
                   '"' || CHAR_LENGTH || '"'
                 when atc.DATA_TYPE in ('UROWID', 'RAW') or  atc.DATA_TYPE LIKE 'INTERVAL%' then
                   '"' || DATA_LENGTH || '"'
                 when atc.DATA_TYPE = 'NUMBER' then
                   case 
                     when DATA_SCALE is NOT NULL and DATA_SCALE <> 0 then
                       '"' || DATA_PRECISION || ',' || DATA_SCALE || '"'
                     when DATA_PRECISION is NOT NULL then
                       '"' || DATA_PRECISION || '"'
                     else 
					   '"38"'
                   end 
                 when atc.DATA_TYPE = 'FLOAT' then
                   '"' || DATA_PRECISION || '"'
                 else
                   '""'
               end
	           ORDER BY INTERNAL_COLUMN_ID) as T_VC4000_TABLE) SIZE_CONSTRAINT_LIST
        ,cast(collect(
               case
                 -- For some reason RAW columns have atc.DATA_TYPE_OWNER set to the current schema.
                 when atc.DATA_TYPE = 'RAW' then
                   '"' || atc.COLUMN_NAME || '"'
                 $IF not JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                 /*
                 ** Pre 18.1 Some Scalar Data Types are not natively supported by JSON_ARRAY()
                 */
                 when atc.DATA_TYPE in ('BINARY_DOUBLE','BINARY_FLOAT') then
                   'TO_NUMBER("' || atc.COLUMN_NAME || '")'
                 when atc.DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE' then
                   'TO_CHAR(SYS_EXTRACT_UTC("' || atc.COLUMN_NAME || '"),''YYYY-MM-DD"T"HH24:MI:SS' || case when atc.DATA_SCALE > 0 then '.FF' || atc.DATA_SCALE else '' end || '"Z"'')'
                 when atc.DATA_TYPE like 'INTERVAL DAY% TO SECOND%' then
                   '''P''
                   || extract(DAY FROM "' || atc.COLUMN_NAME || '") || ''D''
                   || ''T'' || case when extract(HOUR FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(HOUR FROM  "' || atc.COLUMN_NAME || '") ||  ''H'' end
                   || case when extract(MINUTE FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(MINUTE FROM  "' || atc.COLUMN_NAME || '") || ''M'' end
                   || case when extract(SECOND FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(SECOND FROM  "' || atc.COLUMN_NAME || '") ||  ''S'' end'
                 when atc.DATA_TYPE  like 'INTERVAL YEAR% TO MONTH%' then
                   '''P''
                   || extract(YEAR FROM "' || atc.COLUMN_NAME || '") || ''Y''
                   || case when extract(MONTH FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(MONTH FROM  "' || atc.COLUMN_NAME || '") || ''M'' end'
                 when atc.DATA_TYPE in ('NCHAR','NVARCHAR2') then
                   'TO_CHAR("' || atc.COLUMN_NAME || '")'
                 when atc.DATA_TYPE = 'NCLOB' then
                   'TO_CLOB("' || atc.COLUMN_NAME || '")'
                 when jc.FORMAT is not NULL then
                   'JSON_QUERY("' ||  atc.COLUMN_NAME || '",''$'' returning ' || C_RETURN_TYPE || ')'
                 /*
                 ** 18.1 compatible handling of BLOB
                 */
                 when atc.DATA_TYPE = 'BLOB' then
                   'OBJECT_SERIALIZATION.SERIALIZE_BLOB_HEX("' || atc.COLUMN_NAME || '")'                          
                 $END
                 /*
                 ** Quick Fixes for datatypes not natively supported
                 */
                 when ((atc.DATA_TYPE_OWNER = 'MDSYS') and (atc.DATA_TYPE  in ('SDO_GEOMETRY'))) then
                   'case when t."' ||  atc.COLUMN_NAME || '".ST_isValid() = 1 then t."' ||  atc.COLUMN_NAME || '".get_WKT(() else NULL end "' || atc.COLUMN_NAME || '"'
                 when atc.DATA_TYPE = 'XMLTYPE' then -- Can be owned by SYS or PUBLIC
                   'case when "' ||  atc.COLUMN_NAME || '" is NULL then NULL else XMLSERIALIZE(CONTENT "' ||  atc.COLUMN_NAME || '" as CLOB) end "' || atc.COLUMN_NAME || '"'
                 when atc.DATA_TYPE = 'ROWID' or atc.DATA_TYPE = 'UROWID' then
                   'ROWIDTOCHAR("' || atc.COLUMN_NAME || '")'
                 /*
                 ** Fix for BFILENAME
                 */
                 when atc.DATA_TYPE = 'BFILE' then
                   'OBJECT_SERIALIZATION.SERIALIZE_BFILE("' || atc.COLUMN_NAME || '")'
                 /*
                 **
                 ** Support ANYDATA, OBJECT and COLLECTION types
                 **
                 */
                 when atc.DATA_TYPE = 'ANYDATA' then  -- Can be owned by SYS or PUBLIC 
                   'case when "' ||  atc.COLUMN_NAME || '" is NULL then NULL else OBJECT_SERIALIZATION.SERIALIZE_ANYDATA("' ||  atc.COLUMN_NAME || '") end' || atc.COLUMN_NAME || '"'
                 when TYPECODE = 'COLLECTION' then
                   'case when "' || atc.COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(''' || aat.OWNER || ''',ANYDATA.convertCollection("' || atc.COLUMN_NAME || '")) end "' || atc.COLUMN_NAME || '"'
                 when TYPECODE = 'OBJECT' then
                   'case when "' || atc.COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(''' || aat.OWNER || ''',ANYDATA.convertObject("' || atc.COLUMN_NAME || '")) end "' || atc.COLUMN_NAME || '"'
                 /*     
                 ** Comment out unsupported scalar data types and Object types
                 */
                 when atc.DATA_TYPE in ('LONG','LONG RAW') then
                   '''"' || atc.COLUMN_NAME || '". Unsupported data type ["' || atc.DATA_TYPE || '"]'''
                 else
                   '"' || atc.COLUMN_NAME || '"'
               end
        order by INTERNAL_COLUMN_ID) as T_VC4000_TABLE) EXPORT_SELECT_LIST
        ,cast(collect(
               case
                 /*
                 ** Quick Fixes for datatypes not natively supported by NODE.js Driver
                 */
                 when atc.DATA_TYPE = 'RAW' then
                   -- For some reason RAW columns have atc.DATA_TYPE_OWNER set to the current schema.
                   '"' || atc.COLUMN_NAME || '"'
                 when atc.DATA_TYPE like 'INTERVAL DAY% TO SECOND%' then
                   '''P''
                   || extract(DAY FROM "' || atc.COLUMN_NAME || '") || ''D''
                   || ''T'' || case when extract(HOUR FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(HOUR FROM  "' || atc.COLUMN_NAME || '") ||  ''H'' end
                   || case when extract(MINUTE FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(MINUTE FROM  "' || atc.COLUMN_NAME || '") || ''M'' end
                   || case when extract(SECOND FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(SECOND FROM  "' || atc.COLUMN_NAME || '") ||  ''S'' end "' || atc.COLUMN_NAME || '"'
                 when atc.DATA_TYPE  like 'INTERVAL YEAR% TO MONTH%' then
                   '''P''
                   || extract(YEAR FROM "' || atc.COLUMN_NAME || '") || ''Y''
                   || case when extract(MONTH FROM  "' || atc.COLUMN_NAME || '") <> 0 then extract(MONTH FROM  "' || atc.COLUMN_NAME || '") || ''M'' end "' || atc.COLUMN_NAME || '"'
                 when ((atc.DATA_TYPE = 'TIMESTAMP') or (atc.DATA_TYPE like 'TIMESTAMP(%)')) then
                   'TO_CHAR("' || atc.COLUMN_NAME || '",''YYYY-MM-DD"T"HH24:MI:SS' || case when atc.DATA_SCALE > 0 then '.FF' || atc.DATA_SCALE else '' end || '"Z"'')'
                 when atc.DATA_TYPE like 'TIMESTAMP%TIME ZONE' then
                   'TO_CHAR(SYS_EXTRACT_UTC("' || atc.COLUMN_NAME || '"),''YYYY-MM-DD"T"HH24:MI:SS' || case when atc.DATA_SCALE > 0 then '.FF' || atc.DATA_SCALE else '' end || '"Z"'')'
                 when ((atc.DATA_TYPE_OWNER = 'MDSYS') and (atc.DATA_TYPE  in ('SDO_GEOMETRY'))) then
                   'case when t."' ||  atc.COLUMN_NAME || '".ST_isValid() = 1 then t."' ||  atc.COLUMN_NAME || '".get_WKT() else NULL end "' || atc.COLUMN_NAME || '"'
                 when atc.DATA_TYPE = 'XMLTYPE' then  -- Can be owned by SYS or PUBLIC
                   'case when "' ||  atc.COLUMN_NAME || '" is NULL then NULL else XMLSERIALIZE(CONTENT "' ||  atc.COLUMN_NAME || '" as CLOB) end "' || atc.COLUMN_NAME || '"'
                 when atc.DATA_TYPE = 'BFILE' then
                   'OBJECT_SERIALIZATION.SERIALIZE_BFILE("' || atc.COLUMN_NAME || '") "' || atc.COLUMN_NAME || '"'
                 when atc.DATA_TYPE = 'ANYDATA' then  -- Can be owned by SYS or PUBLIC
                   'case when "' ||  atc.COLUMN_NAME || '" is NULL then NULL else OBJECT_SERIALIZATION.SERIALIZE_ANYDATA("' ||  atc.COLUMN_NAME || '") end "' || atc.COLUMN_NAME || '"'
                 when TYPECODE = 'COLLECTION' then
                   'case when "' || atc.COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(''' || aat.OWNER || ''',ANYDATA.convertCollection("' || atc.COLUMN_NAME || '")) end "' || atc.COLUMN_NAME || '"'
                 when TYPECODE = 'OBJECT' then
                   'case when "' || atc.COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(''' || aat.OWNER || ''',ANYDATA.convertObject("' || atc.COLUMN_NAME || '")) end "' || atc.COLUMN_NAME || '"'
                 /*     
                 ** Comment out unsupported scalar data types and Object types
                 */
                 else
                   '"' || atc.COLUMN_NAME || '"'
               end
        order by INTERNAL_COLUMN_ID) as T_VC4000_TABLE) NODE_SELECT_LIST
    from ALL_ALL_TABLES aat
         inner join ALL_TAB_COLS atc
                 on atc.OWNER = aat.OWNER
                and atc.TABLE_NAME = aat.TABLE_NAME
    left outer join ALL_TYPES at
                 on at.TYPE_NAME = atc.DATA_TYPE
                and at.OWNER = atc.DATA_TYPE_OWNER
    left outer join ALL_JSON_COLUMNS jc
                 on jc.COLUMN_NAME = atc.COLUMN_NAME
                AND jc.TABLE_NAME = atc.TABLE_NAME
                and jc.OWNER = atc.OWNER
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
           ((TABLE_TYPE is NULL) and (HIDDEN_COLUMN = 'NO'))
         or 
           ((TABLE_TYPE is not NULL) and (atc.COLUMN_NAME in ('SYS_NC_ROWINFO$','SYS_NC_OID$','ACLOID','OWNERID')))
         )        
	 and amv.MVIEW_NAME is NULL
     and aat.OWNER in (select COLUMN_VALUE from TABLE(V_SCHEMA_LIST))
	 and case
    	   when P_TABLE_NAME is NULL then 1
	       when P_TABLE_NAME is not NULL and aat.TABLE_NAME = P_TABLE_NAME then 1
		   else 0
  		 end = 1
   group by aat.OWNER, aat.TABLE_NAME;
   
  V_FIRST_ROW BOOLEAN := TRUE;
  V_OBJECT_SERIALIZATION CLOB;
  V_ROW                  EXPORT_METADATA_RECORD;

begin
--
   select SCHEMA
     bulk collect into V_SCHEMA_LIST
	 from JSON_TABLE( P_OWNER_LIST,'$[*]' columns (SCHEMA VARCHAR(128) PATH '$'));

   if ((V_SCHEMA_LIST is NULL) or (V_SCHEMA_LIST.count = 0)) then 
     V_SCHEMA_LIST := T_VC4000_TABLE(P_OWNER_LIST);
	end if;
--
  /* Create a  SQL statement for each of the tables in the schema */
  declare
    V_SQL_STATEMENT        CLOB;
	V_TABLE_METADATA       CLOB;
  begin

    for t in getTableMetadata loop  
	
	  V_OBJECT_SERIALIZATION := OBJECT_SERIALIZATION.SERIALIZE_TABLE_TYPES(t.OWNER,t.TABLE_NAME);
	  
	  DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
      V_SQL_FRAGMENT := 'select JSON_ARRAY(';	  
      DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
      DBMS_LOB.APPEND(V_SQL_STATEMENT,TABLE_TO_LIST(t.EXPORT_SELECT_LIST));
      V_SQL_FRAGMENT := ' NULL on NULL returning '|| C_RETURN_TYPE || ') "JSON" from "' || t.OWNER || '"."' || t.TABLE_NAME || '" t';
      DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

 	  V_ROW.OWNER                := t.OWNER;
	  V_ROW.TABLE_NAME           := t.TABLE_NAME;
	  V_ROW.COLUMN_LIST          := TABLE_TO_LIST(t.COLUMN_LIST);
	  V_ROW.DATA_TYPE_LIST       := '[' || TABLE_TO_LIST(t.DATA_TYPE_LIST) || ']';
	  V_ROW.SIZE_CONSTRAINTS     := '[' || TABLE_TO_LIST(t.SIZE_CONSTRAINT_LIST) || ']';
	  V_ROW.EXPORT_SELECT_LIST   := TABLE_TO_LIST(t.EXPORT_SELECT_LIST);
	  V_ROW.NODE_SELECT_LIST     := TABLE_TO_LIST(t.NODE_SELECT_LIST);
      V_ROW.WITH_CLAUSE          := V_OBJECT_SERIALIZATION;
	  V_ROW.SQL_STATEMENT        := V_SQL_STATEMENT;
	  
	  PIPE ROW(V_ROW);

    end loop;
  end;  
--
end;
--
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
$ELSE
procedure JSON_ARRAYAGG(P_JSON_DOCUMENT IN OUT CLOB, P_CURSOR SYS_REFCURSOR)
as
  JSON_ARRAY_OVERFLOW EXCEPTION; PRAGMA EXCEPTION_INIT (JSON_ARRAY_OVERFLOW, -40478);
  V_SEPERATOR         VARCHAR2(1) := ',';
  $IF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  V_ARRAY_MEMBER VARCHAR2(32767);
  $ELSE
  V_ARRAY_MEMBER VARCHAR2(4000);
  $END
  V_START_ARRAY_DATA  PLS_INTEGER;
  V_JSON_ARRAY_ERROR  CLOB;
  V_FIRST_MEMBER      BOOLEAN := true;
begin
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'[');  
  V_START_ARRAY_DATA := DBMS_LOB.GETLENGTH(P_JSON_DOCUMENT);
  loop
    begin
      fetch P_CURSOR into V_ARRAY_MEMBER;
      exit when P_CURSOR%notfound;
      if (NOT V_FIRST_MEMBER) then DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length(V_SEPERATOR),V_SEPERATOR); end if;
      V_FIRST_MEMBER := false;
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length(V_ARRAY_MEMBER),V_ARRAY_MEMBER);
    exception
      when others then
        select JSON_OBJECT(
	  	         'error' value DBMS_UTILITY.FORMAT_ERROR_STACK
--
                 $IF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                 returning VARCHAR2(32767)
                 $ELSE
                 returning VARCHAR2(4000)
                 $END
--
               )
          into V_JSON_ARRAY_ERROR
	      from DUAL;
        DBMS_LOB.TRIM(P_JSON_DOCUMENT,V_START_ARRAY_DATA);
        DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,LENGTH(V_JSON_ARRAY_ERROR),V_JSON_ARRAY_ERROR);
        exit;
    end;
  end loop;
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,']');  
end;
--
function JSON_ARRAYAGG(P_CURSOR SYS_REFCURSOR) 
return CLOB
as
  V_RESULT CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_RESULT,TRUE,DBMS_LOB.SESSION);
  JSON_ARRAYAGG(V_RESULT,P_CURSOR);
  return V_RESULT;
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