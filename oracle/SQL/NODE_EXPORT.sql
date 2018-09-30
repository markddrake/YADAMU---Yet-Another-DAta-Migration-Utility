create or replace package NODE_EXPORT
authid CURRENT_USER
as
  TYPE NODE_EXPORT_METADATA_RECORD is RECORD (
    OWNER                VARCHAR2(128)
   ,TABLE_NAME           VARCHAR2(128)
   ,COLUMN_LIST          CLOB
   ,DATA_TYPE_LIST       CLOB
   ,SIZE_CONSTRAINTS     CLOB
   ,EXPORT_SELECT_LIST   CLOB
   ,IMPORT_SELECT_LIST   CLOB
   ,COLUMN_PATTERN_LIST  CLOB
   ,DESERIALIZATION_INFO CLOB
   ,SQL_STATEMENT        CLOB
  );
  
  TYPE NODE_EXPORT_METADATA_TABLE IS TABLE OF NODE_EXPORT_METADATA_RECORD;
  
  function JSON_EXPORT_DDL(P_OWNER_LIST VARCHAR2,P_TABLE_NAME VARCHAR2 DEFAULT NULL) return NODE_EXPORT_METADATA_TABLE PIPELINED;
  function JSON_FEATURES return VARCHAR2 deterministic;
  function DATABASE_RELEASE return NUMBER deterministic;
	
END;
/
--
show errors
--
create or replace package BODY NODE_EXPORT
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
function DESERIALIZATION_FUNCTION_LIST(P_BFILE_COUNT NUMBER, P_BLOB_COUNT NUMBER, B_ANYDATA_COUNT NUMBER)
/*
** Deserialization functions for BFILE and BLOB data types are exposed direclty by the OBJECT_SERIALIZATION package
** This allows them to called from EXECUTE IMMEDIATE operations when they appear inside serialized objects 
** Since the functions are exposed by OBJECT_SERIALIZATION they do not need to be supplied using a WITH clause
** ANYDATA is de-serialised using methods exposed by the ANYDATA type.
*/
return VARCHAR2
as
  V_FUNCTION_LIST T_VC4000_TABLE := T_VC4000_TABLE();
begin
  if (P_BFILE_COUNT > 0) then
    V_FUNCTION_LIST.extend();
	V_FUNCTION_LIST(V_FUNCTION_LIST.count) := '"CHAR2BFILE"';
  end if;
  if (P_BLOB_COUNT > 0) then
    V_FUNCTION_LIST.extend();
	V_FUNCTION_LIST(V_FUNCTION_LIST.count) := '"HEXBINARY2BLOB"';
  end if;
  /* ANYDATA is deserialzied using the native functionality of the ANYDATA data type */
  return TABLE_TO_LIST(V_FUNCTION_LIST);
end;
--
procedure GENERATE_WITH_CLAUSE(P_OBJECT_SERAIALIZATION CLOB, P_BFILE_COUNT NUMBER, P_BLOB_COUNT NUMBER, P_ANYDATA_COUNT NUMBER, P_SQL_STATEMENT IN OUT CLOB)
as
begin
  if ((P_OBJECT_SERAIALIZATION is not NULL) or(P_BFILE_COUNT + P_BLOB_COUNT + P_ANYDATA_COUNT > 0)) then
    DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB('WITH' || C_NEWLINE));
	if (P_OBJECT_SERAIALIZATION is not NULL) then
      DBMS_LOB.APPEND(P_SQL_STATEMENT,P_OBJECT_SERAIALIZATION);
	else
      if ((P_BFILE_COUNT > 0) or (P_ANYDATA_COUNT > 0)) then
        DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(OBJECT_SERIALIZATION.CODE_BFILE2CHAR));
      end if;
      if ((P_BLOB_COUNT > 0) or (P_ANYDATA_COUNT > 0)) then
        DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(OBJECT_SERIALIZATION.CODE_BLOB2HEXBINARY));
      end if;
      if (P_ANYDATA_COUNT > 0) then
        DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(OBJECT_SERIALIZATION.CODE_SERIALIZE_ANYDATA));
      end if;
    end if;
  end if;
end;
--
function JSON_EXPORT_DDL(P_OWNER_LIST VARCHAR2,P_TABLE_NAME VARCHAR2 DEFAULT NULL) 
return NODE_EXPORT_METADATA_TABLE 
PIPELINED
/*
** Generate the SQL Statements to create a JSON document from the contents of the supplied schemas
*/
as
  V_SQL_FRAGMENT  VARCHAR2(32767);
  
  V_SCHEMA_LIST T_VC4000_TABLE;
 
  cursor getTableMetadata
  is
  select aat.owner
        ,aat.table_name
        ,sum(case when DATA_TYPE = 'BLOB'  then 1 else 0 end) BLOB_COUNT
        ,sum(case when DATA_TYPE = 'BFILE' then 1 else 0 end) BFILE_COUNT
        ,sum(case when DATA_TYPE = 'ANYDATA' then 1 else 0 end) ANYDATA_COUNT
		,sum(case when TYPECODE in ('COLLECTION', 'OBJECT') then 1 else 0 end) OBJECT_COUNT
        ,cast(collect('"' || COLUMN_NAME || '"' ORDER BY INTERNAL_COLUMN_ID) as T_VC4000_TABLE) COLUMN_LIST
		,cast(collect(case when DATA_TYPE_OWNER is null then '"' || DATA_TYPE || '"' else '"' || DATA_TYPE_OWNER || '"."' || DATA_TYPE || '"' end ORDER BY INTERNAL_COLUMN_ID) as T_VC4000_TABLE) DATA_TYPE_LIST
	    ,cast(collect( 
		       case
			     when DATA_TYPE in ('VARCHAR2', 'CHAR', 'NVARCHAR2') 
                   then case 
                          when (CHAR_LENGTH < DATA_LENGTH) 
                            then '"' || CHAR_LENGTH || '"'
                            else '"' || DATA_LENGTH || '"'
                        end
                 when DATA_TYPE in ('NVARCHAR2', 'CHAR', 'UROWID', 'RAW') or  DATA_TYPE LIKE 'INTERVAL%' 
                   then '"' || DATA_LENGTH || '"'
                 when DATA_TYPE = 'NUMBER' 
                   then case 
                          when DATA_SCALE is NOT NULL and DATA_SCALE <> 0
                            then '"' || DATA_PRECISION || ',' || DATA_SCALE || '"'
                          when DATA_PRECISION is NOT NULL
                            then '"' || DATA_PRECISION || '"'
                          else 
						    '"38"'
                        end 
                 when DATA_TYPE = 'FLOAT' 
                   then '"' || DATA_PRECISION || '"'
                 else
                   '""'
               end
	           ORDER BY INTERNAL_COLUMN_ID) as T_VC4000_TABLE) SIZE_CONSTRAINT_LIST
        ,cast(collect(
               case
                 -- For some reason RAW columns have DATA_TYPE_OWNER set to the current schema.
                 when DATA_TYPE = 'RAW'
                   then '"' || COLUMN_NAME || '"'
                 $IF not JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                 /*
                 ** Pre 18.1 Some Scalar Data Types are not natively supported by JSON_ARRAY()
                 */
                 when DATA_TYPE in ('BINARY_DOUBLE','BINARY_FLOAT')
                   then 'TO_CHAR("' || COLUMN_NAME || '")'
                 when DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE'
                   then 'TO_CHAR(SYS_EXTRACT_UTC("' || COLUMN_NAME || '"),''IYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'')'
                 when DATA_TYPE like 'INTERVAL DAY% TO SECOND%'
                   then '''P''
                        || extract(DAY FROM "' || COLUMN_NAME || '") || ''D''
                        || ''T'' || case when extract(HOUR FROM  "' || COLUMN_NAME || '") <> 0 then extract(HOUR FROM  "' || COLUMN_NAME || '") ||  ''H'' end
                        || case when extract(MINUTE FROM  "' || COLUMN_NAME || '") <> 0 then extract(MINUTE FROM  "' || COLUMN_NAME || '") || ''M'' end
                        || case when extract(SECOND FROM  "' || COLUMN_NAME || '") <> 0 then extract(SECOND FROM  "' || COLUMN_NAME || '") ||  ''S'' end'
                 when DATA_TYPE  like 'INTERVAL YEAR% TO MONTH%'
                   then '''P''
                        || extract(YEAR FROM "' || COLUMN_NAME || '") || ''Y''
                        || case when extract(MONTH FROM  "' || COLUMN_NAME || '") <> 0 then extract(MONTH FROM  "' || COLUMN_NAME || '") || ''M'' end'
                 when DATA_TYPE in ('NCHAR','NVARCHAR2')
                   then 'TO_CHAR("' || COLUMN_NAME || '")'
                 when DATA_TYPE = 'NCLOB'
                   then 'TO_CLOB("' || COLUMN_NAME || '")'
                 /*
                 ** 18.1 compatible handling of BLOB
                 */
                 when DATA_TYPE = 'BLOB'
                   then 'BLOB2HEXBINARY("' || COLUMN_NAME || '")'                          
                 $END
                 /*
                 ** Quick Fixes for datatypes not natively supported
                 */
                 when DATA_TYPE = 'XMLTYPE'  -- Can be owned by SYS or PUBLIC
                   then 'case when "' ||  COLUMN_NAME || '" is NULL then NULL else XMLSERIALIZE(CONTENT "' ||  COLUMN_NAME || '" as CLOB) end'
                 when DATA_TYPE = 'ROWID' or DATA_TYPE = 'UROWID'
                   then 'ROWIDTOCHAR("' || COLUMN_NAME || '")'
                 /*
                 ** Fix for BFILENAME
                 */
                 when DATA_TYPE = 'BFILE'
                   then 'BFILE2CHAR("' || COLUMN_NAME || '")'
                 /*
                 **
                 ** Support ANYDATA, OBJECT and COLLECTION types
                 **
                 */
                 when DATA_TYPE = 'ANYDATA'  -- Can be owned by SYS or PUBLIC
                   then 'case when "' ||  COLUMN_NAME || '" is NULL then NULL else SERIALIZE_ANYDATA("' ||  COLUMN_NAME || '") end'
                 when TYPECODE = 'COLLECTION'
                   then 'case when "' || COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(''' || aat.OWNER || ''',ANYDATA.convertCollection("' || COLUMN_NAME || '")) end'
                 when TYPECODE = 'OBJECT'
                   then 'case when "' || COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(''' || aat.OWNER || ''',ANYDATA.convertObject("' || COLUMN_NAME || '")) end'
                 /*     
                 ** Comment out unsupported scalar data types and Object types
                 */
                 when DATA_TYPE in ('LONG','LONG RAW')
                   then '''"' || COLUMN_NAME || '". Unsupported data type ["' || DATA_TYPE || '"]'''
                 else
                   '"' || COLUMN_NAME || '"'
               end
        order by INTERNAL_COLUMN_ID) as T_VC4000_TABLE) EXPORT_SELECT_LIST
	   ,cast(collect(
		       /* Cast JSON representation back into SQL data type where implicit coversion does happen or results in incorrect results */
		       case
			     when DATA_TYPE = 'BFILE'
				    then 'case when "' || COLUMN_NAME || '" is NULL then NULL else OBJECT_SERIALIZATION.CHAR2BFILE("' || COLUMN_NAME || '") end'
			     when DATA_TYPE = 'XMLTYPE'
				    then 'case when "' || COLUMN_NAME || '" is NULL then NULL else XMLTYPE("' || COLUMN_NAME || '") end'
				 when DATA_TYPE = 'ANYDATA'
				    --- ### TODO - Better deserialization of ANYDATA.
				    then 'case when "' || COLUMN_NAME || '" is NULL then NULL else ANYDATA.convertVARCHAR2("' || COLUMN_NAME || '") end'
				 when TYPECODE = 'COLLECTION'
			       then '"#' || DATA_TYPE || '"("' || COLUMN_NAME || '")'
				 when TYPECODE = 'OBJECT'
  				   then '"#' || DATA_TYPE || '"("' || COLUMN_NAME || '")'
			     when DATA_TYPE = 'BLOB'
    		        $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
				    then 'case when "' || COLUMN_NAME || '" is NULL then NULL else OBJECT_SERIALIZATION.HEXBINARY2BLOB("' || COLUMN_NAME || '") end'
				    $ELSE
				    then 'case when "' || COLUMN_NAME || '" is NULL then NULL when substr("' || COLUMN_NAME || '",1,15) = ''BLOB2HEXBINARY:'' then NULL else HEXTORAW("' || COLUMN_NAME || '") end'
				 $END
				 else
				    '"' || COLUMN_NAME || '"'
			   end
	     ORDER BY INTERNAL_COLUMN_ID) as T_VC4000_TABLE) IMPORT_SELECT_LIST
       ,cast(collect(
               /* JSON_TABLE column patterns data types */
               /* Map data types not supported by JSON_TABLE to data types supported by JSON_TABLE */
               case
                 when DATA_TYPE in ('CHAR','NCHAR','NVARCHAR2','RAW','BFILE','ROWID','UROWID') or DATA_TYPE like 'INTERVAL%'
                   then '"VARCHAR2"'
                 when DATA_TYPE like 'TIMESTAMP%WITH LOCAL TIME ZONE'
                   then '"TIMESTAMP WITH TIME ZONE"'
                 when DATA_TYPE in ('XMLTYPE','CLOB','NCLOB','BLOB','LONG','LONG RAW') or TYPECODE is not NULL
                   $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
                   then '"CLOB"'
                   $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
                   then '"VARCHAR2(32767)"'
                   $ELSE
                   then '"VARCHAR2(4000)"'
				   
                   $END
               else
                 '"' || DATA_TYPE || '"'
               end
         order by INTERNAL_COLUMN_ID) as T_VC4000_TABLE) COLUMN_PATTERN_LIST
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
           ((TABLE_TYPE is NULL) and (HIDDEN_COLUMN = 'NO'))
         or 
           ((TABLE_TYPE is not NULL) and (COLUMN_NAME in ('SYS_NC_ROWINFO$','SYS_NC_OID$','ACLOID','OWNERID')))
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
  V_DESERIALIZATION_LIST CLOB;
  V_OBJECT_SERIALIZATION CLOB;
  V_ROW                  NODE_EXPORT_METADATA_RECORD;

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
	
	  DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
	  V_OBJECT_SERIALIZATION := OBJECT_SERIALIZATION.SERIALIZE_TABLE_TYPES(t.OWNER,t.TABLE_NAME);
	  GENERATE_WITH_CLAUSE(V_OBJECT_SERIALIZATION,t.BFILE_COUNT,t.BLOB_COUNT,t.ANYDATA_COUNT,V_SQL_STATEMENT);
	  
	  if (DBMS_LOB.GETLENGTH(V_OBJECT_SERIALIZATION) > 0 ) then
	    V_DESERIALIZATION_LIST := '"OBJECTS"';
	  else
	    V_DESERIALIZATION_LIST := DESERIALIZATION_FUNCTION_LIST(t.BFILE_COUNT,t.BLOB_COUNT,t.ANYDATA_COUNT);
	  end if;

      V_SQL_FRAGMENT := 'select JSON_ARRAY(';
	  
      DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
      DBMS_LOB.APPEND(V_SQL_STATEMENT,TABLE_TO_LIST(t.EXPORT_SELECT_LIST));
      V_SQL_FRAGMENT := ' NULL on NULL returning '|| C_RETURN_TYPE || ') "JSON" from "' || t.OWNER || '"."' || t.TABLE_NAME || '"';
      DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

 	  V_ROW.OWNER                := t.OWNER;
	  V_ROW.TABLE_NAME           := t.TABLE_NAME;
	  V_ROW.COLUMN_LIST          := TABLE_TO_LIST(t.COLUMN_LIST);
	  V_ROW.DATA_TYPE_LIST       := TABLE_TO_LIST(t.DATA_TYPE_LIST);
	  V_ROW.SIZE_CONSTRAINTS     := TABLE_TO_LIST(t.SIZE_CONSTRAINT_LIST);
	  V_ROW.EXPORT_SELECT_LIST   := TABLE_TO_LIST(t.EXPORT_SELECT_LIST);
	  V_ROW.IMPORT_SELECT_LIST   := TABLE_TO_LIST(t.IMPORT_SELECT_LIST);
	  V_ROW.COLUMN_PATTERN_LIST  := TABLE_TO_LIST(t.COLUMN_PATTERN_LIST);
	  V_ROW.DESERIALIZATION_INFO := V_DESERIALIZATION_LIST;
	  V_ROW.SQL_STATEMENT        := V_SQL_STATEMENT;
	  
	  PIPE ROW(V_ROW);

    end loop;
  end;  
--
end;
--
end;
/
--
show errors
--
