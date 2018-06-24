--
create or replace package JSON_EXPORT
AUTHID CURRENT_USER
as
/*
**
**  VERSION 6
**
**  Generate the COLUMN PATTERNS that are required to allow JSON_TABLE to be used to convert the contents of each JSON_ARRAY back into a relational row.
**  Add the COLUMN PATTERN clauses to the table metadata.
**
*/
  type T_CLOB_TAB is table of CLOB;

  type T_JSON_ARRAY_OUTPUT_TAB is table of
    $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
    CLOB;
    $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
    VARCHAR2(32767);
    $ELSE
    VARCHAR2(4000);
    $END

  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
     SQL_STATEMENT CLOB;
  $END

  type T_EXPORT_METADATA_REC is record (
     OWNER                     VARCHAR2(128)
   , TABLE_NAME                VARCHAR2(128)
   , EXPORT_SELECT_LIST        CLOB
   , TABLE_METADATA            CLOB
   $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
   $ELSE
   , SQL_STATEMENT             CLOB
   $END
  );

  type T_EXPORT_METADATA_TAB is table of T_EXPORT_METADATA_REC;

  EXPORT_METADATA_TABLE T_EXPORT_METADATA_TAB;

  procedure GENERATE_EXPORT_METADATA(P_SCHEMA VARCHAR2, P_TABLE_NAME VARCHAR2 DEFAULT NULL);
  function EXPORT_METADATA return T_EXPORT_METADATA_TAB pipelined;

  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  function DUMP_SQL_STATEMENT return CLOB;
  $ELSE
  function EXPORT_TABLE_DATA(P_CURSOR SYS_REFCURSOR) return T_JSON_ARRAY_OUTPUT_TAB pipelined;
  $END

  function EXPORT_TABLE(P_SCHEMA VARCHAR2, P_TABLE_NAME VARCHAR2) return T_CLOB_TAB pipelined;
  function EXPORT_SCHEMA(P_SCHEMA VARCHAR2) return T_CLOB_TAB pipelined;

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
function DUMP_SQL_STATEMENT
return CLOB
as
begin
  return SQL_STATEMENT;
end;
$END
--
function EXPORT_METADATA
return T_EXPORT_METADATA_TAB
pipelined
as
  cursor getRecords
  is
  select *
    from TABLE(EXPORT_METADATA_TABLE);
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
FUNCTION TABLE_TO_LIST(P_TABLE XDB.XDB$STRING_LIST_T,P_DELIMITER VARCHAR2 DEFAULT ',')
return CLOB
as
  V_LIST CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_LIST,TRUE,DBMS_LOB.CALL);
  for i in P_TABLE.FIRST .. P_TABLE.LAST loop
    if (i > 1) then
  	  DBMS_LOB.WRITEAPPEND(V_LIST,LENGTH(P_DELIMITER),P_DELIMITER);
	end if;
	DBMS_LOB.WRITEAPPEND(V_LIST,LENGTH(P_TABLE(i)),P_TABLE(i));
  end loop;
  return V_LIST;
end;
--
procedure GENERATE_EXPORT_METADATA(P_SCHEMA VARCHAR2, P_TABLE_NAME VARCHAR2)
/*
**
** Generate metadata and a SQL Select Statement for each table to be be exported the statements in a PL/SQL table
**
*/
as
  V_SQL_FRAGMENT  VARCHAR2(32767);
  V_SQL_STATEMENT CLOB;

  V_WITH_BLOCK_REQUIRED BOOLEAN;
  V_OBJECT_SERIALIZER   CLOB;

  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
	V_RETURN_TYPE VARCHAR2(32) := 'CLOB';
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
	V_RETURN_TYPE VARCHAR2(32):= 'VARCHAR2(32767)';
  $ELSE
    V_RETURN_TYPE VARCHAR2(32):= 'VARCHAR2(4000)';
  $END

  V_COLUMN_LIST          CLOB;
  V_DATA_TYPE_LIST       CLOB;
  V_EXPORT_SELECT_LIST   CLOB;
  V_COLUMN_PATTERN_LIST  CLOB;
  V_EXPORT_METADATA      CLOB;

  cursor getTableMetadata
  is
  select aat.owner
        ,aat.table_name
  	    ,SUM(CASE WHEN DATA_TYPE = 'BLOB'  THEN 1 ELSE 0 END) BLOB_COUNT
  	    ,SUM(CASE WHEN DATA_TYPE = 'BFILE' THEN 1 ELSE 0 END) BFILE_COUNT
        ,SUM(CASE WHEN DATA_TYPE = 'ANYDATA' THEN 1 ELSE 0 END) ANYDATA_COUNT
 		,cast(collect('"' || COLUMN_NAME || '"' ORDER BY INTERNAL_COLUMN_ID) as XDB.XDB$STRING_LIST_T) COLUMN_LIST
		,cast(collect(case when DATA_TYPE_OWNER is null then '"' || DATA_TYPE || '"' else '"' || DATA_TYPE_OWNER || '"."' || DATA_TYPE || '"' end ORDER BY INTERNAL_COLUMN_ID) as XDB.XDB$STRING_LIST_T) DATA_TYPE_LIST
	    ,cast(collect(
			   case
  		         $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
			     $ELSE
			     /*
			     **
			     ** Pre 18.1 Some Scalar Data Types are not natively supported by JSON_ARRAY()
			     **
			     */
			     when DATA_TYPE in ('BINARY_DOUBLE','BINARY_FLOAT')
			       then 'TO_CHAR("' || COLUMN_NAME || '")'
			     when DATA_TYPE LIKE 'TIMESTAMP%WITH LOCAL TIME ZONE'
				   then 'TO_CHAR(SYS_EXTRACT_UTC("' || COLUMN_NAME || '"),''IYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'')'
			     when DATA_TYPE LIKE 'INTERVAL DAY% TO SECOND%'
			       then '''P''
				        || extract(DAY FROM "' || COLUMN_NAME || '") || ''D''
                        || ''T'' || CASE WHEN extract(HOUR FROM  "' || COLUMN_NAME || '") <> 0 THEN extract(HOUR FROM  "' || COLUMN_NAME || '") ||  ''H'' END
	                    || CASE WHEN extract(MINUTE FROM  "' || COLUMN_NAME || '") <> 0 THEN extract(MINUTE FROM  "' || COLUMN_NAME || '") || ''M'' end
	                    || CASE WHEN extract(SECOND FROM  "' || COLUMN_NAME || '") <> 0 THEN extract(SECOND FROM  "' || COLUMN_NAME || '") ||  ''S'' end'
			     when DATA_TYPE  LIKE 'INTERVAL YEAR% TO MONTH%'
			       then '''P''
				        || extract(YEAR FROM "' || COLUMN_NAME || '") || ''Y''
	                    || CASE WHEN extract(MONTH FROM  "' || COLUMN_NAME || '") <> 0 THEN extract(MONTH FROM  "' || COLUMN_NAME || '") || ''M'' end'
				 when DATA_TYPE in ('NCHAR','NVARCHAR2')
			       then 'TO_CHAR("' || COLUMN_NAME || '")'
			     when DATA_TYPE = 'NCLOB'
			       then 'TO_CLOB("' || COLUMN_NAME || '")'
				 $END
				 /*
				 **
				 ** 18.1 compatible handling of BLOB
				 **
				 */
			     when DATA_TYPE = 'BLOB'
			       then 'BLOB2HEXBINARY("' || COLUMN_NAME || '")' 				 /*
				 **
				 ** Quick Fixes for datatypes not natively supported
				 **
				 */
	 			 when DATA_TYPE = 'XMLTYPE'  -- Can be owned by SYS or PUBLIC
                   then 'case when "' ||  COLUMN_NAME || '" is NULL then NULL else XMLSERIALIZE(CONTENT "' ||  COLUMN_NAME || '" as CLOB) end'
	 			 when DATA_TYPE = 'ROWID' or DATA_TYPE = 'UROWID'
			       then 'ROWIDTOCHAR("' || COLUMN_NAME || '")'
				 /*   				 /*
				 **
				 ** Fix for BFILENAME
				 **
				 */
			     when DATA_TYPE = 'BFILE'
			       then 'BFILE2CHAR("' || COLUMN_NAME || '")'
	 			 when DATA_TYPE = 'ANYDATA'  -- Can be owned by SYS or PUBLIC
                   then 'case when "' ||  COLUMN_NAME || '" is NULL then NULL else SERIALIZE_ANYDATA("' ||  COLUMN_NAME || '") end'
				 /*
				 **
				 ** Support OBJECT and COLLECTION types
				 **
				 */
				 when TYPECODE = 'COLLECTION'
			       then 'case when "' || COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(ANYDATA.convertCollection("' || COLUMN_NAME || '")) end'
				 when TYPECODE = 'OBJECT'
  				   then 'case when "' || COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(ANYDATA.convertObject("' || COLUMN_NAME || '")) end'
			     /*
				 **
				 ** Comment out Unsupported Data Types including objects and collections
				 **
				 */
			   	 when DATA_TYPE in ('LONG','LONG RAW')
			       then '''"' || COLUMN_NAME || '". Unsupported data type ["' || DATA_TYPE || '"]'''
			     else
   			       '"' || COLUMN_NAME || '"'
		       end
		 ORDER BY INTERNAL_COLUMN_ID) as XDB.XDB$STRING_LIST_T) EXPORT_SELECT_LIST
	    ,cast(collect(
		     /* COLUMN_NAME DATA_TYPE PATH '$[idx]' */
		     '"' || COLUMN_NAME || '" ' || DATA_TYPE 
		 ORDER BY INTERNAL_COLUMN_ID) as XDB.XDB$STRING_LIST_T) COLUMN_PATTERN_LIST
    from ALL_ALL_TABLES aat
	     inner join ALL_TAB_COLS atc
		         on atc.OWNER = aat.OWNER
		        and atc.TABLE_NAME = aat.TABLE_NAME
         left outer join ALL_TYPES at
	                  on at.TYPE_NAME = atc.DATA_TYPE
                     and at.OWNER = atc.DATA_TYPE_OWNER
   where aat.STATUS = 'VALID'
     and aat.DROPPED = 'NO'
	 and aat.TEMPORARY = 'N'
     and aat.EXTERNAL = 'NO'
	 and aat.NESTED = 'NO'
	 and aat.SECONDARY = 'N'
	 and (aat.IOT_TYPE is NULL or aat.IOT_TYPE = 'IOT')
	 /*
     **
	 ** For a relational table we want all columns that are not marked hidden or virtual.
     ** Note binary XMLTYPE columns are marked virtual and should be included
	 **
	 ** For an object table we want the OID and CONTENT columns, additonally if the object table
	 ** is a hierarchically enabled XMLTYPE table then we also need the ACL and Owner information
	 **
	 */
	 and (
	      ((aat.TABLE_TYPE is NULL) and ((atc.HIDDEN_COLUMN = 'NO') and ((atc.VIRTUAL_COLUMN = 'NO') or ((atc.VIRTUAL_COLUMN = 'YES') and (atc.DATA_TYPE = 'XMLTYPE')))))
          or
		  ((aat.TABLE_TYPE is not NULL) and (COLUMN_NAME in ('SYS_NC_OID$','SYS_NC_ROWINFO$')))
	      or
		  ((aat.TABLE_TYPE = 'XMLTYPE') and (COLUMN_NAME in ('ACLOID', 'OWNERID')))
		 )
     and aat.OWNER = P_SCHEMA
     and case
		   when P_TABLE_NAME is NULL then 1
	       when P_TABLE_NAME is not NULL and aat.TABLE_NAME = P_TABLE_NAME then 1
		   else 0
  		 end = 1
   group by aat.OWNER, aat.TABLE_NAME;

  V_BLOB_COUNT           NUMBER := 0;
  V_BFILE_COUNT          NUMBER := 0;
  V_ANYDATA_COUNT        NUMBER := 0;

  V_TABLE_NAME_LIST      XDB.XDB$STRING_LIST_T;

begin
  EXPORT_METADATA_TABLE := T_EXPORT_METADATA_TAB();
  for t in getTableMetadata loop

    -- Append the indexes to the JSON_TABLE column patterns.
    for i in 1 .. t.COLUMN_PATTERN_LIST.count() loop
      t.COLUMN_PATTERN_LIST(i) := t.COLUMN_PATTERN_LIST(i) || ' PATH ''$[' || TO_CHAR(i-1) || ']''';
    end loop;														
    EXPORT_METADATA_TABLE.extend();
	EXPORT_METADATA_TABLE(EXPORT_METADATA_TABLE.count).OWNER := t.OWNER;
	EXPORT_METADATA_TABLE(EXPORT_METADATA_TABLE.count).TABLE_NAME := t.TABLE_NAME;
	V_COLUMN_LIST := TABLE_TO_LIST(t.COLUMN_LIST);
	V_DATA_TYPE_LIST := TABLE_TO_LIST(t.DATA_TYPE_LIST);
	V_EXPORT_SELECT_LIST := TABLE_TO_LIST(t.EXPORT_SELECT_LIST);
    V_COLUMN_PATTERN_LIST := TABLE_TO_LIST(t.COLUMN_PATTERN_LIST);

    $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
	V_SQL_STATEMENT  := NULL;					 
  	V_BFILE_COUNT := V_BFILE_COUNT + t.BFILE_COUNT;
	V_BLOB_COUNT  := V_BLOB_COUNT + t.BLOB_COUNT;
	V_ANYDATA_COUNT  := V_ANYDATA_COUNT + t.ANYDATA_COUNT;
    $ELSE
    DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);

	V_OBJECT_SERIALIZER := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.SERIALIZE_TABLE_TYPES(t.OWNER,t.TABLE_NAME);
	V_WITH_BLOCK_REQUIRED := (V_OBJECT_SERIALIZER is not NULL) or ((t.BFILE_COUNT + t.BLOB_COUNT + t.ANYDATA_COUNT) > 0);

    if (V_WITH_BLOCK_REQUIRED) then
      V_SQL_FRAGMENT := 'WITH' ||  C_NEWLINE;
      DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

      if (V_OBJECT_SERIALIZER is not null) then
        DBMS_LOB.APPEND(V_SQL_STATEMENT,V_OBJECT_SERIALIZER);
	  else
        V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.PROCEDURE_CHECK_SIZE;
        DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  	    if ((t.BFILE_COUNT > 0) or (t.ANYDATA_COUNT > 0)) then
          V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.FUNCTION_BFILE2CHAR;
          DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	    end if;
	    if ((t.BLOB_COUNT > 0) or (t.ANYDATA_COUNT > 0)) then
          V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.FUNCTION_BLOB2HEXBINARY;
          DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	    end if;
	    if (t.ANYDATA_COUNT > 0) then
          V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.PROCEDURE_SERIALIZE_ANYDATA;
          DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	    end if;
	  end if;
    end if;

    V_SQL_FRAGMENT := 'select JSON_ARRAY(';
    DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    DBMS_LOB.APPEND(V_SQL_STATEMENT,V_EXPORT_SELECT_LIST);
    V_SQL_FRAGMENT := ' NULL on NULL returning '|| V_RETURN_TYPE || ') FROM "' || t.OWNER || '"."' || t.TABLE_NAME || '"';
    DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    EXPORT_METADATA_TABLE(EXPORT_METADATA_TABLE.count).SQL_STATEMENT := V_SQL_STATEMENT;
	$END

	begin
	  /*
	  **
	  ** PL/SQL JSON_OBJECT does not support CLOB return, even in 18.1
	  **
	  */
	  select JSON_OBJECT(
				'owner' value t.OWNER				 
	          , 'tableName' value t.TABLE_NAME
	          ,'columns' value V_COLUMN_LIST
			  ,'dataTypes'  value V_DATA_TYPE_LIST
	          ,'exportSelectList' value V_EXPORT_SELECT_LIST
			  ,'columnPatterns' value V_COLUMN_PATTERN_LIST
			   $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
			   returning CLOB
			   $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
	           returning VARCHAR2(32767)
               $ELSE
               returning VARCHAR2(4000)
               $END
             )
	    into V_EXPORT_METADATA
		from dual;
    exception
	  when OTHERS then
	    V_EXPORT_METADATA := JSON_OBJECT(
                              'tableName' value t.TABLE_NAME
		                     ,'error' value 'ORA-' || SQLCODE
							 ,'message' value SQLERRM
							);
	end;
	
    EXPORT_METADATA_TABLE(EXPORT_METADATA_TABLE.count).EXPORT_SELECT_LIST := V_EXPORT_SELECT_LIST;
	EXPORT_METADATA_TABLE(EXPORT_METADATA_TABLE.count).TABLE_METADATA := V_EXPORT_METADATA;			
	
  end loop;

  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  DBMS_LOB.CREATETEMPORARY(SQL_STATEMENT,TRUE,DBMS_LOB.CALL);

  select TABLE_NAME
	bulk collect into V_TABLE_NAME_LIST
	from TABLE(EXPORT_METADATA_TABLE);

  V_OBJECT_SERIALIZER := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.SERIALIZE_TABLE_TYPES(P_SCHEMA,V_TABLE_NAME_LIST);
  V_WITH_BLOCK_REQUIRED := (V_OBJECT_SERIALIZER is not NULL) or ((V_BFILE_COUNT + V_BLOB_COUNT + V_ANYDATA_COUNT) > 0);

  if (V_WITH_BLOCK_REQUIRED) then
    V_SQL_FRAGMENT := 'WITH' ||  C_NEWLINE;
    DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

	if (V_OBJECT_SERIALIZER is not null) then
      DBMS_LOB.APPEND(SQL_STATEMENT,V_OBJECT_SERIALIZER);
	else
      V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.PROCEDURE_CHECK_SIZE;
      DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  	  if ((V_BFILE_COUNT > 0) or (V_ANYDATA_COUNT > 0)) then
        V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.FUNCTION_BFILE2CHAR;
        DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	  end if;
	  if ((V_BLOB_COUNT > 0) or (V_ANYDATA_COUNT > 0)) then
        V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.FUNCTION_BLOB2HEXBINARY;
        DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
      end if;
	  if (V_ANYDATA_COUNT > 0) then
        V_SQL_FRAGMENT := &CURRENT_SCHEMA..OBJECT_SERIALIZATION.PROCEDURE_SERIALIZE_ANYDATA;
        DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	  end if;
	end if;
  end if;

  $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED $THEN
  V_SQL_FRAGMENT := 'select JSON_OBJECT(''metadata'' value (select JSON_OBJECTAGG(TABLE_NAME,TREAT(TABLE_METADATA as JSON) returning CLOB) from TABLE(:1)), ''data'' value JSON_OBJECT (' || C_NEWLINE;
  $ELSE
  V_SQL_FRAGMENT := 'select JSON_OBJECT(''metadata'' value (select JSON_OBJECTAGG(TABLE_NAME,JSON_QUERY(TABLE_METADATA, ''$'' RETURNING CLOB) returning CLOB) from TABLE(:1)), ''data'' value JSON_OBJECT (' || C_NEWLINE;
  $END

  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  for i in 1 .. EXPORT_METADATA_TABLE.count loop
	V_SQL_FRAGMENT := C_SINGLE_QUOTE || EXPORT_METADATA_TABLE(i).TABLE_NAME || C_SINGLE_QUOTE || ' value ( select JSON_ARRAYAGG(JSON_ARRAY(';
    if (i > 1) then
      V_SQL_FRAGMENT := ',' || V_SQL_FRAGMENT;
	end if;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	DBMS_LOB.APPEND(SQL_STATEMENT,EXPORT_METADATA_TABLE(i).EXPORT_SELECT_LIST);
    V_SQL_FRAGMENT := ' NULL ON NULL returning CLOB) returning CLOB) FROM "' || EXPORT_METADATA_TABLE(i).OWNER || '"."' || EXPORT_METADATA_TABLE(i).TABLE_NAME || '")' || C_NEWLINE;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  end loop;
  V_SQL_FRAGMENT := '             returning CLOB' || C_NEWLINE
                 || '           )' || C_NEWLINE;
                 || '         returning CLOB' || C_NEWLINE
                 || '       )' || C_NEWLINE
                 || '  from DUAL';
  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  $END

end;
--
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
/*
**
** Generate a select statement that leverages the returning CLOB feature to retrieve all the data in a single operation
**
** Release 18.1: JSON_ARRAYAGG with CLOB and TREAT AS JSON are supported
** Release 12.2 with Patch: JSON_ARRAYAGG with CLOB is supported. TREAT AS JSON is not supported
**
*/
function PROCESS_TABLE_LIST(P_SCHEMA VARCHAR2, P_TABLE_NAME VARCHAR2 DEFAULT NULL)
return CLOB
as
  V_CURRENT_SCHEMA           CONSTANT VARCHAR2(128) := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');
  TYPE T_CURSOR is REF CURSOR;
  V_CURSOR      T_CURSOR;
  V_EXPORT_CONTENTS      CLOB;

begin
  SET_CURRENT_SCHEMA(P_SCHEMA);
  GENERATE_EXPORT_METADATA(P_SCHEMA,P_TABLE_NAME);

  OPEN V_CURSOR FOR SQL_STATEMENT using EXPORT_METADATA_TABLE;
  FETCH V_CURSOR INTO V_EXPORT_CONTENTS;
  CLOSE V_CURSOR;

  SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
  return V_EXPORT_CONTENTS;
exception
  when OTHERS then
    SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
    RAISE;
end;
--
$ELSE
/*
**
** Iterate over the selected tables generating a JSON ARRAY from each row in each table.
** Concatenate the results into a single JSON document using PL/SQL.
**
** Release 12.2 without CLOB Support Patch. Using the native JSON_ARRAYAGG operator with a return type of VARCHAR2 is not a realistic option
**
*/
procedure APPEND_METADATA_OBJECT(P_EXPORT_CONTENTS IN OUT NOCOPY CLOB)
as
  V_JSON_FRAGMENT VARCHAR2(4000);
begin
  V_JSON_FRAGMENT := '"metadata":{';
  DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);
  for i in 1 .. EXPORT_METADATA_TABLE.count loop
    V_JSON_FRAGMENT := '"' || EXPORT_METADATA_TABLE(i).table_name || '":';
	if (i > 1) then
  	  V_JSON_FRAGMENT := ',' || V_JSON_FRAGMENT;
	end if;
    DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);
	DBMS_LOB.APPEND(P_EXPORT_CONTENTS,EXPORT_METADATA_TABLE(i).TABLE_METADATA);
  end loop;
  DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,1,'}');
end;
--
FUNCTION GET_TABLE_CURSOR(P_SQL_STATEMENT CLOB)
return SYS_REFCURSOR
as
  V_CURSOR_ID NUMBER;
  V_DUMMY  PLS_INTEGER;
  V_CURSOR SYS_REFCURSOR;
begin
  V_CURSOR_ID := DBMS_SQL.OPEN_CURSOR;
  DBMS_SQL.PARSE(V_CURSOR_ID,P_SQL_STATEMENT,DBMS_SQL.NATIVE);
  V_DUMMY := DBMS_SQL.EXECUTE(V_CURSOR_ID);
  return DBMS_SQL.TO_REFCURSOR(V_CURSOR_ID);
end;
--
function EXPORT_TABLE_DATA(P_CURSOR SYS_REFCURSOR)
return T_JSON_ARRAY_OUTPUT_TAB
pipelined
/*
**
** Return each row in the table as a JSON ARRAY
**
*/
is
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  V_JSON_ARRAY CLOB;
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  V_JSON_ARRAY VARCHAR2(32767);
  $ELSE
  V_JSON_ARRAY VARCHAR2(4000);
  $END
begin
  loop
    fetch P_CURSOR into V_JSON_ARRAY;
	exit when P_CURSOR%notfound;
    pipe row (V_JSON_ARRAY);
  end loop;
end;
--
procedure APPEND_DATA_OBJECT(P_EXPORT_CONTENTS IN OUT NOCOPY CLOB)
as
  V_JSON_FRAGMENT VARCHAR2(4000);
  V_FIRST_ITEM    BOOLEAN := TRUE;
  V_CURSOR        SYS_REFCURSOR;

  cursor exportTableData(C_CURSOR SYS_REFCURSOR)
  is
  select COLUMN_VALUE
    from TABLE(&CURRENT_SCHEMA..JSON_EXPORT.EXPORT_TABLE_DATA(C_CURSOR));
begin
  V_JSON_FRAGMENT := '"data":{';
  DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);
  for i in 1 .. EXPORT_METADATA_TABLE.count loop
    V_JSON_FRAGMENT := '"' || EXPORT_METADATA_TABLE(i).table_name || '":[';
	if (i > 1) then
  	  V_JSON_FRAGMENT := ',' || V_JSON_FRAGMENT;
	end if;
	DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);
    V_FIRST_ITEM := TRUE;
	V_CURSOR := GET_TABLE_CURSOR(EXPORT_METADATA_TABLE(i).SQL_STATEMENT);
    for r in exportTableData(V_CURSOR) loop
	  if (NOT V_FIRST_ITEM) then
    	DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,1,',');
	  end if;
 	  V_FIRST_ITEM := FALSE;
      DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,LENGTH(r.COLUMN_VALUE),r.COLUMN_VALUE);
	end loop;
	CLOSE V_CURSOR;
    DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,1,']');
  end loop;
  DBMS_LOB.WRITEAPPEND(P_EXPORT_CONTENTS,1,'}');
end;
--
function PROCESS_TABLE_LIST(P_SCHEMA VARCHAR2, P_TABLE_NAME VARCHAR2 DEFAULT NULL)
return CLOB
as
  V_CURRENT_SCHEMA       CONSTANT VARCHAR2(128) := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');
  V_EXPORT_CONTENTS      CLOB;
begin
  SET_CURRENT_SCHEMA(P_SCHEMA);
  GENERATE_EXPORT_METADATA(P_SCHEMA,P_TABLE_NAME);
  DBMS_LOB.CREATETEMPORARY(V_EXPORT_CONTENTS,TRUE,DBMS_LOB.CALL);
  -- Start outer JSON Object
  DBMS_LOB.WRITEAPPEND(V_EXPORT_CONTENTS,1,'{');
  -- Add metadata object
  APPEND_METADATA_OBJECT(V_EXPORT_CONTENTS);
  DBMS_LOB.WRITEAPPEND(V_EXPORT_CONTENTS,1,',');
  -- Add data object
  APPEND_DATA_OBJECT(V_EXPORT_CONTENTS);
  -- Close outer JSON Object
  DBMS_LOB.WRITEAPPEND(V_EXPORT_CONTENTS,1,'}');
  SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
  return V_EXPORT_CONTENTS;
exception
  when OTHERS then
    SET_CURRENT_SCHEMA(V_CURRENT_SCHEMA);
    RAISE;
end;
--
$END
--
function EXPORT_TABLE(P_SCHEMA VARCHAR2, P_TABLE_NAME VARCHAR2)
return T_CLOB_TAB
pipelined
as
--
-- Exports a speciic table in the specified schema
--
begin
  pipe row (PROCESS_TABLE_LIST(P_SCHEMA, P_TABLE_NAME));
end;
--
function EXPORT_SCHEMA(P_SCHEMA VARCHAR2)
return T_CLOB_TAB
pipelined
as
--
-- Exports all tables in the specified schema
--
begin
  pipe row (PROCESS_TABLE_LIST(P_SCHEMA));
end;
--
end;
/
show errors
--