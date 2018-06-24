--
create or replace type T_VC4000_TABLE is TABLE of VARCHAR2(4000)
/
--  
/*
** Add support for Object Tables 
*/			
create or replace package JSON_EXPORT
authid CURRENT_USER
as
  ROW_LIMIT NUMBER := -1;
  procedure SET_ROW_LIMIT(P_ROW_LIMIT NUMBER);
--  
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
  SQL_STATEMENT CLOB;
  function DUMP_SQL_STATEMENT return CLOB;
--
$ELSE
--
  type T_EXPORT_METADATA_RECORD is record(
    OWNER         VARCHAR2(128)
   ,TABLE_NAME    VARCHAR2(128)
   ,SQL_STATEMENT CLOB
  );
  
  type T_EXPORT_METADATA_TABLE is table of T_EXPORT_METADATA_RECORD;
  EXPORT_METADATA_CACHE T_EXPORT_METADATA_TABLE;
--  
  function EXPORT_METADATA return T_EXPORT_METADATA_TABLE pipelined;
$END  
--
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
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
function DUMP_SQL_STATEMENT
return CLOB
as
begin
  return SQL_STATEMENT;
end;
--
$ELSE
--
function EXPORT_METADATA
return T_EXPORT_METADATA_TABLE
pipelined
as
  cursor getRecords
  is
  select *
    from TABLE(EXPORT_METADATA_CACHE);
begin
  for r in getRecords loop
    pipe row (r);
  end loop;
end;
--
$END
--
procedure SET_ROW_LIMIT(P_ROW_LIMIT NUMBER)
as
begin
  ROW_LIMIT := P_ROW_LIMIT;
end;
--
function TABLE_TO_LIST(P_TABLE T_VC4000_TABLE,P_DELIMITER VARCHAR2 DEFAULT ',') 
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
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
procedure GENERATE_WITH_CLAUSE(P_SOURCE_SCHEMA VARCHAR2, P_TABLE_NAME_LIST T_VC4000_TABLE, P_BFILE_COUNT NUMBER, P_BLOB_COUNT NUMBER, P_ANYDATA_COUNT NUMBER, P_SQL_STATEMENT IN OUT CLOB)
as
  V_OBJECT_SERIALIZER CLOB;
begin
  V_OBJECT_SERIALIZER := OBJECT_SERIALIZATION.SERIALIZE_TABLE_TYPES(P_SOURCE_SCHEMA,P_TABLE_NAME_LIST);
$ELSE
procedure GENERATE_WITH_CLAUSE(P_SOURCE_SCHEMA VARCHAR2, P_TABLE_NAME VARCHAR2, P_BFILE_COUNT NUMBER, P_BLOB_COUNT NUMBER, P_ANYDATA_COUNT NUMBER, P_SQL_STATEMENT IN OUT CLOB)
as
  V_OBJECT_SERIALIZER CLOB;
begin
  V_OBJECT_SERIALIZER :=  OBJECT_SERIALIZATION.SERIALIZE_TABLE_TYPES(P_SOURCE_SCHEMA,P_TABLE_NAME);	
$END
  if ((P_BFILE_COUNT + P_BLOB_COUNT + P_ANYDATA_COUNT = 0) AND (V_OBJECT_SERIALIZER is NULL)) then
    return;
  end if;

  DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB('WITH' || C_NEWLINE));

  if (V_OBJECT_SERIALIZER is not null) then
    DBMS_LOB.APPEND(P_SQL_STATEMENT,V_OBJECT_SERIALIZER);
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
  	    ,sum(case when DATA_TYPE = 'BLOB'  then 1 else 0 end) BLOB_COUNT
  	    ,sum(case when DATA_TYPE = 'BFILE' then 1 else 0 end) BFILE_COUNT
        ,sum(case when DATA_TYPE = 'ANYDATA' then 1 else 0 end) ANYDATA_COUNT
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
			       then 'case when "' || COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(ANYDATA.convertCollection("' || COLUMN_NAME || '")) end'
				 when TYPECODE = 'OBJECT'
  				   then 'case when "' || COLUMN_NAME || '" is NULL then NULL else SERIALIZE_OBJECT(ANYDATA.convertObject("' || COLUMN_NAME || '")) end'
				 /*   	
				 ** Comment out unsupported scalar data types and Object types
				 */
			   	 when DATA_TYPE in ('LONG','LONG RAW')
			       then '''"' || COLUMN_NAME || '". Unsupported data type ["' || DATA_TYPE || '"]'''
			     else
   			       '"' || COLUMN_NAME || '"'
		       end
		order by INTERNAL_COLUMN_ID) as T_VC4000_TABLE) EXPORT_SELECT_LIST
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
	 and (
	       ((TABLE_TYPE is NULL) and (HIDDEN_COLUMN = 'NO'))
		 or 
		   ((TABLE_TYPE is not NULL) and (COLUMN_NAME in ('SYS_NC_ROWINFO$','SYS_NC_OID$','ACLOID','OWNERID')))
         )		  
	 and aat.OWNER = P_SOURCE_SCHEMA
   group by aat.OWNER, aat.TABLE_NAME;
   
  V_FIRST_ROW BOOLEAN := TRUE;
begin
--
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
  DBMS_LOB.CREATETEMPORARY(SQL_STATEMENT,TRUE,DBMS_LOB.CALL);

  declare
    V_BFILE_COUNT     NUMBER := 0;
    V_BLOB_COUNT      NUMBER := 0;
	V_ANYDATA_COUNT   NUMBER := 0;
    V_TABLE_NAME_LIST T_VC4000_TABLE := T_VC4000_TABLE();
   begin 
    for t in getTableMetadata loop  
   	  V_BFILE_COUNT    := V_BFILE_COUNT   + t.BFILE_COUNT;
	  V_BLOB_COUNT     := V_BLOB_COUNT    + t.BLOB_COUNT;
	  V_ANYDATA_COUNT  := V_ANYDATA_COUNT + t.ANYDATA_COUNT;
	  V_TABLE_NAME_LIST.extend();
	  V_TABLE_NAME_LIST(V_TABLE_NAME_LIST.count) := t.TABLE_NAME;
    end loop;
    GENERATE_WITH_CLAUSE(P_SOURCE_SCHEMA, V_TABLE_NAME_LIST, V_BFILE_COUNT, V_BLOB_COUNT, V_ANYDATA_COUNT, SQL_STATEMENT);
  end; 
    
  V_SQL_FRAGMENT := 'select JSON_OBJECT(''data'' value JSON_OBJECT (' || C_NEWLINE;
  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

  for t in getTableMetadata loop  
	V_SQL_FRAGMENT := C_SINGLE_QUOTE || t.TABLE_NAME || C_SINGLE_QUOTE || ' value ( select JSON_ARRAYAGG(JSON_ARRAY(';
    if (not V_FIRST_ROW) then
      V_SQL_FRAGMENT := ',' || V_SQL_FRAGMENT;
	end if;
	V_FIRST_ROW := FALSE;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	DBMS_LOB.APPEND(SQL_STATEMENT,TABLE_TO_LIST(t.EXPORT_SELECT_LIST));
    V_SQL_FRAGMENT := ' NULL on NULL returning ' || C_RETURN_TYPE || ') returning ' || C_RETURN_TYPE || ') from "' || t.OWNER || '"."' || t.TABLE_NAME || '"';
	if (ROW_LIMIT > -1) then
	  V_SQL_FRAGMENT := V_SQL_FRAGMENT || 'where ROWNUM < ' || ROW_LIMIT;
	end if;
	V_SQL_FRAGMENT :=  V_SQL_FRAGMENT || ')' || C_NEWLINE;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  end loop;

  V_SQL_FRAGMENT := '             returning ' || C_RETURN_TYPE || C_NEWLINE
                 || '           )' || C_NEWLINE
                 || '         returning ' || C_RETURN_TYPE || C_NEWLINE
                 || '       )' || C_NEWLINE
                 || '  from DUAL';
  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
-- 
$ELSE
--
  declare
    V_SQL_STATEMENT CLOB;
  begin
    EXPORT_METADATA_CACHE := T_EXPORT_METADATA_TABLE();
    for t in getTableMetadata loop  
      EXPORT_METADATA_CACHE.extend();
	  EXPORT_METADATA_CACHE(EXPORT_METADATA_CACHE.count).OWNER := t.OWNER;
	  EXPORT_METADATA_CACHE(EXPORT_METADATA_CACHE.count).TABLE_NAME := t.TABLE_NAME;

      DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
      GENERATE_WITH_CLAUSE(P_SOURCE_SCHEMA, t.TABLE_NAME, t.BFILE_COUNT, t.BLOB_COUNT, t.ANYDATA_COUNT, V_SQL_STATEMENT); 
      V_SQL_FRAGMENT := 'select JSON_ARRAY(';
      DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
      DBMS_LOB.APPEND(V_SQL_STATEMENT,TABLE_TO_LIST(t.EXPORT_SELECT_LIST));
      V_SQL_FRAGMENT := ' NULL on NULL returning '|| C_RETURN_TYPE 
	                  --  ### if only || ' DEFAULT ''["ROW[['' || TO_CHAR(ROWID) || '']: Maximum return size (' || C_MAX_OUTPUT_SIZE || ') for JSON_ARRAY operator exceeded."]'' ON ERROR 
					  || ') from "' || t.OWNER || '"."' || t.TABLE_NAME || '"';
  	  if (ROW_LIMIT > -1) then
	    V_SQL_FRAGMENT := V_SQL_FRAGMENT || 'where ROWNUM < ' || ROW_LIMIT;
	  end if;
      DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
      EXPORT_METADATA_CACHE(EXPORT_METADATA_CACHE.count).SQL_STATEMENT := V_SQL_STATEMENT;	
    end loop;
  end;	
$END
--
end;
--
$IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
function EXPORT_SCHEMA(P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')))
return CLOB
as
  V_JSON_DOCUMENT CLOB;
  V_CURSOR      SYS_REFCURSOR;
begin
  GENERATE_STATEMENT(P_SOURCE_SCHEMA);
  open V_CURSOR for SQL_STATEMENT;
  fetch V_CURSOR into V_JSON_DOCUMENT;
  close V_CURSOR;
  return V_JSON_DOCUMENT;
end;
--   
$ELSE
--
function EXPORT_SCHEMA(P_SOURCE_SCHEMA VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'))
return CLOB
as
  V_JSON_DOCUMENT CLOB;
  V_CURSOR        SYS_REFCURSOR;

  V_JSON_FRAGMENT VARCHAR2(4000);
  
  V_FIRST_TABLE   BOOLEAN := TRUE;
  V_FIRST_ITEM    BOOLEAN := TRUE;
-- 
  $IF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  V_JSON_ARRAY VARCHAR2(32767);
  $ELSE
  V_JSON_ARRAY VARCHAR2(4000);
  $END  
--
begin
  GENERATE_STATEMENT(P_SOURCE_SCHEMA);									  
  DBMS_LOB.CREATETEMPORARY(V_JSON_DOCUMENT,TRUE,DBMS_LOB.CALL);
  V_JSON_FRAGMENT := '{"data":{';
  DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);
  for i in 1 .. EXPORT_METADATA_CACHE.count loop
    V_JSON_FRAGMENT := '"' || EXPORT_METADATA_CACHE(i).table_name || '":[';
	if (not V_FIRST_TABLE) then 
  	  V_JSON_FRAGMENT := ',' || V_JSON_FRAGMENT;
	end if;
	V_FIRST_TABLE := false;
	DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);													   
    V_FIRST_ITEM := TRUE;
    open V_CURSOR for EXPORT_METADATA_CACHE(i).SQL_STATEMENT;
	loop		  
	  fetch V_CURSOR into V_JSON_ARRAY;
	  exit when V_CURSOR%notfound;	  
	  if (not V_FIRST_ITEM) then
    	DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,',');
	  end if;
 	  V_FIRST_ITEM := FALSE;
      DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,length(V_JSON_ARRAY),V_JSON_ARRAY);
	end loop;
	close V_CURSOR;
    DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,']');
  end loop;
  DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,'}');
  DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,'}');
  return V_JSON_DOCUMENT;
end;
--
$END
--
end;
/
show errors
--
spool off
--