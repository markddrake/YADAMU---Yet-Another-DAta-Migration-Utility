create or replace package YADAMU_UTILITIES
authid CURRENT_USER
as
--
  JSON_OVERFLOW1 EXCEPTION; 
  PRAGMA EXCEPTION_INIT (JSON_OVERFLOW1, -40478);
  
  JSON_OVERFLOW2 EXCEPTION; 
  PRAGMA EXCEPTION_INIT (JSON_OVERFLOW2, -40459);

  JSON_OVERFLOW3 EXCEPTION; 
  PRAGMA EXCEPTION_INIT (JSON_OVERFLOW3, -46077);

  BUFFER_OVERFLOW EXCEPTION; 
  PRAGMA EXCEPTION_INIT (BUFFER_OVERFLOW, -22835);
    
  C_NULL    CONSTANT NUMBER(1) := 0;
  C_BOOLEAN CONSTANT NUMBER(1) := 1;
  C_NUMERIC CONSTANT NUMBER(1) := 2;
  C_STRING  CONSTANT NUMBER(1) := 3;
  C_CLOB    CONSTANT NUMBER(1) := 4;
  C_JSON    CONSTANT NUMBER(1) := 5;

  C_NEWLINE         CONSTANT CHAR(1) := CHR(10);
  C_CARRIAGE_RETURN CONSTANT CHAR(1) := CHR(13);
  C_SINGLE_QUOTE    CONSTANT CHAR(1) := CHR(39);

  function CLOBTOBLOB(P_CLOB IN CLOB, P_CHARSET VARCHAR2 DEFAULT 'AL32UTF8') return BLOB;

  TYPE KVP_RECORD is RECORD (
    KEY               VARCHAR2(4000)
   ,DATA_TYPE         NUMBER
   ,NUMERIC_VALUE     NUMBER
   -- ,BOOLEAN_VALUE     BOOLEAN
    $IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
   ,STRING_VALUE      VARCHAR2(32767)
    $ELSE
   ,STRING_VALUE      VARCHAR2(4000)
    $END
   ,CLOB_VALUE        CLOB
  );
  
  TYPE KVP_TABLE is TABLE of KVP_RECORD;
  TYPE JSON_ARRAY_TABLE is TABLE OF CLOB;
  TYPE KVP_TABLE_TABLE  is TABLE OF KVP_TABLE;
    
--
$IF NOT YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
  function KVNUL(KEY VARCHAR2) return KVP_RECORD;
  function KVB(KEY VARCHAR2,VALUE BOOLEAN) return KVP_RECORD;
  function KVN(KEY VARCHAR2,VALUE NUMBER) return KVP_RECORD;
  function KVS(KEY VARCHAR2,VALUE VARCHAR2) return KVP_RECORD;
  function KVC(KEY VARCHAR2,VALUE CLOB) return KVP_RECORD;  
  function KVJ(KEY VARCHAR2,VALUE CLOB) return KVP_RECORD;

  function JSON_ARRAY_CLOB(P_ARRAY_ENTRIES KVP_TABLE) return CLOB;
  function JSON_OBJECT_CLOB(P_KVP_LIST KVP_TABLE) return CLOB;
  function JSON_ARRAYAGG_CLOB(P_CURSOR SYS_REFCURSOR) return CLOB;
  function JSON_ARRAYAGG_CLOB(P_JSON_ARRAYS JSON_ARRAY_TABLE) return CLOB;  
  function JSON_ARRAYAGG_CLOB(P_ARRAY_ARRAY_ENTRIES KVP_TABLE_TABLE) return CLOB;
  function JSON_OBJECTAGG_CLOB(P_STATEMENT VARCHAR2) return CLOB;
  
--
$END

--
$IF DBMS_DB_VERSION.VER_LE_11_2 $THEN
$ELSIF DBMS_DB_VERSION.VER_LE_12_2 $THEN
  function JSON_KEYS(P_JSON CLOB) return CLOB;
  function JSON_KEYS(P_JSON BLOB) return CLOB;
  function ORDER_JSON_OBJECTS(P_UNORDERED_JSON CLOB) return CLOB;
  function ORDER_JSON_OBJECTS(P_UNORDERED_JSON BLOB) return BLOB;
$ELSIF DBMS_DB_VERSION.VER_LE_18 and YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--   
  function JSON_KEYS(P_JSON CLOB) return CLOB;
  function JSON_KEYS(P_JSON BLOB) return CLOB;
  function ORDER_JSON_OBJECTS(P_UNORDERED_JSON CLOB) return CLOB;
  function ORDER_JSON_OBJECTS(P_UNORDERED_JSON BLOB) return BLOB;
--
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
create or replace package body YADAMU_UTILITIES
as
--
function CLOBTOBLOB(P_CLOB IN CLOB, P_CHARSET VARCHAR2 DEFAULT 'AL32UTF8') 
return BLOB 
as
  V_BLOB           BLOB;
  V_WARNING        VARCHAR2(255);
  V_CHARSET_ID     NUMBER := NLS_CHARSET_ID(P_CHARSET);
  V_DEST_OFFSET    NUMBER := 1; 
  V_SRC_OFFSET     NUMBER := 1; 
  V_LANG_CONTEXT   NUMBER := 0; 
begin
   DBMS_LOB.CREATETEMPORARY(V_BLOB, TRUE );
   DBMS_LOB.CONVERTTOBLOB(V_BLOB, P_CLOB, DBMS_LOB.LOBMAXSIZE, V_DEST_OFFSET, V_SRC_OFFSET, V_CHARSET_ID, V_LANG_CONTEXT, V_WARNING);
   RETURN V_BLOB; 
end;
--
$IF NOT YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
function JSON_ESCAPE(V_CHUNK VARCHAR2) 
return VARCHAR2
as
begin
  return REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(V_CHUNK,'\','\\') ,'"','\"'),CHR(13),'\n'),CHR(10),'\r'),CHR(9),'\t');
end;
-- "
--
function KVNUL(KEY VARCHAR2) 
return KVP_RECORD
as
  KVP KVP_RECORD;
begin
  KVP.DATA_TYPE := C_NULL;
  KVP.KEY := KEY;
  return KVP;
end;
--
function KVB(KEY VARCHAR2, VALUE BOOLEAN) 
return KVP_RECORD
as
  KVP KVP_RECORD;
begin
  if (VALUE is NULL) then return KVNUL(KEY); end if;
  KVP.DATA_TYPE := C_BOOLEAN;
  KVP.KEY := KEY;
  -- KVP.BOOLEAN_VALUE := VALUE;
  KVP.NUMERIC_VALUE := CASE WHEN VALUE THEN 1 ELSE 0 END;
  return KVP;
end;
--
function KVN(KEY VARCHAR2, VALUE NUMBER) 
return KVP_RECORD
as
  KVP KVP_RECORD;
begin
  if (VALUE is NULL) then return KVNUL(KEY); end if;
  KVP.DATA_TYPE := C_NUMERIC;
  KVP.KEY := KEY;
  KVP.NUMERIC_VALUE := VALUE;
  return KVP;
end;
--
function KVJ(KEY VARCHAR2, VALUE CLOB) 
return KVP_RECORD
as
  KVP KVP_RECORD;
begin
  if (VALUE is NULL) then return KVNUL(KEY); end if;
  KVP.DATA_TYPE := C_JSON;
  KVP.KEY := KEY;
  KVP.CLOB_VALUE := VALUE;
  return KVP;
end;
--
function KVC(KEY VARCHAR2, VALUE CLOB) 
return KVP_RECORD
as
  V_VALUE_LENGTH PLS_INTEGER := DBMS_LOB.getLength(VALUE);
  KVP KVP_RECORD;

  V_CHUNK_START  NUMBER := 1;

  $IF YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED $THEN
  $IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  V_CHUNK           VARCHAR2(32767);
  V_CHUNK_ESCAPED   VARCHAR2(32767);
  $ELSE
  V_CHUNK           VARCHAR2(4000);
  V_CHUNK_ESCAPED   VARCHAR2(4000);
  $END
  V_CHUNK_SIZE      NUMBER := YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE-4;
  $ELSE
  V_CHUNK           VARCHAR2(8192);
  V_CHUNK_ESCAPED   VARCHAR2(32767);
  V_CHUNK_SIZE      NUMBER := 8192;
  $END
  
  
begin
  if (VALUE is NULL) then return KVNUL(KEY); end if;
  KVP.DATA_TYPE := C_CLOB;
  KVP.KEY := KEY;
  -- Use JSON_ARRAY to create properly escapaed value of string content
  -- Write the Array Content (which is correctly escaped JSON) into the buffer.
  -- Strip the [" and "] from the escaped value.
  DBMS_LOB.CREATETEMPORARY(KVP.CLOB_VALUE,TRUE,DBMS_LOB.SESSION);
  while (V_CHUNK_START < V_VALUE_LENGTH) loop
    DBMS_LOB.READ(VALUE,V_CHUNK_SIZE,V_CHUNK_START,V_CHUNK);
    $IF YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED $THEN
    begin
      --
      select JSON_ARRAY(V_CHUNK) 
        into V_CHUNK_ESCAPED
        from DUAL;
      DBMS_LOB.WRITEAPPEND(KVP.CLOB_VALUE,LENGTH(V_CHUNK_ESCAPED)-4,TRIM('"' FROM LTRIM(RTRIM(V_CHUNK_ESCAPED,']'),'[')));
      --
    exception
      when JSON_OVERFLOW1 or JSON_OVERFLOW2 then
        select JSON_ARRAY(SUBSTR(V_CHUNK,1,FLOOR(YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE/2))) 
          into V_CHUNK_ESCAPED
          from DUAL;
        DBMS_LOB.WRITEAPPEND(KVP.CLOB_VALUE,LENGTH(V_CHUNK_ESCAPED)-4,TRIM('"' FROM LTRIM(RTRIM(V_CHUNK_ESCAPED,']'),'[')));
        --
        if (LENGTH(V_CHUNK) > FLOOR(YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE/2)) then 
          select JSON_ARRAY(SUBSTR(V_CHUNK,CEIL(YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE/2))) 
               into V_CHUNK_ESCAPED
              from DUAL;
          DBMS_LOB.WRITEAPPEND(KVP.CLOB_VALUE,LENGTH(V_CHUNK_ESCAPED)-4,TRIM('"' FROM LTRIM(RTRIM(V_CHUNK_ESCAPED,']'),'[')));
        end if;
      when OTHERS then
        RAISE;
     --
    end;
    --
    V_CHUNK_START := V_CHUNK_START + V_CHUNK_SIZE;
    V_CHUNK_SIZE := YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE-4;
    -- 
    $ELSE
    --
    V_CHUNK_ESCAPED := JSON_ESCAPE(V_CHUNK);
    DBMS_LOB.WRITEAPPEND(KVP.CLOB_VALUE,LENGTH(V_CHUNK_ESCAPED),V_CHUNK_ESCAPED);
    V_CHUNK_START := V_CHUNK_START + V_CHUNK_SIZE;
    V_CHUNK_SIZE := 8192;
    --
    $END
    --
  end loop;  
  return KVP;
end;
--
function KVS(KEY VARCHAR2, VALUE VARCHAR2) 
return KVP_RECORD
as
  KVP KVP_RECORD;
begin
  if (VALUE is NULL) then return KVNUL(KEY); end if;
  KVP.DATA_TYPE := C_STRING;
  KVP.KEY := KEY;
  -- Use JSON_ARRAY to create properly escapaed value of string content
  -- Write the Array Content (which is correctly escaped JSON) into the buffer.
  -- Strip the [" and "] from the escaped value.

  --
  $IF YADAMU_FEATURE_DETECTION.JSON_GENERATION_SUPPORTED $THEN
  select TRIM('"' FROM LTRIM(RTRIM(JSON_ARRAY(VALUE),']'),'['))
    into KVP.STRING_VALUE
    from DUAL;
  $ELSE
  KVP.STRING_VALUE := JSON_ESCAPE(VALUE);
  $END
  --
  return KVP;

exception
  when JSON_OVERFLOW1 or JSON_OVERFLOW2 then
    return KVC(KEY, VALUE);
  when OTHERS then
    RAISE;
end;
--
procedure WRITE_VALUE(P_JSON_DOCUMENT IN OUT CLOB,P_KVP KVP_RECORD)
as
begin
  case 
    when P_KVP.DATA_TYPE = C_NULL then
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length('null'),'null');  
    when P_KVP.DATA_TYPE = C_BOOLEAN and not P_KVP.NUMERIC_VALUE = 0 then
    -- when P_KVP.DATA_TYPE = C_BOOLEAN and P_KVP.BOOLEAN_VALUE then
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length('true'),'true');  
    when P_KVP.DATA_TYPE = C_BOOLEAN and P_KVP.NUMERIC_VALUE = 0 then
    -- when P_KVP.DATA_TYPE = C_BOOLEAN and not P_KVP.BOOLEAN_VALUE then
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length('false'),'false');  
    when P_KVP.DATA_TYPE = C_NUMERIC then
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length(TO_CHAR(P_KVP.NUMERIC_VALUE)),TO_CHAR(P_KVP.NUMERIC_VALUE));
    when P_KVP.DATA_TYPE = C_JSON then
      DBMS_LOB.APPEND(P_JSON_DOCUMENT,P_KVP.CLOB_VALUE);
    when P_KVP.DATA_TYPE = C_STRING then
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'"');  
      DBMS_LOB.APPEND(P_JSON_DOCUMENT,P_KVP.STRING_VALUE);
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'"');  
    when P_KVP.DATA_TYPE = C_CLOB then
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'"');  
      DBMS_LOB.APPEND(P_JSON_DOCUMENT,P_KVP.CLOB_VALUE);
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'"');  
  end case;
end;
--
procedure JSON_OBJECT_CLOB(P_JSON_DOCUMENT IN OUT CLOB, P_KVP_LIST KVP_TABLE)
as
begin
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'{');  
  if (P_KVP_LIST.count > 0) then
    for i in P_KVP_LIST.FIRST .. P_KVP_LIST.LAST loop
      if (i > 1) then DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,','); end if;  
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'"');  
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length(P_KVP_LIST(i).KEY),P_KVP_LIST(i).KEY);  
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'"');  
      DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,':');  
      WRITE_VALUE(P_JSON_DOCUMENT,P_KVP_LIST(i));
    end loop;
  end if;
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'}');  
end;
--
function  JSON_OBJECT_CLOB(P_KVP_LIST KVP_TABLE)
return CLOB
as
  V_RESULT CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_RESULT,TRUE,DBMS_LOB.SESSION);
  JSON_OBJECT_CLOB(V_RESULT,P_KVP_LIST);
  return V_RESULT;
end;
--
procedure JSON_ARRAY_CLOB(P_JSON_DOCUMENT IN OUT CLOB, P_ARRAY_ENTRIES KVP_TABLE)
as
begin
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'[');  
  if (P_ARRAY_ENTRIES.count > 0) then
    for i in P_ARRAY_ENTRIES.FIRST .. P_ARRAY_ENTRIES.LAST loop
      if (i > 1) then DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,','); end if;  
      WRITE_VALUE(P_JSON_DOCUMENT,P_ARRAY_ENTRIES(i));
    end loop;
  end if;
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,']');  
end;
--
function  JSON_ARRAY_CLOB(P_ARRAY_ENTRIES KVP_TABLE)
return CLOB
as
  V_RESULT CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_RESULT,TRUE,DBMS_LOB.SESSION);
  JSON_ARRAY_CLOB(V_RESULT,P_ARRAY_ENTRIES);
  return V_RESULT;
end;
--
procedure JSON_ARRAYAGG_CLOB(P_JSON_DOCUMENT IN OUT CLOB, P_ARRAY_ARRAY_ENTRIES KVP_TABLE_TABLE)
as
begin
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'[');  
  if (P_ARRAY_ARRAY_ENTRIES.count > 0) then
    for i in P_ARRAY_ARRAY_ENTRIES.FIRST .. P_ARRAY_ARRAY_ENTRIES.LAST loop
      if (i > 1) then DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,','); end if;  
      JSON_ARRAY_CLOB(P_JSON_DOCUMENT,P_ARRAY_ARRAY_ENTRIES(i));
    end loop;
  end if;
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,']');  
end;
--
function  JSON_ARRAYAGG_CLOB(P_ARRAY_ARRAY_ENTRIES KVP_TABLE_TABLE)
return CLOB
as
  V_RESULT CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_RESULT,TRUE,DBMS_LOB.SESSION);
  JSON_ARRAYAGG_CLOB(V_RESULT,P_ARRAY_ARRAY_ENTRIES);
  return V_RESULT;
end;
--
procedure JSON_ARRAYAGG_CLOB(P_JSON_DOCUMENT IN OUT CLOB, P_JSON_ARRAYS JSON_ARRAY_TABLE)
as
begin
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'[');  
  if (P_JSON_ARRAYS.count > 0) then
    for i in P_JSON_ARRAYS.FIRST .. P_JSON_ARRAYS.LAST loop
      if (i > 1) then DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,','); end if;  
      DBMS_LOB.APPEND(P_JSON_DOCUMENT,P_JSON_ARRAYS(i));
    end loop;
  end if;
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,']');  
end;
--
function JSON_ARRAYAGG_CLOB(P_JSON_ARRAYS JSON_ARRAY_TABLE)
return CLOB
as
  V_RESULT CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_RESULT,TRUE,DBMS_LOB.SESSION);
  JSON_ARRAYAGG_CLOB(V_RESULT,P_JSON_ARRAYS);
  return V_RESULT;
end;
--
procedure JSON_ARRAYAGG_CLOB(P_JSON_DOCUMENT IN OUT CLOB, P_CURSOR SYS_REFCURSOR)
as
  V_SEPERATOR         VARCHAR2(1) := ',';
  V_ARRAY_MEMBER      CLOB;
  V_FIRST_MEMBER      BOOLEAN := true;
begin
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,'[');  
  loop
    fetch P_CURSOR into V_ARRAY_MEMBER;
    exit when P_CURSOR%notfound;
    if (NOT V_FIRST_MEMBER) then DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,length(V_SEPERATOR),V_SEPERATOR); end if;
    V_FIRST_MEMBER := false;
    DBMS_LOB.APPEND(P_JSON_DOCUMENT,V_ARRAY_MEMBER);
  end loop;
  DBMS_LOB.WRITEAPPEND(P_JSON_DOCUMENT,1,']');  
end;
--
function JSON_ARRAYAGG_CLOB(P_CURSOR SYS_REFCURSOR) 
return CLOB
as
  V_RESULT CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_RESULT,TRUE,DBMS_LOB.SESSION);
  JSON_ARRAYAGG_CLOB(V_RESULT,P_CURSOR);
  return V_RESULT;
end;
--
function JSON_OBJECTAGG_CLOB(P_STATEMENT VARCHAR2)
return CLOB
as
  V_KVP_TABLE KVP_TABLE;
begin
--  select PARAMETER "KEY", C_STRING DATA_TYPE, NULL "NUMERIC_VALUE", VALUE "STRING_VALUE", NULL "CLOB_VALUE"
--    bulk collect into V_KVP_TABLE
--    from NLS_DATABASE_PARAMETERS;
  execute immediate P_STATEMENT bulk collect into V_KVP_TABLE;
  return JSON_OBJECT_CLOB(V_KVP_TABLE);
end;
--
$END
--
$IF DBMS_DB_VERSION.VER_LE_11_2 $THEN
$ELSIF DBMS_DB_VERSION.VER_LE_12_2 $THEN
--
function JSON_KEYS(P_JSON CLOB) 
return CLOB 
as
  V_OBJECT    JSON_OBJECT_T;
  V_ARRAY     JSON_ARRAY_T;
  V_KEYS      JSON_KEY_LIST;
BEGIN
  V_ARRAY  := new JSON_ARRAY_T;
  V_OBJECT := JSON_OBJECT_T.parse(P_JSON);
  V_KEYS   := V_OBJECT.get_keys();
  FOR i IN 1..V_KEYS.COUNT LOOP
    V_ARRAY.append(V_KEYS(i));
  END LOOP;
  return V_ARRAY.to_string();
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(P_JSON);
	return '[]';
END;
--
function JSON_KEYS(P_JSON BLOB) 
return CLOB 
as
  V_OBJECT    JSON_OBJECT_T;
  V_ARRAY     JSON_ARRAY_T;
  V_KEYS      JSON_KEY_LIST;
BEGIN
  V_ARRAY  := new JSON_ARRAY_T;
  V_OBJECT := JSON_OBJECT_T.parse(P_JSON);
  V_KEYS   := V_OBJECT.get_keys();
  FOR i IN 1..V_KEYS.COUNT LOOP
    V_ARRAY.append(V_KEYS(i));
  END LOOP;
  return V_ARRAY.to_string();
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(JSON_QUERY(P_JSON,'$' RETURNING CLOB));
    return '[]';
END;
--
procedure ORDER_JSON_OBJECTS(P_UNORDERED_JSON CLOB,P_ORDERED_JSON IN OUT CLOB)
as
begin
    
   /*
   **
   ** CLOB FORMAT JSON will be NULL if the Value is not VALID JSON. Naked Strings and Numbers are not valid for FORMAT JSON
   ** BINARY_DOUBLE will return NULL if the value is a not a valid JSON Number
   ** SV will return null if a string value is too long 
   ** CV will catch eveything else but NULL
   **
   */
   
   if (SUBSTR(P_UNORDERED_JSON,1,1) = '[') then
     DBMS_OUTPUT.PUT_LINE('Array');
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,'[');  
     for O in (
	   select v.I
	         ,v.JV
	         ,case 
	             when v.SV in ('true','false') and v.NV in (1,0) then 
	  			  JSON_ARRAY(v.SV format JSON returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))				
	            when v.NV is not NULL then
	  			  JSON_ARRAY(v.NV returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))
	  		    when v.SV is not NULL then 
 	  		      JSON_ARRAY(v.SV returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))
	  		    else 
	  			  JSON_ARRAY(NULL format JSON NULL ON NULL returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))				
	  	      end JSON
         from JSON_TABLE(P_UNORDERED_JSON,'$[*]' 
	  	        COLUMNS 
	  		      I                                                                                                               FOR ORDINALITY
	  		    , NV    NUMBER                                                                                                    PATH '$' 
	  		    , SV    VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END)              PATH '$' 
	  		    , JV    VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END)  FORMAT JSON PATH '$' 
	          ) V
	    order by v.I 
	  ) loop
	    if (o.i > 1) then
          DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,',');  
		end if;
        if (o.JV is not NULL) then
     	  ORDER_JSON_OBJECTS(o.JV,P_ORDERED_JSON);
        else 
		  DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,LENGTH(o.JSON)-2,SUBSTR(o.JSON,2,LENGTH(O.json)-1));
		end if;
      end loop;
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,']');   
   else
     DBMS_OUTPUT.PUT_LINE('Object');
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,'{');  
     for O in (
	   with ORDERED_KEYS as (
	     select K
		       ,NV
			   ,SV
			   ,JV
           from JSON_TABLE(YADAMU_UTILITIES.JSON_KEYS(P_UNORDERED_JSON),'$[*]' COLUMNS I FOR ORDINALITY, K VARCHAR2(256) PATH '$') K,
                JSON_TABLE(P_UNORDERED_JSON,'$.*' 
	  	        COLUMNS 
	  		      I                                                                                                              FOR ORDINALITY
	  		    , NV   NUMBER                                                                                                    PATH '$' 
  	  		    , SV   VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END)              PATH '$' 
	  		    , JV   VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END) FORMAT JSON  PATH '$' 
	            ) V
          where k.I = v.I
	      order by k.K 
	   ) 
       select case when (ROWNUM = 1) then '"' else ',"' end || K || '":' K
	         ,JV
	         ,case 
	            when SV in ('true','false') and NV in (1,0) then 
	  			  JSON_ARRAY(SV format JSON returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))				
	            when NV is not NULL then
	  			  JSON_ARRAY(NV returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))
	  		    when SV is not NULL then 
 	  		      JSON_ARRAY(SV returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))
	  		    else 
	  			  JSON_ARRAY(NULL format JSON NULL ON NULL returning VARCHAR2($IF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN 32767 $ELSE 4000 $END))				
	  	      end JSON
		 from ORDERED_KEYS
      ) loop
	    DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,length(o.K),o.K);  
		DBMS_OUTPUT.PUT_LINE('KEY' || o.K);
        if (o.JV is not NULL) then
     	  ORDER_JSON_OBJECTS(o.JV,P_ORDERED_JSON);
        else 
		  DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,LENGTH(o.JSON)-2,SUBSTR(o.JSON,2,LENGTH(O.json)-1));
		end if;
     end loop;
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,'}');       
   end if;
end;
--
function ORDER_JSON_OBJECTS(P_UNORDERED_JSON CLOB)
return CLOB
as
  V_ORDERED_JSON    CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_ORDERED_JSON, TRUE );
  ORDER_JSON_OBJECTS(P_UNORDERED_JSON,V_ORDERED_JSON);
  return V_ORDERED_JSON;
end;
--
function ORDER_JSON_OBJECTS(P_UNORDERED_JSON BLOB)
return BLOB
as
  V_ORDERED_JSON    CLOB;
  V_UNORDERED_CLOB  CLOB;
  V_ORDERED_BLOB    BLOB;

  V_SRC_OFFSET      PLS_INTEGER := 1;
  V_DEST_OFFSET     PLS_INTEGER := 1;

  V_LANG_CONTEXT    PLS_INTEGER := 0;
  V_WARNING         PLS_INTEGER;
  
begin
  DBMS_LOB.CREATETEMPORARY(V_UNORDERED_CLOB, TRUE);
  DBMS_LOB.CREATETEMPORARY(V_ORDERED_JSON, TRUE);
  DBMS_LOB.CONVERTTOCLOB(
    V_UNORDERED_CLOB,
    P_UNORDERED_JSON, 
    DBMS_LOB.GETLENGTH(P_UNORDERED_JSON),
    V_SRC_OFFSET,
    V_DEST_OFFSET,
    NLS_CHARSET_ID('AL32UTF8'),
    V_LANG_CONTEXT, 
    V_WARNING 
  );
  
  DBMS_OUTPUT.PUT_line(DBMS_LOB.GETLENGTH(P_UNORDERED_JSON) || ',' || DBMS_LOB.GETLENGTH(V_UNORDERED_CLOB));
  
  ORDER_JSON_OBJECTS(V_UNORDERED_CLOB,V_ORDERED_JSON);
  
  V_SRC_OFFSET := 1;
  V_DEST_OFFSET := 1;
  
  DBMS_LOB.CREATETEMPORARY(V_ORDERED_BLOB,TRUE);
  DBMS_LOB.CONVERTTOBLOB(
    V_ORDERED_BLOB,
    V_ORDERED_JSON, 
    DBMS_LOB.GETLENGTH(V_ORDERED_JSON),
    V_SRC_OFFSET,
    V_DEST_OFFSET,
    NLS_CHARSET_ID('AL32UTF8'),
    V_LANG_CONTEXT,
    V_WARNING 
  );
  DBMS_LOB.FREETEMPORARY(V_ORDERED_JSON);
  return V_ORDERED_BLOB;
end;
--
$ELSIF DBMS_DB_VERSION.VER_LE_18 and YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
--
function JSON_KEYS(P_JSON CLOB) 
return CLOB 
as
  V_OBJECT    JSON_OBJECT_T;
  V_ARRAY     JSON_ARRAY_T;
  V_KEYS      JSON_KEY_LIST;
BEGIN
  V_ARRAY  := new JSON_ARRAY_T;
  V_OBJECT := JSON_OBJECT_T.parse(P_JSON);
  V_KEYS   := V_OBJECT.get_keys();
  FOR i IN 1..V_KEYS.COUNT LOOP
    V_ARRAY.append(V_KEYS(i));
  END LOOP;
  return V_ARRAY.to_string();
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(P_JSON);
	return '[]';
END;
--
function JSON_KEYS(P_JSON BLOB) 
return CLOB 
as
  V_OBJECT    JSON_OBJECT_T;
  V_ARRAY     JSON_ARRAY_T;
  V_KEYS      JSON_KEY_LIST;
BEGIN
  V_ARRAY  := new JSON_ARRAY_T;
  V_OBJECT := JSON_OBJECT_T.parse(P_JSON);
  V_KEYS   := V_OBJECT.get_keys();
  FOR i IN 1..V_KEYS.COUNT LOOP
    V_ARRAY.append(V_KEYS(i));
  END LOOP;
  return V_ARRAY.to_string();
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(JSON_QUERY(P_JSON,'$' RETURNING CLOB));
    return '[]';
END;
--
procedure ORDER_JSON_OBJECTS(P_UNORDERED_JSON CLOB,P_ORDERED_JSON IN OUT CLOB)
as
  V_ESCAPED_VALUE CLOB;
begin
    
   /*
   **
   ** CLOB FORMAT JSON will be NULL if the Value is not VALID JSON. Naked Strings and Numbers are not valid for FORMAT JSON
   ** BINARY_DOUBLE will return NULL if the value is a not a valid JSON Number
   ** SV will return null if a string value is too long 
   ** CV will catch eveything else but NULL
   **
   */
   
   if (SUBSTR(P_UNORDERED_JSON,1,1) = '[') then
     -- DBMS_OUTPUT.PUT_LINE('Array');
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,'[');  
     for O in (
	   select v.I
	         ,v.JV
	         ,case 
	             when v.SV in ('true','false') and v.NV in (1,0) then 
	  			  JSON_ARRAY(v.SV format JSON returning CLOB)				
	            when v.NV is not NULL then
	  			  JSON_ARRAY(v.NV returning CLOB)
	  		    when v.SV is not NULL then 
 	  		      JSON_ARRAY(v.SV returning CLOB)
	  		    when v.CV is not NULL then 
	  			  JSON_ARRAY(NULL format JSON NULL ON NULL returning CLOB)				
	  		    else 
	  		     to_CLOB('[null]')
	  	      end JSON
         from JSON_TABLE(P_UNORDERED_JSON,'$[*]' 
	  	        COLUMNS 
	  		      I                       FOR ORDINALITY
	  		    , NV    NUMBER            PATH '$' 
	  		    , SV    VARCHAR2(32767)   PATH '$' 
	  		    , CV    CLOB              PATH '$'
	  		    , JV    CLOB FORMAT JSON  PATH '$'
	          ) V
	    order by v.I 
	  ) loop
	    if (o.i > 1) then
          DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,',');  
		end if;
        if (o.JV is not NULL) then
     	  ORDER_JSON_OBJECTS(o.JV,P_ORDERED_JSON);
        else 
		  DBMS_LOB.CREATETEMPORARY(V_ESCAPED_VALUE, TRUE );
		  DBMS_LOB.COPY(V_ESCAPED_VALUE,o.JSON,DBMS_LOB.GETLENGTH(o.JSON)-2,1,2);
		  DBMS_LOB.APPEND(P_ORDERED_JSON,V_ESCAPED_VALUE);
		  DBMS_LOB.FREETEMPORARY(V_ESCAPED_VALUE);
		end if;
      end loop;
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,']');   
   else
     -- DBMS_OUTPUT.PUT_LINE('Object');
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,'{');  
     for O in (
	   with ORDERED_KEYS as (
	     select K
		       ,NV
			   ,SV
			   ,CV
			   ,JV
           from JSON_TABLE(YADAMU_UTILITIES.JSON_KEYS(P_UNORDERED_JSON),'$[*]' COLUMNS I FOR ORDINALITY, K VARCHAR2(256) PATH '$') K,
                JSON_TABLE(P_UNORDERED_JSON,'$.*' 
	  	        COLUMNS 
	  		      I                      FOR ORDINALITY
	  		    , NV   NUMBER            PATH '$' 
  	  		    , SV   VARCHAR2(32767)   PATH '$' 
	  		    , CV   CLOB              PATH '$'
	  		    , JV   CLOB FORMAT JSON  PATH '$'
	            ) V
          where k.I = v.I
	      order by k.K 
	   ) 
       select case when (ROWNUM = 1) then '"' else ',"' end || K || '":' K
	         ,JV
	         ,case 
	            when SV in ('true','false') and NV in (1,0) then 
	  			  JSON_ARRAY(SV format JSON returning CLOB)				
	            when NV is not NULL then
	  			  JSON_ARRAY(NV returning CLOB)
	  		    when SV is not NULL then 
 	  		      JSON_ARRAY(SV returning CLOB)
	  		    when CV is not NULL then 
 	  		      JSON_ARRAY(CV returning CLOB)
	  		    else 
	  			  JSON_ARRAY(NULL format JSON NULL ON NULL returning CLOB)				
	  	      end JSON
		 from ORDERED_KEYS
      ) loop
	    DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,length(o.K),o.K);  
		-- DBMS_OUTPUT.PUT_LINE('KEY' || o.K);
        if (o.JV is not NULL) then
     	  ORDER_JSON_OBJECTS(o.JV,P_ORDERED_JSON);
        else 
		  DBMS_LOB.CREATETEMPORARY(V_ESCAPED_VALUE, TRUE );
		  DBMS_LOB.COPY(V_ESCAPED_VALUE,o.JSON,DBMS_LOB.GETLENGTH(o.JSON)-2,1,2);
		  DBMS_LOB.APPEND(P_ORDERED_JSON,V_ESCAPED_VALUE);
		  DBMS_LOB.FREETEMPORARY(V_ESCAPED_VALUE);
		end if;
     end loop;
     DBMS_LOB.WRITEAPPEND(P_ORDERED_JSON,1,'}');       
   end if;
end;
--
function ORDER_JSON_OBJECTS(P_UNORDERED_JSON CLOB)
return CLOB
as
  V_ORDERED_JSON    CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_ORDERED_JSON, TRUE );
  ORDER_JSON_OBJECTS(P_UNORDERED_JSON,V_ORDERED_JSON);
  return V_ORDERED_JSON;
end;
--
function ORDER_JSON_OBJECTS(P_UNORDERED_JSON BLOB)
return BLOB
as
  V_ORDERED_JSON    CLOB;
  V_UNORDERED_CLOB  CLOB;
  V_ORDERED_BLOB    BLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_ORDERED_JSON, TRUE );
  select JSON_QUERY(P_UNORDERED_JSON, '$' RETURNING CLOB) into V_UNORDERED_CLOB from DUAL;
  ORDER_JSON_OBJECTS(V_UNORDERED_CLOB,V_ORDERED_JSON);
  select JSON_QUERY(V_ORDERED_JSON, '$' RETURNING BLOB) into V_ORDERED_BLOB from DUAL;
  return V_ORDERED_BLOB;
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