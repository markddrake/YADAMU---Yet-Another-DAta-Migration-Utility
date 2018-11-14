--
create or replace package OBJECT_SERIALIZATION
AUTHID CURRENT_USER
as
  TYPE T_TABLE_INFO_RECORD is RECORD (
    OWNER      VARCHAR2(128)
   ,TABLE_NAME VARCHAR2(128)
   );  

  TYPE T_TABLE_INFO_TABLE is TABLE of T_TABLE_INFO_RECORD;
  
  TYPE TYPE_LIST_T is RECORD (
    OWNER               VARCHAR2(128)
  , TYPE_NAME           VARCHAR2(128)
  , ATTR_COUNT          NUMBER
  , TYPECODE            VARCHAR2(32)
  );

  TYPE TYPE_LIST_TAB is TABLE of TYPE_LIST_T;
  
  function SERIALIZE_TYPE(P_TYPE_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2) return CLOB;
  function SERIALIZE_TABLE_TYPES(P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2) return CLOB;
  function SERIALIZE_TABLE_TYPES(P_TABLE_LIST T_TABLE_INFO_TABLE) return CLOB;

  function DESERIALIZE_TYPE(P_TYPE_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2) return VARCHAR2;
  function DESERIALIZE_TABLE_TYPES(P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2) return CLOB;
  function DESERIALIZE_TABLE_TYPES(P_TABLE_LIST T_TABLE_INFO_TABLE) return CLOB;
  procedure DESERIALIZE_TYPE(P_TYPE_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2, P_PLSQL_BLOCK IN OUT NOCOPY CLOB);
  procedure DESERIALIZE_TABLE_TYPES(P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2, P_PLSQL_BLOCK IN OUT NOCOPY CLOB);
  procedure DESERIALIZE_TABLE_TYPES(P_TABLE_LIST T_TABLE_INFO_TABLE, P_PLSQL_BLOCK IN OUT NOCOPY CLOB);

  function CLOB2XMLTYPE(P_SERIALIZATION CLOB) return XMLTYPE;
  
  function CODE_BFILE2CHAR return VARCHAR2 deterministic;
  function CODE_BLOB2BASE64 return VARCHAR2 deterministic;
  function CODE_BLOB2HEXBINARY return VARCHAR2 deterministic;

  function CODE_CHAR2BFILE return VARCHAR2 deterministic;
  -- function CODE_BLOB2BASE64 return VARCHAR2 deterministic;
  function CODE_HEXBINARY2BLOB return VARCHAR2 deterministic;
  
  function CODE_SERIALIZE_ANYDATA return VARCHAR2 deterministic;
  function CODE_SERIALIZE_OBJECT return VARCHAR2 deterministic;

  function CHAR2BFILE(P_SERIALIZATION VARCHAR2) return BFILE;
  function BFILE2CHAR(P_BFILE BFILE) return VARCHAR2;
  function CHUNKS2BLOB(P_CHUNKED_CLOB CHUNKED_CLOB_T)  return BLOB;
  function CHUNKS2CLOB(P_CHUNKED_CLOB CHUNKED_CLOB_T)  return CLOB;
  function HEXBINARY2BLOB(P_SERIALIZATION CLOB) return BLOB;
end;
/
--
set TERMOUT on
--
show errors
--
@@SET_TERMOUT
--
create or replace package body OBJECT_SERIALIZATION
as
--
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := DBMS_LOB.LOBMAXSIZE;
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := 32767;
  $ELSE
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := 4000;
  $END
--
  C_NEWLINE       CONSTANT CHAR(1) := CHR(10);
  C_SINGLE_QUOTE  CONSTANT CHAR(1) := CHR(39);
--
  C_BFILE2CHAR CONSTANT VARCHAR2(512) := 
--
'function BFILE2CHAR(P_BFILE BFILE) return VARCHAR2
as
  V_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);
  V_DIRECTORY_ALIAS  VARCHAR2(128 CHAR);
  V_PATH2FILE        VARCHAR2(2000 CHAR);
begin
  DBMS_LOB.FILEGETNAME(P_BFILE,V_DIRECTORY_ALIAS,V_PATH2FILE);
  return ''BFILENAME('' || V_SINGLE_QUOTE || V_DIRECTORY_ALIAS || V_SINGLE_QUOTE || '','' || V_SINGLE_QUOTE || V_PATH2FILE || V_SINGLE_QUOTE || '')'';
end;
';
--
  C_CHAR2BFILE CONSTANT VARCHAR2(512) := 
--
'function CHAR2BFILE(P_SERIALIZATION VARCHAR2) 
return BFILE
as
  V_BFILE BFILE := NULL;
begin
  if (P_SERIALIZATION is not NULL) then
    EXECUTE IMMEDIATE ''select '' || P_SERIALIZATION || '' from dual'' into V_BFILE;
  end if;
  return V_BFILE;
end;
';
--  
  C_BLOB2HEXBINARY CONSTANT VARCHAR2(1024) := 
--
'function BLOB2HEXBINARY(P_BLOB BLOB)
return CLOB
is
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := ' || (TRUNC(C_MAX_SUPPORTED_SIZE / 2) - 4) || ';
  V_BLOB_SIZE      NUMBER := DBMS_LOB.GETLENGTH(P_BLOB);
  V_HEXBINARY_CLOB CLOB;
  V_OFFSET         INTEGER := 1;
  V_AMOUNT         INTEGER := 2000;
  V_RAW_CONTENT    RAW(2000);
begin
  if (V_BLOB_SIZE > C_MAX_SUPPORTED_SIZE) then
    return TO_CLOB(''BLOB2HEXBINARY: Input size ('' || V_BLOB_SIZE || '') exceeds maximum supported size ('' || C_MAX_SUPPORTED_SIZE || '').'');
  end if;
  DBMS_LOB.CREATETEMPORARY(V_HEXBINARY_CLOB,TRUE,DBMS_LOB.CALL);
  while (V_OFFSET <= V_BLOB_SIZE) loop
    V_AMOUNT := 2000;
    DBMS_LOB.READ(P_BLOB,V_AMOUNT,V_OFFSET,V_RAW_CONTENT);
	V_OFFSET := V_OFFSET + V_AMOUNT;
    DBMS_LOB.APPEND(V_HEXBINARY_CLOB,TO_CLOB(RAWTOHEX(V_RAW_CONTENT)));
  end loop;
  return V_HEXBINARY_CLOB;
end;
';
--
  C_BLOB2BASE64 CONSTANT VARCHAR2(1024) := 
--
'function BLOB2BASE64(P_BLOB BLOB)
return CLOB
is
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := ' || (TRUNC(C_MAX_SUPPORTED_SIZE / 1.5) - 4) || ';
  V_BLOB_SIZE      NUMBER := DBMS_LOB.GETLENGTH(P_BLOB);
  V_BASE64_CLOB    CLOB;
  V_OFFSET         INTEGER := 1;
  V_AMOUNT         INTEGER := 2000;drop 
  V_RAW_CONTENT    RAW(2000);
begin
  if (V_BLOB_SIZE > C_MAX_SUPPORTED_SIZE) then
    return TO_CLOB(''BLOB2BASE64: Input size ('' || V_BLOB_SIZE || '') exceeds maximum supported size ('' || C_MAX_SUPPORTED_SIZE || '').'');
  end if;
  DBMS_LOB.CREATETEMPORARY(V_BASE64_CLOB,TRUE,DBMS_LOB.CALL);
  while (V_OFFSET <= V_BLOB_SIZE) loop
    V_AMOUNT := 2000;
    DBMS_LOB.READ(P_BLOB,V_AMOUNT,V_OFFSET,V_RAW_CONTENT);
	V_OFFSET := V_OFFSET + V_AMOUNT;
    DBMS_LOB.APPEND(V_BASE64_CLOB,UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_ENCODE(V_RAW_DATA)));
  end loop;
  return V_BASE64_CLOB;
end;
';
--
  C_HEXBINARY2BLOB CONSTANT VARCHAR2(1024) := 
--
'function HEXBINARY2BLOB(P_SERIALIZATION CLOB)
return BLOB
is
  V_BLOB BLOB;
  V_OFFSET INTEGER := 1;
  V_AMOUNT INTEGER := 32000;
  V_INPUT_LENGTH NUMBER := DBMS_LOB.GETLENGTH(P_SERIALIZATION);
  V_HEXBINARY_DATA VARCHAR2(32000);
begin
  if (P_SERIALIZATION is NULL) then return NULL; end if;
  if (DBMS_LOB.substr(P_SERIALIZATION,15,1) = ''BLOB2HEXBINARY:'') then return NULL; end if;
  DBMS_LOB.CREATETEMPORARY(V_BLOB,TRUE,DBMS_LOB.CALL);
  while (V_OFFSET <= V_INPUT_LENGTH) loop
    V_AMOUNT := 32000;
    DBMS_LOB.READ(P_SERIALIZATION,V_AMOUNT,V_OFFSET,V_HEXBINARY_DATA);
    V_OFFSET := V_OFFSET + V_AMOUNT;
    DBMS_LOB.APPEND(V_BLOB,TO_BLOB(HEXTORAW(V_HEXBINARY_DATA)));
  end loop;
  return V_BLOB;
end;
';
--
  C_CHUNKS2CLOB CONSTANT VARCHAR2(1024) := 
--
'function CHUNKS2CLOB(P_CHUNKED_CLOB CHUNKED_CLOB_T)  return CLOB 
as 
  V_CLOB CLOB; 
  cursor getChunks
  is 
  select * from TABLE(P_CHUNKED_CLOB); 
begin 
  DBMS_LOB.createTemporary(V_CLOB,TRUE,DBMS_LOB.CALL); 
  for c in getChunks loop 
    DBMS_LOB.APPEND(V_CLOB,TO_CLOB(c.COLUMN_VALUE)); 
  end loop; 
  return V_CLOB; 
end;
';
--
  C_CHUNKS2BLOB CONSTANT VARCHAR2(1024) := 
--
'function CHUNKS2BLOB(P_CHUNKED_CLOB CHUNKED_CLOB_T)  return BLOB 
as 
  V_BLOB BLOB; 
  cursor getChunks
  is 
  select * from TABLE(P_CHUNKED_CLOB); 
begin 
  DBMS_LOB.createTemporary(V_BLOB,TRUE,DBMS_LOB.CALL); 
  for c in getChunks loop 
    DBMS_LOB.APPEND(V_BLOB,TO_BLOB(HEXTORAW(c.COLUMN_VALUE))); 
  end loop; 
  return V_BLOB; 
end;
';
-- 
  C_BLOB2CHUNKS CONSTANT VARCHAR2(2048) := 
-- 
/* 
** Converts the BLOB to an ARRAY of HEXBINARY encoded Strings.. 
*/
'function BLOB2CHUNKS(P_BLOB BLOB)
return CLOB
is
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := ' || (C_MAX_SUPPORTED_SIZE - 4)   || ';
  C_SINGLE_QUOTE CONSTANT CHAR(1)  := CHR(39);
  C_CHUNK_SIZE   CONSTANT NUMBER := 2000;
  V_CLOB         CLOB;
  V_BLOB_LENGTH  NUMBER := DBMS_LOB.GETLENGTH(P_BLOB);
  V_OFFSET       NUMBER := 1;
  V_AMOUNT       NUMBER;
  V_CHUNK        VARCHAR2(4004); 
  V_RAW_DATA     RAW(2000);
begin
  DBMS_LOB.createTemporary(V_CLOB,TRUE,DBMS_LOB.CALL); 
  V_CHUNK := ''OBJECT_SERIALIZATION.CHUNKS2BLOB(CHUNKED_CLOB_T('';
  DBMS_LOB.WRITEAPPEND(V_CLOB,length(V_CHUNK),V_CHUNK); 
  while (V_OFFSET <= V_BLOB_LENGTH) loop
    V_AMOUNT := C_CHUNK_SIZE;
    DBMS_LOB.READ(P_BLOB,V_AMOUNT,V_OFFSET,V_RAW_DATA);
	V_OFFSET := V_OFFSET + V_AMOUNT;
    V_CHUNK := C_SINGLE_QUOTE || RAWTOHEX(V_RAW_DATA) || C_SINGLE_QUOTE; 
    if (V_OFFSET < V_BLOB_LENGTH) then 
      V_CHUNK := V_CHUNK || '',''; 
    end if; 
	DBMS_LOB.WRITEAPPEND(V_CLOB,length(V_CHUNK),V_CHUNK); 
  end loop;
  DBMS_LOB.WRITEAPPEND(V_CLOB,2,''))''); 
  if (DBMS_LOB.GETLENGTH(V_CLOB) > C_MAX_SUPPORTED_SIZE) then
    return TO_CLOB(''BLOB2CHUNKS: Serialized size ('' || DBMS_LOB.GETLENGTH(V_CLOB) || '') exceeds maximum supported size ('' || C_MAX_SUPPORTED_SIZE || '').'');
  else
    return V_CLOB;
  end if;
end;
';
--
  C_CLOB2CHUNKS CONSTANT VARCHAR2(2048) := 
--
'function CLOB2CHUNKS(P_CLOB CLOB) return CLOB 
as 
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := ' || (C_MAX_SUPPORTED_SIZE - 4)   || ';
  C_SINGLE_QUOTE CONSTANT CHAR(1)  := CHR(39);
  C_CHUNK_SIZE   CONSTANT NUMBER := 4000;
  V_CLOB         CLOB; 
  V_CLOB_LENGTH  NUMBER := DBMS_LOB.GETLENGTH(P_CLOB); 
  V_OFFSET       NUMBER := 1; 
  V_AMOUNT       NUMBER; 
  V_CHUNK        VARCHAR2(4000); 
begin 
  DBMS_LOB.createTemporary(V_CLOB,TRUE,DBMS_LOB.CALL); 
  V_CHUNK := ''OBJECT_SERIALIZATION.CHUNKS2CLOB(CHUNKED_CLOB_T('';
  DBMS_LOB.WRITEAPPEND(V_CLOB,length(V_CHUNK),V_CHUNK); 
  while (V_OFFSET < V_CLOB_LENGTH) loop 
    V_AMOUNT := C_CHUNK_SIZE - 3; 
    DBMS_LOB.READ(P_CLOB,V_AMOUNT,V_OFFSET,V_CHUNK); 
    V_OFFSET := V_OFFSET + V_AMOUNT; 
    V_CHUNK := C_SINGLE_QUOTE || V_CHUNK || C_SINGLE_QUOTE; 
    if (V_OFFSET < V_CLOB_LENGTH) then 
      V_CHUNK := V_CHUNK || '',''; 
    end if; 
	DBMS_LOB.WRITEAPPEND(V_CLOB,length(V_CHUNK),V_CHUNK); 
  end loop; 
  DBMS_LOB.WRITEAPPEND(V_CLOB,2,''))''); 
  if (DBMS_LOB.GETLENGTH(V_CLOB) > C_MAX_SUPPORTED_SIZE) then
    return TO_CLOB(''CLOB2CHUNKS: Serialized size ('' || DBMS_LOB.GETLENGTH(V_CLOB) || '') exceeds maximum supported size ('' || C_MAX_SUPPORTED_SIZE || '').'');
  else
    return V_CLOB;
  end if;
end; 
function CHUNKLARGECLOB(P_CLOB CLOB) return CLOB 
as 
  C_CHUNK_SIZE   CONSTANT NUMBER := 4000;
begin 
  if (DBMS_LOB.GETLENGTH(P_CLOB) > C_CHUNK_SIZE) then 
    return CLOB2CHUNKS(P_CLOB); 
  else 
    return P_CLOB; 
  end if; 
end;
';
--
  C_SERIALIZE_ANYDATA CONSTANT VARCHAR2(32767) := 
--
/*
** Cannot handle OBJECT TYPES as it is necessary to create a variable of the type in order to get payaload, which requires the TYPES to be known in advance.
** Also cannot handle types with certain oracle data types such as INTEGER, since the ANYTYPE does not support ''GETTERS'' for these data types.
** Obvious solution to handle these cases using Dynamic SQL does not work for nested objects as context for the ''PIECEWISE'' operations is lost when the** ANYDATA object is passed to EXECUTE IMMEDIATE
** Also the ''GETTER'' for ROWID appears to be missing ???
*/
'procedure SERIALIZE_ANYDATA(P_ANYDATA ANYDATA, P_SERIALIZATION IN OUT NOCOPY CLOB)
as
  V_SINGLE_QUOTE         CONSTANT CHAR(1) := CHR(39);
  TYPE TYPE_INFO_T is RECORD (
    TYPE_ID              NUMBER
   ,PRECISION            NUMBER
   ,SCALE                NUMBER
   ,length               NUMBER
   ,CSID                 NUMBER
   ,CSFRM                NUMBER
   ,SCHEMA_NAME          VARCHAR2(128)
   ,TYPE_NAME            VARCHAR2(128)
   ,TYPE_VERSION         VARCHAR2(128)
   ,ATTR_COUNT           NUMBER
  );
  V_TYPE_ID              NUMBER;
  V_TYPE_METADATA        ANYTYPE;
  V_TYPE_INFO            TYPE_INFO_T;
  V_OBJECT_CONSTRUCTOR   VARCHAR2(512);
  V_ANYDATA_CONTENT_TYPE VARCHAR2(512);
  V_RESULT               PLS_INTEGER;
  V_ANYDATA_PAYLOAD      CLOB;
  V_CLOB                 CLOB;
  V_BLOB                 BLOB;
  V_NCLOB                NCLOB;
  V_BFILE                BFILE;
  V_BDOUBLE              BINARY_DOUBLE;
  V_BFLOAT               BINARY_FLOAT;
  V_NUMBER               NUMBER;
  V_CHAR                 CHAR(4000);
  V_NCHAR                NCHAR(4000);
  V_VARCHAR              VARCHAR(32767);
  V_VARCHAR2             VARCHAR2(32767);
  V_NVARCHAR2            NVARCHAR2(32767);
  V_RAW                  RAW(32767);
  V_DATE                 DATE;
  V_INTERVAL_DS          INTERVAL DAY TO SECOND;
  V_INTERVAL_YM          INTERVAL YEAR TO MONTH;
  V_TIMESTAMP            TIMESTAMP;
  V_TIMESTAMP_TZ         TIMESTAMP WITH TIME ZONE;
  V_TIMESTAMP_LTZ        TIMESTAMP WITH LOCAL TIME ZONE;
  V_UROWID               UROWID;
begin
  if (P_ANYDATA is NULL) then
    DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');
    return;
  end if;
  V_TYPE_ID := P_ANYDATA.getType(V_TYPE_METADATA);
  V_TYPE_INFO.TYPE_ID := V_TYPE_METADATA.getInfo(
    V_TYPE_INFO.PRECISION,
    V_TYPE_INFO.SCALE,
    V_TYPE_INFO.length,
    V_TYPE_INFO.CSID,
    V_TYPE_INFO.CSFRM,
    V_TYPE_INFO.SCHEMA_NAME,
    V_TYPE_INFO.TYPE_NAME,
    V_TYPE_INFO.TYPE_VERSION,
    V_TYPE_INFO.ATTR_COUNT
  );
  V_OBJECT_CONSTRUCTOR := ''"SYS"."ANYDATA"('';
  V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || V_TYPE_INFO.SCHEMA_NAME || V_SINGLE_QUOTE;
  case V_TYPE_ID
    when DBMS_TYPES.TYPECODE_BDOUBLE then
      V_RESULT := P_ANYDATA.getBDouble(V_BDOUBLE);
	  V_ANYDATA_PAYLOAD := TO_CHAR(V_BDOUBLE);
    when DBMS_TYPES.TYPECODE_BFILE then
      V_RESULT := P_ANYDATA.getBFile(V_BFILE);
	  V_ANYDATA_PAYLOAD := BFILE2CHAR(V_BFILE);
	when DBMS_TYPES.TYPECODE_BFLOAT then
      V_RESULT := P_ANYDATA.getBFloat(V_BFLOAT);
	  V_ANYDATA_PAYLOAD := TO_CHAR(V_BFLOAT);
    when DBMS_TYPES.TYPECODE_BLOB then
	  V_RESULT := P_ANYDATA.getBLOB(V_BLOB);
      DBMS_LOB.CREATETEMPORARY(V_ANYDATA_PAYLOAD,TRUE,DBMS_LOB.CALL);
      DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);
	     DBMS_LOB.APPEND(V_ANYDATA_PAYLOAD,BLOB2HEXBINARY(V_BLOB));
	     DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);
    when DBMS_TYPES.TYPECODE_CHAR then
      V_RESULT := P_ANYDATA.getCHAR(V_CHAR);
    V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || V_CHAR || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_CLOB then
	  V_RESULT := P_ANYDATA.getCLOB(V_CLOB);
      DBMS_LOB.CREATETEMPORARY(V_ANYDATA_PAYLOAD,TRUE,DBMS_LOB.CALL);
	     DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);
	     DBMS_LOB.APPEND(V_ANYDATA_PAYLOAD,V_CLOB);
	     DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);
    when DBMS_TYPES.TYPECODE_DATE then
      V_RESULT := P_ANYDATA.getDate(V_DATE);
	  V_ANYDATA_PAYLOAD := TO_CHAR(V_DATE);
    when DBMS_TYPES.TYPECODE_INTERVAL_DS then
      V_RESULT := P_ANYDATA.getIntervalDS(V_INTERVAL_DS);
	  V_ANYDATA_PAYLOAD := TO_CHAR(V_INTERVAL_DS);
    when DBMS_TYPES.TYPECODE_INTERVAL_YM then
      V_RESULT := P_ANYDATA.getIntervalYM(V_INTERVAL_YM);
	  V_ANYDATA_PAYLOAD := TO_CHAR(V_INTERVAL_YM);
    when DBMS_TYPES.TYPECODE_NCHAR then
      V_RESULT := P_ANYDATA.getNCHAR(V_NCHAR);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_NCHAR) || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_NCLOB then
      V_RESULT := P_ANYDATA.getNCLOB(V_NCLOB);
      DBMS_LOB.CREATETEMPORARY(V_ANYDATA_PAYLOAD,TRUE,DBMS_LOB.CALL);
	     DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);
	     DBMS_LOB.APPEND(V_ANYDATA_PAYLOAD,TO_CLOB(V_NCLOB));
	     DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);
    when DBMS_TYPES.TYPECODE_NUMBER then
      V_RESULT := P_ANYDATA.getNumber(V_NUMBER);
	  V_ANYDATA_PAYLOAD := TO_CHAR(V_NUMBER);
    when DBMS_TYPES.TYPECODE_NVARCHAR2 then
      V_RESULT := P_ANYDATA.getNVarchar2(V_NVARCHAR2);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_NVARCHAR2) || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_RAW then
	  V_RESULT := P_ANYDATA.getRaw(V_RAW);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_RAW) || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_TIMESTAMP then
      V_RESULT := P_ANYDATA.getTimestamp(V_TIMESTAMP);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP) || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then
      V_RESULT := P_ANYDATA.getTimestampLTZ(V_TIMESTAMP_LTZ);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP_LTZ) || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then
      V_RESULT := P_ANYDATA.getTimestampLTZ(V_TIMESTAMP_TZ);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP_TZ) || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_UROWID then
      -- V_RESULT := P_ANYDATA.getROWID(V_ROWID);
	  V_UROWID := P_ANYDATA.accessUROWID();
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_UROWID) || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_VARCHAR2 then
      V_RESULT := P_ANYDATA.getVARCHAR2(V_VARCHAR2);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || V_VARCHAR2 || V_SINGLE_QUOTE;
    when DBMS_TYPES.TYPECODE_VARCHAR then
      V_RESULT := P_ANYDATA.getVARCHAR(V_VARCHAR);
      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || V_VARCHAR || V_SINGLE_QUOTE;
    when 3 then -- INTERGER used in ORDSYS.ORDIMAGE
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: INTEGER.'';
    when DBMS_TYPES.TYPECODE_CFILE then
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: CFILE.'';
   when DBMS_TYPES.TYPECODE_MLSLABEL then
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: MSLABEL.'';
    when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then
      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: NAMEDCOLLECTION.'';
    when DBMS_TYPES.TYPECODE_OBJECT then
      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: OBJECT.'';
    when DBMS_TYPES.TYPECODE_OPAQUE then
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: OPAQUE.'';
    when DBMS_TYPES.TYPECODE_REF then
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: REF.'';
    when DBMS_TYPES.TYPECODE_TABLE then
      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: TABLE.'';
    when DBMS_TYPES.TYPECODE_VARRAY then
      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;
      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: VARRAY.'';
	else
	  V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: UNKNOWN.'';
  end case;
  DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_ANYDATA_CONTENT_TYPE),V_ANYDATA_CONTENT_TYPE);
  DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'','');
  DBMS_LOB.APPEND(P_SERIALIZATION,V_ANYDATA_PAYLOAD);
  DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'')'');
end;
function SERIALIZE_ANYDATA(P_ANYDATA ANYDATA) 
return CLOB
as
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := ' || (C_MAX_SUPPORTED_SIZE - 4)  || ';
  V_SERIALIZATION    CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_SERIALIZATION,TRUE,DBMS_LOB.CALL);
  SERIALIZE_ANYDATA(P_ANYDATA,V_SERIALIZATION);
  if (DBMS_LOB.GETLENGTH(V_SERIALIZATION) > C_MAX_SUPPORTED_SIZE) then
    return TO_CLOB(''SERIALIZE_ANYDATA: Serialized size ('' || DBMS_LOB.GETLENGTH(V_SERIALIZATION) || '') exceeds maximum supported size ('' || C_MAX_SUPPORTED_SIZE || '').'');
  else
    return V_SERIALIZATION;
  end if;
end;
';
--
  C_SERIALIZE_OBJECT_PART1 VARCHAR2(32767) := 
--
'procedure SERIALIZE_OBJECT(P_TABLE_OWNER VARCHAR2,P_ANYDATA ANYDATA, P_SERIALIZATION IN OUT NOCOPY CLOB)
as
  V_SINGLE_QUOTE        CONSTANT CHAR(1) := CHR(39);
  TYPE TYPE_INFO_T is RECORD (
    TYPE_ID             NUMBER
   ,PRECISION           NUMBER
   ,SCALE               NUMBER
   ,length              NUMBER
   ,CSID                NUMBER
   ,CSFRM               NUMBER
   ,OWNER               VARCHAR2(128)
   ,TYPE_NAME           VARCHAR2(128)
   ,TYPE_VERSION        VARCHAR2(128)
   ,ATTR_COUNT          NUMBER
  );
  TYPE ATTR_INFO_T IS RECORD (
    TYPE_ID             NUMBER
   ,PRECISION           NUMBER
   ,SCALE               NUMBER
   ,length              NUMBER
   ,CSID                NUMBER
   ,CSFRM               NUMBER
   ,ATTR_TYPE_METADATA  ANYTYPE
   ,ATTR_NAME           VARCHAR2(128)
  );
  V_TYPE_ID             PLS_INTEGER;
  V_ANYDATA             ANYDATA;
  V_TYPE_METADATA       ANYTYPE;
  V_TYPE_INFO           TYPE_INFO_T;
  V_ATTR_INFO           ATTR_INFO_T;
  V_RESULT              PLS_INTEGER;
  V_OBJECT_CONSTRUCTOR  VARCHAR2(266);
  V_SERIALIZED_VALUE    VARCHAR2(266);
begin
  V_TYPE_ID := P_ANYDATA.getType(V_TYPE_METADATA);
  V_TYPE_INFO.TYPE_ID := V_TYPE_METADATA.getInfo(
    V_TYPE_INFO.PRECISION,
    V_TYPE_INFO.SCALE,
    V_TYPE_INFO.length,
    V_TYPE_INFO.CSID,
    V_TYPE_INFO.CSFRM,
    V_TYPE_INFO.OWNER,
    V_TYPE_INFO.TYPE_NAME,
    V_TYPE_INFO.TYPE_VERSION,
    V_TYPE_INFO.ATTR_COUNT
  );
  case
';
--
  C_SERIALIZE_OBJECT CONSTANT VARCHAR(1024) :=
--
'function SERIALIZE_OBJECT(P_TABLE_OWNER VARCHAR2,P_ANYDATA ANYDATA)
return CLOB
as
  C_MAX_SUPPORTED_SIZE CONSTANT NUMBER := ' || (C_MAX_SUPPORTED_SIZE - 4) || ';
  V_SERIALIZATION    CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_SERIALIZATION,TRUE,DBMS_LOB.CALL);
  SERIALIZE_OBJECT(P_TABLE_OWNER,P_ANYDATA,V_SERIALIZATION);
  if (DBMS_LOB.GETLENGTH(V_SERIALIZATION) > C_MAX_SUPPORTED_SIZE) then
    return TO_CLOB(''SERIALIZE_OBJECT: Serialized size ('' || DBMS_LOB.GETLENGTH(V_SERIALIZATION) || '') exceeds maximum supported size ('' || C_MAX_SUPPORTED_SIZE || '').'');
  else
    return V_SERIALIZATION;
  end if;
end;
';
--
function CODE_BFILE2CHAR return VARCHAR2 deterministic
as
begin
  return C_BFILE2CHAR;
end;
--	
function CODE_BLOB2HEXBINARY return VARCHAR2 deterministic
as
begin
  return C_BLOB2HEXBINARY;
end;
--
function CODE_CHAR2BFILE return VARCHAR2 deterministic
as
begin
  return C_CHAR2BFILE;
end;
--
function CODE_BLOB2BASE64 return VARCHAR2 deterministic
as
begin
  return C_BLOB2BASE64;
end;
--
function CODE_HEXBINARY2BLOB return VARCHAR2 deterministic
as
begin
  return C_HEXBINARY2BLOB;
end;
--
function CODE_CHUNKS2CLOB return VARCHAR2 deterministic 
as begin
  return C_CHUNKS2CLOB;
end;
--
function CODE_CHUNKS2BLOB return VARCHAR2 deterministic
as
begin
  return C_CHUNKS2BLOB;
end;
--
function CODE_BLOB2CHUNKS return VARCHAR2 deterministic
as
begin
  return C_BLOB2CHUNKS;
end;
--
function CODE_CLOB2CHUNKS return VARCHAR2 deterministic
as
begin
  return C_CLOB2CHUNKS;
end;
--
function CODE_SERIALIZE_ANYDATA return VARCHAR2 deterministic
as
begin
  return C_SERIALIZE_ANYDATA;
end;
--
function CODE_SERIALIZE_OBJECT return VARCHAR2 deterministic
as
begin
  return C_SERIALIZE_OBJECT;
end;
--
function CODE_SERIALIZE_OBJECT_PART1 return VARCHAR2 deterministic
as
begin
  return C_SERIALIZE_OBJECT_PART1;
end;
--
function BFILE2CHAR(P_BFILE BFILE) return VARCHAR2
as
  V_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);
  V_DIRECTORY_ALIAS  VARCHAR2(128 CHAR);
  V_PATH2FILE        VARCHAR2(2000 CHAR);
begin
  DBMS_LOB.FILEGETNAME(P_BFILE,V_DIRECTORY_ALIAS,V_PATH2FILE);
  return 'BFILENAME(' || V_SINGLE_QUOTE || V_DIRECTORY_ALIAS || V_SINGLE_QUOTE || ',' || V_SINGLE_QUOTE || V_PATH2FILE || V_SINGLE_QUOTE || ')';
end;
--
function CLOB2XMLTYPE(P_SERIALIZATION CLOB) 
return XMLTYPE
as
begin
  if (P_SERIALIZATION is not NULL) then
    return XMLTYPE(P_SERIALIZATION);
  end if;
  return NULL;
end;
--
function CHAR2BFILE(P_SERIALIZATION VARCHAR2) 
return BFILE
as
  V_BFILE BFILE := NULL;
begin
  if (P_SERIALIZATION is not NULL) then
    EXECUTE IMMEDIATE 'select ' || P_SERIALIZATION || ' from dual' into V_BFILE;
  end if;
  return V_BFILE;
end;
--
function CHUNKS2BLOB(P_CHUNKED_CLOB CHUNKED_CLOB_T)  
return BLOB 
as 
  V_BLOB BLOB; 
  cursor getChunks
  is 
  select * from TABLE(P_CHUNKED_CLOB); 
begin 
  DBMS_LOB.createTemporary(V_BLOB,TRUE,DBMS_LOB.CALL); 
  for c in getChunks loop 
    DBMS_LOB.APPEND(V_BLOB,TO_BLOB(HEXTORAW(c.COLUMN_VALUE))); 
  end loop; 
  return V_BLOB; 
end;
--
function CHUNKS2CLOB(P_CHUNKED_CLOB CHUNKED_CLOB_T)  
return CLOB 
as 
  V_CLOB CLOB; 
  cursor getChunks
  is 
  select * from TABLE(P_CHUNKED_CLOB); 
begin 
  DBMS_LOB.createTemporary(V_CLOB,TRUE,DBMS_LOB.CALL); 
  for c in getChunks loop 
    DBMS_LOB.APPEND(V_CLOB,TO_CLOB(c.COLUMN_VALUE)); 
  end loop; 
  return V_CLOB; 
end;
--
function HEXBINARY2BLOB(P_SERIALIZATION CLOB)
return BLOB
is
  V_BLOB BLOB;
  V_OFFSET INTEGER := 1;
  V_AMOUNT INTEGER := 32000;
  V_INPUT_LENGTH NUMBER := DBMS_LOB.GETLENGTH(P_SERIALIZATION);
  V_HEXBINARY_DATA VARCHAR2(32000);
begin
  if (P_SERIALIZATION is NULL) then return NULL; end if;
  if (DBMS_LOB.substr(P_SERIALIZATION,15,1) = 'BLOB2HEXBINARY:') then return NULL; end if;
  DBMS_LOB.CREATETEMPORARY(V_BLOB,TRUE,DBMS_LOB.CALL);
  while (V_OFFSET <= V_INPUT_LENGTH) loop
    V_AMOUNT := 32000;
    DBMS_LOB.READ(P_SERIALIZATION,V_AMOUNT,V_OFFSET,V_HEXBINARY_DATA);
    V_OFFSET := V_OFFSET + V_AMOUNT;
    DBMS_LOB.APPEND(V_BLOB,TO_BLOB(HEXTORAW(V_HEXBINARY_DATA)));
  end loop;
  return V_BLOB;
end;
--
function extendTypeList(P_TYPE_LIST IN OUT NOCOPY TYPE_LIST_TAB, P_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2)
return VARCHAR2
as
  V_COUNT     NUMBER;
  V_TYPECODE  VARCHAR2(32);
begin
  select TYPECODE
    into V_TYPECODE
    from TABLE(P_TYPE_LIST)
   where OWNER = P_OWNER and TYPE_NAME = P_TYPE_NAME;
   return V_TYPECODE;
exception
  when NO_DATA_FOUND then
    select ATTRIBUTES, TYPECODE
      into V_COUNT, V_TYPECODE
      from ALL_TYPES
     where OWNER = P_OWNER
       and TYPE_NAME = P_TYPE_NAME;

    P_TYPE_LIST.extend();
    P_TYPE_LIST(P_TYPE_LIST.count).OWNER := P_OWNER;
    P_TYPE_LIST(P_TYPE_LIST.count).TYPE_NAME := P_TYPE_NAME;
    P_TYPE_LIST(P_TYPE_LIST.count).ATTR_COUNT := V_COUNT;
    P_TYPE_LIST(P_TYPE_LIST.count).TYPECODE := V_TYPECODE;
    return V_TYPECODE;
 when OTHERS then
   RAISE;
end;
--
function serializeAttr(P_ATTR_NAME VARCHAR2, P_ATTR_TYPE_OWNER VARCHAR2, P_ATTR_TYPE_NAME VARCHAR2, P_ATTR_TYPE_MOD VARCHAR2, P_TYPE_LIST IN OUT NOCOPY TYPE_LIST_TAB)
return VARCHAR2
as
  V_PLSQL VARCHAR2(32767);
  V_TYPECODE VARCHAR2(32);
begin
  V_PLSQL := '        if (' || P_ATTR_NAME || ' is NULL) then' || C_NEWLINE
          || '          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || C_NEWLINE
          || '        else' || C_NEWLINE;

  case
    when P_ATTR_TYPE_OWNER is NULL then
      case
        when P_ATTR_TYPE_NAME = 'BINARY DOUBLE' then
          V_PLSQL := V_PLSQL
                  || '         V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  || '         DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'BFILE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := BFILE2CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'BINARY FLOAT' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'BLOB' then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.APPEND(P_SERIALIZATION,BLOB2CHUNKS(' || P_ATTR_NAME || '));' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = CFILE then
        when P_ATTR_TYPE_NAME = 'CHAR' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || ' || P_ATTR_NAME || ' || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'CLOB' then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.APPEND(P_SERIALIZATION,CLOB2CHUNKS(' || P_ATTR_NAME || '));' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'DATE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'INTERVAL DAY TO SECOND' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'INTEGER' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'INTERVAL YEAR TO MONTH' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = MLSLABEL then
        when P_ATTR_TYPE_NAME = 'NCHAR' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'NCLOB' then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.APPEND(P_SERIALIZATION,TO_CLOB(' || P_ATTR_NAME || '));' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'NUMBER' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'NVARCHAR2' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = OPAQUE then
        when P_ATTR_TYPE_NAME = 'RAW' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = REF then
        when P_ATTR_TYPE_NAME = 'TIMESTAMP' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'TIMESTAMP WITH LOCAL TIME ZONE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'TIMESTAMP WITH TIME ZONE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'UROWID' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'VARCHAR2' then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(' || P_ATTR_NAME || '),' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'VARCHAR'  then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(' || P_ATTR_NAME || '),' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        else
          DBMS_OUTPUT.PUT_LINE('Unsupported Type: "' || P_ATTR_TYPE_NAME || '".');
      end case;
    when P_ATTR_TYPE_MOD = 'REF' then
	  -- The serialzied form needs to include the decode function eg "HEXTOREF(' || REF_VALUE || ')" to that it will be correctly deserialized 
	  V_PLSQL := V_PLSQL
              ||'          -- V_SERIALIZED_VALUE := REFTOHEX(' || P_ATTR_NAME || ');' || C_NEWLINE
		      ||'          select ''HEXTOREF('' || V_SINGLE_QUOTE || REFTOHEX(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE || '')'' into V_SERIALIZED_VALUE from dual;' || C_NEWLINE
              ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
    when P_ATTR_TYPE_OWNER is not NULL then
      V_TYPECODE := extendTypeList(P_TYPE_LIST, P_ATTR_TYPE_OWNER, P_ATTR_TYPE_NAME);
      $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('Adding "' || P_ATTR_TYPE_OWNER || '"."' || P_ATTR_TYPE_NAME || '": Type = "' || V_TYPECODE || '". Type count = ' || P_TYPE_LIST.count); $end
      case
        when V_TYPECODE = 'COLLECTION' then
          V_PLSQL := V_PLSQL
                  ||'          V_ANYDATA := ANYDATA.convertCollection(' || P_ATTR_NAME || ');' || C_NEWLINE;
        when V_TYPECODE = 'OBJECT' then
          V_PLSQL := V_PLSQL
                  ||'          V_ANYDATA := ANYDATA.convertObject(' || P_ATTR_NAME || ');' || C_NEWLINE;
      end case;
      V_PLSQL := V_PLSQL
              || '          serialize_Object(P_TABLE_OWNER,V_ANYDATA,P_SERIALIZATION);' || C_NEWLINE;
    else
      V_PLSQL := V_PLSQL
              ||'           V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
              ||'           DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
  end case;

  return V_PLSQL;

end;

function serializeType(P_TYPE_RECORD TYPE_LIST_T,P_TYPE_LIST IN OUT NOCOPY TYPE_LIST_TAB)
return CLOB
/*
**
** Returns a PL/SQL 'when' block that will serialize a type using the attribute names.
**
** Assumes that the type to be serialized is in an PL/SQL ANYDATA Variable called P_ANYDATA
** and the serialized content will be wriite to PL/SQL CLOB Variable called P_SERIALIZATION
**
**/
as
  V_PLSQL_BLOCK      CLOB;
  V_PLSQL            VARCHAR2(32767);

  cursor getAttributes
  is
  select ATTR_NAME, ATTR_TYPE_OWNER, ATTR_TYPE_NAME, ATTR_TYPE_MOD, ATTR_NO
    from ALL_TYPE_ATTRS
   where OWNER = P_TYPE_RECORD.OWNER
     and TYPE_NAME = P_TYPE_RECORD.TYPE_NAME
   order by ATTR_NO;

  cursor getCollectionTypeInfo
  is
  select ELEM_TYPE_OWNER, ELEM_TYPE_NAME, ELEM_TYPE_MOD
    from ALL_COLL_TYPES
   where OWNER = P_TYPE_RECORD.OWNER
     and TYPE_NAME = P_TYPE_RECORD.TYPE_NAME;

begin
  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('serializeType() : Processing Type: "' || P_TYPE_RECORD.OWNER || '"."' || P_TYPE_RECORD.TYPE_NAME || '".'); $end
  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);


  V_PLSQL := '    when V_TYPE_INFO.OWNER = ''' || P_TYPE_RECORD.OWNER || ''' and V_TYPE_INFO.TYPE_NAME = ''' || P_TYPE_RECORD.TYPE_NAME || ''' then' || C_NEWLINE
          || '      declare' || C_NEWLINE
          || '        V_OBJECT           "' || P_TYPE_RECORD.OWNER || '"."' || P_TYPE_RECORD.TYPE_NAME || '";' || C_NEWLINE
          || '      begin' || C_NEWLINE;

  if (P_TYPE_RECORD.TYPECODE = 'OBJECT') then
    V_PLSQL := V_PLSQL
            || '        V_RESULT := P_ANYDATA.getObject(V_OBJECT);' || C_NEWLINE;
  else
    V_PLSQL := V_PLSQL
            || '        V_RESULT := P_ANYDATA.getCollection(V_OBJECT);' || C_NEWLINE;
  end if;

  V_PLSQL := V_PLSQL
          || '        if (V_OBJECT is NULL) then' || C_NEWLINE
          || '          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || C_NEWLINE
          || '          return;' || C_NEWLINE
          || '        end if; ' || C_NEWLINE
		  || '        if (P_TABLE_OWNER = ''' || P_TYPE_RECORD.OWNER || ''') then ' || C_NEWLINE
		  || '   	    V_OBJECT_CONSTRUCTOR := ''"'|| P_TYPE_RECORD.TYPE_NAME || '"('';' || C_NEWLINE
		  || '        else' || C_NEWLINE
		  || '   	    V_OBJECT_CONSTRUCTOR := ''"' || P_TYPE_RECORD.OWNER || '"."' || P_TYPE_RECORD.TYPE_NAME || '"('';' || C_NEWLINE
		  || '        end if;' ||C_NEWLINE;
		  
  V_PLSQL := V_PLSQL

  || '        DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,length(V_OBJECT_CONSTRUCTOR),V_OBJECT_CONSTRUCTOR);' || C_NEWLINE;

  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(V_PLSQL),V_PLSQL);

  if P_TYPE_RECORD.TYPECODE = 'OBJECT' then

    for a in getAttributes loop

      $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('serializeType() : Processing Attribute: ' || a.ATTR_NAME || '. Data Type: ' || a.ATTR_TYPE_NAME); $end

      V_PLSQL := serializeAttr('V_OBJECT."' || a.ATTR_NAME || '"', a.ATTR_TYPE_OWNER, a.ATTR_TYPE_NAME, a.ATTR_TYPE_MOD, P_TYPE_LIST);

      V_PLSQL := V_PLSQL
              || '        end if;' || C_NEWLINE;

      if (a.ATTR_NO <  P_TYPE_RECORD.ATTR_COUNT) then
        V_PLSQL := V_PLSQL
                ||'         DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'','');' || C_NEWLINE;
      else
        V_PLSQL := V_PLSQL
                || '        DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'')'');' || C_NEWLINE;
      end if;

      DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(V_PLSQL),V_PLSQL);

    end loop;
  else
    for c in getCollectionTypeInfo loop
      V_PLSQL := '        for IDX in 1..V_OBJECT.count loop' || C_NEWLINE
              || serializeAttr('V_OBJECT(IDX)',c.ELEM_TYPE_OWNER,c.ELEM_TYPE_NAME,c.ELEM_TYPE_MOD,P_TYPE_LIST) || C_NEWLINE
              || '        end if;' || C_NEWLINE
              || '        if (IDX < V_OBJECT.count) then ' || C_NEWLINE
              || '          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'','');' || C_NEWLINE
              || '        end if;' || C_NEWLINE
              || '        end loop;' || C_NEWLINE
              || '        DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'')'');' || C_NEWLINE;
    end loop;
    DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(V_PLSQL),V_PLSQL);
  end if;

  V_PLSQL := '      end;' || C_NEWLINE;
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(V_PLSQL),V_PLSQL);

  return V_PLSQL_BLOCK;

end;
--
function serializeTypes(P_TYPE_LIST IN OUT NOCOPY TYPE_LIST_TAB)
return CLOB
as
--
  V_PLSQL_BLOCK    CLOB;
  V_CASE_BLOCK     CLOB;
  V_SQL_FRAGMENT   VARCHAR2(32767);
  V_ANYTYPE        ANYTYPE;
  V_IDX            PLS_INTEGER := 1;

begin
  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(CODE_BFILE2CHAR),CODE_BFILE2CHAR);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(CODE_BLOB2HEXBINARY),CODE_BLOB2HEXBINARY);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(CODE_CLOB2CHUNKS),CODE_CLOB2CHUNKS);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(CODE_BLOB2CHUNKS),CODE_BLOB2CHUNKS);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(CODE_SERIALIZE_ANYDATA),CODE_SERIALIZE_ANYDATA);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(CODE_SERIALIZE_OBJECT_PART1),CODE_SERIALIZE_OBJECT_PART1);
  
  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.serializeTypes(): Type count = ' || P_TYPE_LIST.count); $end

  if (P_TYPE_LIST.count = 0) then
    return NULL;
  end if;

  loop
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.serializeTypes() : Processing[' || V_IDX || '].'); $end
    V_CASE_BLOCK := serializeType(P_TYPE_LIST(V_IDX),P_TYPE_LIST);
    DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_CASE_BLOCK);
    DBMS_LOB.FREETEMPORARY(V_CASE_BLOCK);
    exit when (V_IDX = P_TYPE_LIST.count);
    V_IDX := V_IDX + 1;
  end loop;

  V_SQL_FRAGMENT := '  end case;' || C_NEWLINE
                 || 'end;' || C_NEWLINE;

  DBMS_LOB.writeAppend(V_PLSQL_BLOCK,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

  DBMS_LOB.writeAppend(V_PLSQL_BLOCK,length(CODE_SERIALIZE_OBJECT),CODE_SERIALIZE_OBJECT);

  return V_PLSQL_BLOCK;

end;
--
function SERIALIZE_TYPE(P_TYPE_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
   select distinct OWNER, TYPE_NAME, ATTRIBUTES, TYPECODE
     bulk collect into V_TYPE_LIST
     from ALL_TYPES
          start with OWNER = P_TYPE_OWNER and TYPE_NAME = P_TYPE_NAME
          connect by prior TYPE_NAME = SUPERTYPE_NAME
                 and prior OWNER = SUPERTYPE_OWNER;
  return serializeTypes(V_TYPE_LIST);
end;
--
function SERIALIZE_TABLE_TYPES(P_TABLE_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
  select distinct OWNER, TYPE_NAME, ATTRIBUTES, TYPECODE
    bulk collect into V_TYPE_LIST
  from ALL_TYPES at,
       (
         select distinct DATA_TYPE_OWNER,  DATA_TYPE
           from ALL_TAB_COLS atc
          where atc.DATA_TYPE_OWNER is not NULL
            and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
	        and ((HIDDEN_COLUMN = 'NO') or (COLUMN_NAME = 'SYS_NC_ROWINFO$'))
            and atc.OWNER = P_TABLE_OWNER
            and atc.TABLE_NAME = P_TABLE_NAME
       ) tlt
       start with at.TYPE_NAME = tlt.DATA_TYPE
              and at.OWNER = tlt.DATA_TYPE_OWNER
                  connect by prior at.TYPE_NAME = SUPERTYPE_NAME
                         and prior at.OWNER = SUPERTYPE_OWNER;

  return serializeTypes(V_TYPE_LIST);
end;
--
function SERIALIZE_TABLE_TYPES(P_TABLE_LIST T_TABLE_INFO_TABLE)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
  select distinct OWNER, TYPE_NAME, ATTRIBUTES, TYPECODE
    bulk collect into V_TYPE_LIST
  from ALL_TYPES at,
       (
         select distinct DATA_TYPE_OWNER,  DATA_TYPE
           from ALL_TAB_COLS atc, TABLE(P_TABLE_LIST) tl
          where atc.DATA_TYPE_OWNER is not NULL
            and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
	        and ((HIDDEN_COLUMN = 'NO') or (COLUMN_NAME = 'SYS_NC_ROWINFO$'))
            and atc.OWNER = tl.OWNER
            and atc.TABLE_NAME = tl.TABLE_NAME
       ) tlt
       start with at.TYPE_NAME = tlt.DATA_TYPE
              and at.OWNER = tlt.DATA_TYPE_OWNER
                  connect by prior at.TYPE_NAME = SUPERTYPE_NAME
                         and prior at.OWNER = SUPERTYPE_OWNER;

  return serializeTypes(V_TYPE_LIST);
end;
--
function DESERIALIZE_TYPE(P_TYPE_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2) 
return VARCHAR2 
as
  V_TYPE_REFERENCE VARCHAR2(266);
  V_SQL_FRAGMENT   VARCHAR2(4000);
begin
    V_TYPE_REFERENCE := '"' || P_TYPE_OWNER || '"."' || P_TYPE_NAME || '"';

	V_SQL_FRAGMENT := 'function "#' || P_TYPE_NAME || '"(P_SERIALIZATION CLOB)' || C_NEWLINE
	               || 'return ' ||  V_TYPE_REFERENCE || C_NEWLINE
				   || 'as' || C_NEWLINE
				   || '   V_OBJECT ' || V_TYPE_REFERENCE ||';' || C_NEWLINE
				   || 'begin' || C_NEWLINE
				   || '  if (P_SERIALIZATION is NULL) then return NULL; end if;' || C_NEWLINE  
                   || '  if (DBMS_LOB.SUBSTR(P_SERIALIZATION,17,1) = ''SERIALIZE_OBJECT:'') then return NULL; end if;	'|| C_NEWLINE			   
				   || '  EXECUTE IMMEDIATE ''SELECT '' || P_SERIALIZATION || '' FROM DUAL'' into V_OBJECT;' || C_NEWLINE
				   || '  return V_OBJECT;' || C_NEWLINE
				   || 'end;' || C_NEWLINE;
                   
  return V_SQL_FRAGMENT;
end;
--
procedure DESERIALIZE_TYPE(P_TYPE_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2, P_PLSQL_BLOCK IN OUT NOCOPY CLOB)
as
  V_SQL_FRAGMENT   VARCHAR2(4000);
begin
  V_SQL_FRAGMENT := DESERIALIZE_TYPE(P_TYPE_OWNER,P_TYPE_NAME);
  DBMS_LOB.WRITEAPPEND(P_PLSQL_BLOCK,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
end;
--
procedure DESERIALIZE_TYPES(P_TYPE_LIST IN OUT NOCOPY TYPE_LIST_TAB, P_PLSQL_BLOCK IN OUT NOCOPY CLOB)
as
--
  V_SQL_FRAGMENT   VARCHAR2(4000);
  V_IDX            PLS_INTEGER := 1;
begin

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.DESERIALIZE_TYPES(): Type count = ' || P_TYPE_LIST.count); $end
 
  for V_IDX in 1 .. P_TYPE_LIST.count loop
    V_SQL_FRAGMENT := DESERIALIZE_TYPE(P_TYPE_LIST(V_IDX).OWNER,P_TYPE_LIST(V_IDX).TYPE_NAME);			
    DBMS_LOB.WRITEAPPEND(P_PLSQL_BLOCK,length(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  end loop;

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.DESERIALIZE_TYPES(): Size = ' || DBMS_LOB.GETLENGTH(P_PLSQL_BLOCK)); $end

end;
--
function DESERIALIZE_TYPES(P_TYPE_LIST IN OUT NOCOPY TYPE_LIST_TAB)
return CLOB
as
  V_PLSQL_BLOCK    CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  DESERIALIZE_TYPES(P_TYPE_LIST, V_PLSQL_BLOCK);
  return V_PLSQL_BLOCK;
end;
--
procedure DESERIALIZE_TABLE_TYPES(P_TABLE_OWNER VARCHAR2,P_TABLE_NAME VARCHAR2, P_PLSQL_BLOCK IN OUT NOCOPY CLOB)
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
  select distinct DATA_TYPE_OWNER, DATA_TYPE, NULL, NULL
    bulk collect into V_TYPE_LIST
    from ALL_TAB_COLS atc
   where atc.DATA_TYPE_OWNER is not NULL
     and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
     and ((HIDDEN_COLUMN = 'NO') or (COLUMN_NAME = 'SYS_NC_ROWINFO$'))
     and atc.OWNER = P_TABLE_OWNER
     and atc.TABLE_NAME = P_TABLE_NAME;
  DESERIALIZE_TYPES(V_TYPE_LIST, P_PLSQL_BLOCK);
end;
--
function DESERIALIZE_TABLE_TYPES(P_TABLE_OWNER VARCHAR2,P_TABLE_NAME VARCHAR2)
return CLOB
as
  V_PLSQL_BLOCK    CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  DESERIALIZE_TABLE_TYPES(P_TABLE_OWNER, P_TABLE_NAME, V_PLSQL_BLOCK);
  return V_PLSQL_BLOCK;
end;
--
procedure DESERIALIZE_TABLE_TYPES(P_TABLE_LIST T_TABLE_INFO_TABLE,P_PLSQL_BLOCK IN OUT NOCOPY CLOB)
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin  
  select distinct DATA_TYPE_OWNER, DATA_TYPE, NULL, NULL
    bulk collect into V_TYPE_LIST
    from ALL_TAB_COLS atc, TABLE(P_TABLE_LIST) tl
   where atc.DATA_TYPE_OWNER is not NULL
     and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
    and ((HIDDEN_COLUMN = 'NO') or (COLUMN_NAME = 'SYS_NC_ROWINFO$'))
     and atc.OWNER = tl.OWNER
     and atc.TABLE_NAME = tl.TABLE_NAME;
  DESERIALIZE_TYPES(V_TYPE_LIST, P_PLSQL_BLOCK);
end;
--
function DESERIALIZE_TABLE_TYPES(P_TABLE_LIST T_TABLE_INFO_TABLE)
return CLOB
as
  V_PLSQL_BLOCK    CLOB;
begin
  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  DESERIALIZE_TABLE_TYPES(P_TABLE_LIST,V_PLSQL_BLOCK);
  return V_PLSQL_BLOCK;
end;
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