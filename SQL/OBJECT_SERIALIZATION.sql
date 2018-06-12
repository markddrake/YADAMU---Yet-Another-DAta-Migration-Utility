--
create or replace package OBJECT_SERIALIZATION
AUTHID CURRENT_USER
as
  TYPE TYPE_LIST_T is RECORD (
    OWNER               VARCHAR2(128)
  , TYPE_NAME           VARCHAR2(128)
  , ATTR_COUNT          NUMBER
  , TYPECODE            VARCHAR2(32)
  );

  TYPE TYPE_LIST_TAB is TABLE of TYPE_LIST_T;

  function SERIALIZE_TYPE(P_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2) return CLOB;
  function SERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2) return CLOB;
  function SERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2,P_TABLE_LIST XDB.XDB$STRING_LIST_T) return CLOB;

  function DESERIALIZE_TYPE(P_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2) return CLOB;
  function DESERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2, P_TABLE_NAME VARCHAR2) return CLOB;
  function DESERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2,P_TABLE_LIST XDB.XDB$STRING_LIST_T) return CLOB;

  function FUNCTION_BFILE2CHAR return VARCHAR2 deterministic;
  function FUNCTION_BLOB2BASE64 return VARCHAR2 deterministic;
  function FUNCTION_BLOB2HEXBINARY return VARCHAR2 deterministic;

  function FUNCTION_CHAR2BFILE return VARCHAR2 deterministic;
  -- function FUNCTION_BLOB2BASE64 return VARCHAR2 deterministic;
  function FUNCTION_HEXBINARY2BLOB return VARCHAR2 deterministic;
  
  function PROCEDURE_CHECK_SIZE return VARCHAR2 deterministic;
  function PROCEDURE_SERIALIZE_ANYDATA return VARCHAR2 deterministic;
  function FUNCTION_SERIALIZE_OBJECT return VARCHAR2 deterministic;
end;
/
show errors;
--
create or replace package body OBJECT_SERIALIZATION
as
--
  C_NEWLINE CONSTANT CHAR(1) := CHR(10);
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
function PROCEDURE_CHECK_SIZE
return VARCHAR2
deterministic
as
  C_PROCEDURE CONSTANT VARCHAR2(512)   := 'procedure SIZE_CHECK(P_CLOB IN OUT NOCOPY CLOB, P_SOURCE_DETAILS VARCHAR2)' || C_NEWLINE
                                       ||'as' || C_NEWLINE
                                       ||'begin' || C_NEWLINE
									   $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
									   ||'NULL;' || C_NEWLINE
                                       $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
									   ||' if (DBMS_LOB.GETLENGTH(P_CLOB) > 32767) THEN P_CLOB := ''OVERFLOW: '' || P_SOURCE_DETAILS || ''. Size ('' || DBMS_LOB.GETLENGTH(P_CLOB) || '')  exceeds maximum supported by source database [VARCHAR2(32767)].''; end if;'|| C_NEWLINE
									   $ELSE
									   ||' if (DBMS_LOB.GETLENGTH(P_CLOB) > 4000) THEN P_CLOB := ''OVERFLOW: '' || P_SOURCE_DETAILS || ''. Size ('' || DBMS_LOB.GETLENGTH(P_CLOB) || '')  exceeds maximum supported by source database [VARCHAR2(4000)].''; end if;'|| C_NEWLINE
				                       $END
									   ||'end;' || C_NEWLINE;
begin
  return C_PROCEDURE;
end;
--
function FUNCTION_BFILE2CHAR
return VARCHAR2
deterministic
as
  C_FUNCTION  CONSTANT VARCHAR2(512)   := 'function BFILE2CHAR(P_BFILE BFILE) return VARCHAR2' || C_NEWLINE
                                       || 'as' || C_NEWLINE
                                       || '  V_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);' || C_NEWLINE
                                       || '  V_DIRECTORY_ALIAS  VARCHAR2(128 CHAR);' || C_NEWLINE
                                       || '  V_PATH2FILE        VARCHAR2(2000 CHAR);' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
                                       || '  DBMS_LOB.FILEGETNAME(P_BFILE,V_DIRECTORY_ALIAS,V_PATH2FILE);' || C_NEWLINE
                                       || '  return ''BFILENAME('' || V_SINGLE_QUOTE || V_DIRECTORY_ALIAS || V_SINGLE_QUOTE || '','' || V_SINGLE_QUOTE || V_PATH2FILE || V_SINGLE_QUOTE || '')'';' || C_NEWLINE
                                       || 'end;' || C_NEWLINE;
begin
  return C_FUNCTION;
end;
--									   
function FUNCTION_CHAR2BFILE
return VARCHAR2
deterministic
as
  C_FUNCTION  CONSTANT VARCHAR2(512)   := 'function CHAR2BFILE(P_SERIALIZATION VARCHAR2) return BFILE' || C_NEWLINE
                                       || 'as' || C_NEWLINE
                                       || '  V_BFILE BFILE;' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
                                       || '  EXECUTE IMMEDIATE ''select '' || P_SERIALIZATION || '' from dual'' into V_BFILE;' || C_NEWLINE
                                       || '  return V_BFILE;' || C_NEWLINE
                                       || 'end;' || C_NEWLINE;
begin
  return C_FUNCTION;
end;
--
function FUNCTION_BLOB2BASE64
return VARCHAR2
deterministic
as
  C_FUNCTION  CONSTANT VARCHAR2(1024)  := 'function BLOB2BASE64(P_BLOB BLOB)' || C_NEWLINE
                                       || 'return CLOB' || C_NEWLINE
                                       || 'is' || C_NEWLINE
                                       || '  V_CLOB CLOB;' || C_NEWLINE
                                       || '  V_OFFSET INTEGER := 1;' || C_NEWLINE
                                       || '  V_AMOUNT INTEGER := 2000;' || C_NEWLINE
                                       || 'V_BLOB_LENGTH NUMBER := DBMS_LOB.GETLENGTH(P_BLOB);' || C_NEWLINE
                                       || '  V_RAW_DATA RAW(2000);' || C_NEWLINE
                                       || '  V_BASE64_DATA VARCHAR2(32767);' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
                                       || '  DBMS_LOB.CREATETEMPORARY(V_CLOB,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '  while (V_OFFSET <= V_BLOB_LENGTH) loop' || C_NEWLINE
                                       || '    DBMS_LOB.READ(P_BLOB,V_AMOUNT,V_OFFSET,V_RAW_DATA);' || C_NEWLINE
                                       || '	V_OFFSET := V_OFFSET + V_AMOUNT;' || C_NEWLINE
                                       || '	V_AMOUNT := 2000;' || C_NEWLINE
                                       || '    V_BASE64_DATA := UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_ENCODE(V_RAW_DATA));' || C_NEWLINE
                                       || '    DBMS_LOB.WRITEAPPEND(V_CLOB,LENGTH(V_BASE64_DATA),V_BASE64_DATA);' || C_NEWLINE
                                       || '  end loop;' || C_NEWLINE
					        		   || '  SIZE_CHECK(V_CLOB,''BLOB to BASE64'');' || C_NEWLINE
                                       || '  return V_CLOB;' || C_NEWLINE
                                       || 'end;' || C_NEWLINE;
begin
  return C_FUNCTION;
end;
--
function FUNCTION_BLOB2HEXBINARY
return VARCHAR2
deterministic
as
  C_FUNCTION  CONSTANT VARCHAR2(1024)  := 'function BLOB2HEXBINARY(P_BLOB BLOB)' || C_NEWLINE
                                       || 'return CLOB' || C_NEWLINE
                                       || 'is' || C_NEWLINE
                                       || '  V_CLOB CLOB;' || C_NEWLINE
                                       || '  V_OFFSET INTEGER := 1;' || C_NEWLINE
                                       || '  V_AMOUNT INTEGER := 2000;' || C_NEWLINE
                                       || '  V_INPUT_LENGTH NUMBER := DBMS_LOB.GETLENGTH(P_BLOB);' || C_NEWLINE
                                       || '  V_RAW_DATA RAW(2000);' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
                                       || '  DBMS_LOB.CREATETEMPORARY(V_CLOB,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '  while (V_OFFSET <= V_INPUT_LENGTH) loop' || C_NEWLINE
                                       || '    V_AMOUNT := 2000;' || C_NEWLINE
                                       || '    DBMS_LOB.READ(P_BLOB,V_AMOUNT,V_OFFSET,V_RAW_DATA);' || C_NEWLINE
                                       || '	   V_OFFSET := V_OFFSET + V_AMOUNT;' || C_NEWLINE
                                       || '    DBMS_LOB.APPEND(V_CLOB,TO_CLOB(RAWTOHEX(V_RAW_DATA)));' || C_NEWLINE
                                       || '  end loop;' || C_NEWLINE
					        		   || '  SIZE_CHECK(V_CLOB,''BLOB to HEXBINARY'');' || C_NEWLINE
                                       || '  return V_CLOB;' || C_NEWLINE
                                       || 'end;' || C_NEWLINE;
begin
  return C_FUNCTION;
end;
--
function FUNCTION_HEXBINARY2BLOB
return VARCHAR2
deterministic
as
  C_FUNCTION  CONSTANT VARCHAR2(1024)  := 'function HEXBINARY2BLOB(P_SERIALIZATION CLOB)' || C_NEWLINE
                                       || 'return BLOB' || C_NEWLINE
                                       || 'is' || C_NEWLINE
                                       || '  V_BLOB BLOB;' || C_NEWLINE
                                       || '  V_OFFSET INTEGER := 1;' || C_NEWLINE
                                       || '  V_AMOUNT INTEGER := 32000;' || C_NEWLINE
                                       || '  V_INPUT_LENGTH NUMBER := DBMS_LOB.GETLENGTH(P_SERIALIZATION);' || C_NEWLINE
                                       || '  V_HEXBINARY_DATA VARCHAR2(32000);' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
									   || '  if (DBMS_LOB.substr(P_SERIALIZATION,8,1) = ''OVERFLOW'') then return NULL; end if;' || C_NEWLINE
                                       || '  DBMS_LOB.CREATETEMPORARY(V_BLOB,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '  while (V_OFFSET <= V_INPUT_LENGTH) loop' || C_NEWLINE
                                       || '    V_AMOUNT := 32000;' || C_NEWLINE
                                       || '    DBMS_LOB.READ(P_SERIALIZATION,V_AMOUNT,V_OFFSET,V_HEXBINARY_DATA);' || C_NEWLINE
                                       || '	   V_OFFSET := V_OFFSET + V_AMOUNT;' || C_NEWLINE
                                       || '    DBMS_LOB.APPEND(V_BLOB,TO_BLOB(HEXTORAW(V_HEXBINARY_DATA)));' || C_NEWLINE
                                       || '  end loop;' || C_NEWLINE
                                       || '  return V_BLOB;' || C_NEWLINE
                                       || 'end;' || C_NEWLINE;
begin
  return C_FUNCTION;
end;
--
function PROCEDURE_SERIALIZE_ANYDATA
return VARCHAR2
deterministic
--
-- Cannot handle OBJECT TYPES as it is necessary to create a variable of the type in order to get payaload, which requires the TYPES to be known in advance.
-- Also cannot handle types with certain oracle data types such as INTEGER, since the ANYTYPE does not support ''GETTERS'' for these data types.
-- Obvious solution to handle these cases using Dynamic SQL does not work for nested objects as context for the ''PIECEWISE'' operations is lost when the
-- ANYDATA object is passed to EXECUTE IMMEDIATE
-- Also the ''GETTER'' for ROWID appears to be missing ???
--
as
  C_PROCEDURE CONSTANT VARCHAR2(32767) := 'procedure SERIALIZE_ANYDATA(P_ANYDATA ANYDATA, P_SERIALIZATION IN OUT NOCOPY CLOB)' || C_NEWLINE
                                       || 'as' || C_NEWLINE
                                       || '  V_SINGLE_QUOTE         CONSTANT CHAR(1) := CHR(39);' || C_NEWLINE
                                       || '  TYPE TYPE_INFO_T is RECORD (' || C_NEWLINE
                                       || '    TYPE_ID              NUMBER' || C_NEWLINE
                                       || '   ,PRECISION            NUMBER' || C_NEWLINE
                                       || '   ,SCALE                NUMBER' || C_NEWLINE
                                       || '   ,LENGTH               NUMBER' || C_NEWLINE
                                       || '   ,CSID                 NUMBER' || C_NEWLINE
                                       || '   ,CSFRM                NUMBER' || C_NEWLINE
                                       || '   ,SCHEMA_NAME          VARCHAR2(128)' || C_NEWLINE
                                       || '   ,TYPE_NAME            VARCHAR2(128)' || C_NEWLINE
                                       || '   ,TYPE_VERSION         VARCHAR2(128)' || C_NEWLINE
                                       || '   ,ATTR_COUNT           NUMBER' || C_NEWLINE
                                       || '  );' || C_NEWLINE
                                       || '  V_TYPE_ID              NUMBER;' || C_NEWLINE
                                       || '  V_TYPE_METADATA        ANYTYPE;' || C_NEWLINE
                                       || '  V_TYPE_INFO            TYPE_INFO_T;' || C_NEWLINE
                                       || '  V_OBJECT_CONSTRUCTOR   VARCHAR2(512);' || C_NEWLINE
                                       || '  V_ANYDATA_CONTENT_TYPE VARCHAR2(512);' || C_NEWLINE
                                       || '  V_RESULT               PLS_INTEGER;' || C_NEWLINE
                                       || '  V_ANYDATA_PAYLOAD      CLOB;' || C_NEWLINE
                                       || '  V_CLOB                 CLOB;' || C_NEWLINE
                                       || '  V_BLOB                 BLOB;' || C_NEWLINE
                                       || '  V_NCLOB                NCLOB;' || C_NEWLINE
                                       || '  V_BFILE                BFILE;' || C_NEWLINE
                                       || '  V_BDOUBLE              BINARY_DOUBLE;' || C_NEWLINE
                                       || '  V_BFLOAT               BINARY_FLOAT;' || C_NEWLINE
                                       || '  V_NUMBER               NUMBER;' || C_NEWLINE
                                       || '  V_CHAR                 CHAR(4000);' || C_NEWLINE
                                       || '  V_NCHAR                NCHAR(4000);' || C_NEWLINE
                                       || '  V_VARCHAR              VARCHAR(32767);' || C_NEWLINE
                                       || '  V_VARCHAR2             VARCHAR2(32767);' || C_NEWLINE
                                       || '  V_NVARCHAR2            NVARCHAR2(32767);' || C_NEWLINE
                                       || '  V_RAW                  RAW(32767);' || C_NEWLINE
                                       || '  V_DATE                 DATE;' || C_NEWLINE
                                       || '  V_INTERVAL_DS          INTERVAL DAY TO SECOND;' || C_NEWLINE
                                       || '  V_INTERVAL_YM          INTERVAL YEAR TO MONTH;' || C_NEWLINE
                                       || '  V_TIMESTAMP            TIMESTAMP;' || C_NEWLINE
                                       || '  V_TIMESTAMP_TZ         TIMESTAMP WITH TIME ZONE;' || C_NEWLINE
                                       || '  V_TIMESTAMP_LTZ        TIMESTAMP WITH LOCAL TIME ZONE;' || C_NEWLINE
                                       || '  V_UROWID               UROWID;' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
                                       || '  if (P_ANYDATA is NULL) then' || C_NEWLINE
                                       || '    DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || C_NEWLINE
                                       || '    return;' || C_NEWLINE
                                       || '  end if;' || C_NEWLINE
                                       || '  V_TYPE_ID := P_ANYDATA.getType(V_TYPE_METADATA);' || C_NEWLINE
                                       || '  V_TYPE_INFO.TYPE_ID := V_TYPE_METADATA.getInfo(' || C_NEWLINE
                                       || '    V_TYPE_INFO.PRECISION,' || C_NEWLINE
                                       || '    V_TYPE_INFO.SCALE,' || C_NEWLINE
                                       || '    V_TYPE_INFO.LENGTH,' || C_NEWLINE
                                       || '    V_TYPE_INFO.CSID,' || C_NEWLINE
                                       || '    V_TYPE_INFO.CSFRM,' || C_NEWLINE
                                       || '    V_TYPE_INFO.SCHEMA_NAME,' || C_NEWLINE
                                       || '    V_TYPE_INFO.TYPE_NAME,' || C_NEWLINE
                                       || '    V_TYPE_INFO.TYPE_VERSION,' || C_NEWLINE
                                       || '    V_TYPE_INFO.ATTR_COUNT' || C_NEWLINE
                                       || '  );' || C_NEWLINE
                                       || '  V_OBJECT_CONSTRUCTOR := ''"SYS"."ANYDATA"('';' || C_NEWLINE
                                       || '  V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || V_TYPE_INFO.SCHEMA_NAME || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '  case V_TYPE_ID' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_BDOUBLE then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getBDouble(V_BDOUBLE);' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := TO_CHAR(V_BDOUBLE);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_BFILE then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getBFile(V_BFILE);' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := BFILE2CHAR(V_BFILE);' || C_NEWLINE
                                       || '	when DBMS_TYPES.TYPECODE_BFLOAT then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getBFloat(V_BFLOAT);' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := TO_CHAR(V_BFLOAT);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_BLOB then' || C_NEWLINE
                                       || '	  V_RESULT := P_ANYDATA.getBLOB(V_BLOB);' || C_NEWLINE
                                       || '      DBMS_LOB.CREATETEMPORARY(V_ANYDATA_PAYLOAD,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '      DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);' || C_NEWLINE
                                       || '	  DBMS_LOB.APPEND(V_ANYDATA_PAYLOAD,BLOB2HEXBINARY(V_BLOB));' || C_NEWLINE
                                       || '	  DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_CHAR then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getCHAR(V_CHAR);' || C_NEWLINE
                                       || '    V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || V_CHAR || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_CLOB then' || C_NEWLINE
                                       || '	  V_RESULT := P_ANYDATA.getCLOB(V_CLOB);' || C_NEWLINE
                                       || '      DBMS_LOB.CREATETEMPORARY(V_ANYDATA_PAYLOAD,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '	  DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);' || C_NEWLINE
                                       || '	  DBMS_LOB.APPEND(V_ANYDATA_PAYLOAD,V_CLOB);' || C_NEWLINE
                                       || '	  DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_DATE then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getDate(V_DATE);' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := TO_CHAR(V_DATE);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_INTERVAL_DS then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getIntervalDS(V_INTERVAL_DS);' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := TO_CHAR(V_INTERVAL_DS);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_INTERVAL_YM then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getIntervalYM(V_INTERVAL_YM);' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := TO_CHAR(V_INTERVAL_YM);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_NCHAR then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getNCHAR(V_NCHAR);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_NCHAR) || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_NCLOB then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getNCLOB(V_NCLOB);' || C_NEWLINE
                                       || '      DBMS_LOB.CREATETEMPORARY(V_ANYDATA_PAYLOAD,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '	  DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);' || C_NEWLINE
                                       || '	  DBMS_LOB.APPEND(V_ANYDATA_PAYLOAD,TO_CLOB(V_NCLOB));' || C_NEWLINE
                                       || '	  DBMS_LOB.WRITEAPPEND(V_ANYDATA_PAYLOAD,1,V_SINGLE_QUOTE);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_NUMBER then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getNumber(V_NUMBER);' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := TO_CHAR(V_NUMBER);' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_NVARCHAR2 then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getNVarchar2(V_NVARCHAR2);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_NVARCHAR2) || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_RAW then' || C_NEWLINE
                                       || '	  V_RESULT := P_ANYDATA.getRaw(V_RAW);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_RAW) || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_TIMESTAMP then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getTimestamp(V_TIMESTAMP);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP) || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getTimestampLTZ(V_TIMESTAMP_LTZ);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP_LTZ) || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getTimestampLTZ(V_TIMESTAMP_TZ);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP_TZ) || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_UROWID then' || C_NEWLINE
                                       || '      -- V_RESULT := P_ANYDATA.getROWID(V_ROWID);' || C_NEWLINE
                                       || '	  V_UROWID := P_ANYDATA.accessUROWID();' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || TO_CHAR(V_UROWID) || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_VARCHAR2 then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getVARCHAR2(V_VARCHAR2);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || V_VARCHAR2 || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_VARCHAR then' || C_NEWLINE
                                       || '      V_RESULT := P_ANYDATA.getVARCHAR(V_VARCHAR);' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD :=  V_SINGLE_QUOTE || V_VARCHAR || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '    when 3 then -- INTERGER used in ORDSYS.ORDIMAGE' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: INTEGER.'';' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_CFILE then' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: CFILE.'';' || C_NEWLINE
                                       || '   when DBMS_TYPES.TYPECODE_MLSLABEL then' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: MSLABEL.'';' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then' || C_NEWLINE
                                       || '      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: NAMEDCOLLECTION.'';' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_OBJECT then' || C_NEWLINE
                                       || '      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: OBJECT.'';' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_OPAQUE then' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: OPAQUE.'';' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_REF then' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: REF.'';' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_TABLE then' || C_NEWLINE
                                       || '      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: TABLE.'';' || C_NEWLINE
                                       || '    when DBMS_TYPES.TYPECODE_VARRAY then' || C_NEWLINE
                                       || '      V_ANYDATA_CONTENT_TYPE := V_SINGLE_QUOTE || ''"'' || V_TYPE_INFO.SCHEMA_NAME || ''"."'' || V_TYPE_INFO.TYPE_NAME || ''"'' || V_SINGLE_QUOTE;' || C_NEWLINE
                                       || '      V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: VARRAY.'';' || C_NEWLINE
                                       || '	else' || C_NEWLINE
                                       || '	  V_ANYDATA_PAYLOAD := ''Unsupported ANYDATA content [''|| V_TYPE_ID || '']: UNKNOWN.'';' || C_NEWLINE
                                       || '  end case;' || C_NEWLINE
                                       || '  DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_ANYDATA_CONTENT_TYPE),V_ANYDATA_CONTENT_TYPE);' || C_NEWLINE
                                       || '  DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'','');' || C_NEWLINE
                                       || '  SIZE_CHECK(V_ANYDATA_PAYLOAD,''SERIALIZE ANYDATA'');' || C_NEWLINE
                                       || '  DBMS_LOB.APPEND(P_SERIALIZATION,V_ANYDATA_PAYLOAD);' || C_NEWLINE
                                       || '  DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'')'');' || C_NEWLINE
                                       || 'end;' || C_NEWLINE
									   || 'function SERIALIZE_ANYDATA(P_ANYDATA ANYDATA) return CLOB' || C_NEWLINE
                                       || 'as' || C_NEWLINE
                                       || '  V_SERIALIZATION CLOB;' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
                                       || '  DBMS_LOB.CREATETEMPORARY(V_SERIALIZATION,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '  SERIALIZE_ANYDATA(P_ANYDATA,V_SERIALIZATION);' || C_NEWLINE
                                       || '  return V_SERIALIZATION;' || C_NEWLINE
                                       || 'end;' || CHR(13) || C_NEWLINE;
begin
  return C_PROCEDURE;
end;
--
function FUNCTION_SERIALIZE_OBJECT
return VARCHAR2
deterministic
as
  C_FUNCTION  CONSTANT VARCHAR(512)    := 'function SERIALIZE_OBJECT(P_ANYDATA ANYDATA) return CLOB' || C_NEWLINE
                                       || 'as' || C_NEWLINE
                                       || '  V_SERIALIZATION CLOB;' || C_NEWLINE
                                       || 'begin' || C_NEWLINE
                                       || '  DBMS_LOB.CREATETEMPORARY(V_SERIALIZATION,TRUE,DBMS_LOB.CALL);' || C_NEWLINE
                                       || '  SERIALIZE_OBJECT(P_ANYDATA,V_SERIALIZATION);' || C_NEWLINE
                                       || '  return V_SERIALIZATION;' || C_NEWLINE
                                       || 'end;' || CHR(13) || C_NEWLINE;
begin
  return C_FUNCTION;
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
                  || '         DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'BFILE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := BFILE2CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'BINARY FLOAT' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'BLOB' then
          V_PLSQL := V_PLSQL
		          ||'          if (' || P_ATTR_NAME || ' is NULL) then' || C_NEWLINE
				  ||'            DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || C_NEWLINE
		          ||'          else' || C_NEWLINE
                  ||'            DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'            DBMS_LOB.APPEND(P_SERIALIZATION,BLOB2HEXBINARY(' || P_ATTR_NAME || '));' || C_NEWLINE
                  ||'            DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
		          ||'          end if;' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = CFILE then
        when P_ATTR_TYPE_NAME = 'CHAR' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || ' || P_ATTR_NAME || ' || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'CLOB' then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.APPEND(P_SERIALIZATION,' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'DATE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'INTERVAL DAY TO SECOND' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'INTEGER' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'INTERVAL YEAR TO MONTH' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = MLSLABEL then
        when P_ATTR_TYPE_NAME = 'NCHAR' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'NCLOB' then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.APPEND(P_SERIALIZATION,TO_CLOB(' || P_ATTR_NAME || '));' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'NUMBER' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'NVARCHAR2' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = OPAQUE then
        when P_ATTR_TYPE_NAME = 'RAW' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        --  when P_ATTR_TYPE_NAME = REF then
        when P_ATTR_TYPE_NAME = 'TIMESTAMP' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'TIMESTAMP WITH LOCAL TIME ZONE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'TIMESTAMP WITH TIME ZONE' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'UROWID' then
          V_PLSQL := V_PLSQL
                  ||'          V_SERIALIZED_VALUE := TO_CHAR(' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'VARCHAR2' then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(' || P_ATTR_NAME || '),' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        when P_ATTR_TYPE_NAME = 'VARCHAR'  then
          V_PLSQL := V_PLSQL
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(' || P_ATTR_NAME || '),' || P_ATTR_NAME || ');' || C_NEWLINE
                  ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || C_NEWLINE;
        else
          DBMS_OUTPUT.PUT_LINE('Unsupported Type: "' || P_ATTR_TYPE_NAME || '".');
      end case;
    when P_ATTR_TYPE_MOD = 'REF' then
	  -- The serialzied form needs to include the decode function eg "HEXTOREF(' || REF_VALUE || ')" to that it will be correctly deserialized 
	  V_PLSQL := V_PLSQL
              ||'          -- V_SERIALIZED_VALUE := REFTOHEX(' || P_ATTR_NAME || ');' || C_NEWLINE
		      ||'          select ''HEXTOREF('' || V_SINGLE_QUOTE || REFTOHEX(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE || '')'' into V_SERIALIZED_VALUE from dual;' || C_NEWLINE
              ||'          DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
    when P_ATTR_TYPE_OWNER is NOT NULL then
      V_TYPECODE := extendTypeList(P_TYPE_LIST, P_ATTR_TYPE_OWNER, P_ATTR_TYPE_NAME);
      $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('Adding "' || P_ATTR_TYPE_OWNER || '"."' || P_ATTR_TYPE_NAME || '": Type = "' || V_TYPECODE || '". Type count = ' || P_TYPE_LIST.count); $END
      case
        when V_TYPECODE = 'COLLECTION' then
          V_PLSQL := V_PLSQL
                  ||'          V_ANYDATA := ANYDATA.convertCollection(' || P_ATTR_NAME || ');' || C_NEWLINE;
        when V_TYPECODE = 'OBJECT' then
          V_PLSQL := V_PLSQL
                  ||'          V_ANYDATA := ANYDATA.convertObject(' || P_ATTR_NAME || ');' || C_NEWLINE;
      end case;
      V_PLSQL := V_PLSQL
              || '          serialize_Object(V_ANYDATA,P_SERIALIZATION);' || C_NEWLINE;
    else
      V_PLSQL := V_PLSQL
              ||'           V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(' || P_ATTR_NAME || ') || V_SINGLE_QUOTE;' || C_NEWLINE
              ||'           DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || C_NEWLINE;
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
  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('serializeType() : Processing Type: "' || P_TYPE_RECORD.OWNER || '"."' || P_TYPE_RECORD.TYPE_NAME || '".'); $END
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
          || '        if (V_TYPE_INFO.OWNER = SYS_CONTEXT(''USERENV'',''CURRENT_SCHEMA'')) then' || C_NEWLINE
          || '          V_OBJECT_CONSTRUCTOR := ''"'|| P_TYPE_RECORD.TYPE_NAME || '"('';'|| C_NEWLINE
		  || '        else' || C_NEWLINE
          || '          V_OBJECT_CONSTRUCTOR := ''"' || P_TYPE_RECORD.OWNER || '"."' || P_TYPE_RECORD.TYPE_NAME || '"('';' || C_NEWLINE
          || '        end if; ' || C_NEWLINE
          || '        DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT_CONSTRUCTOR),V_OBJECT_CONSTRUCTOR);' || C_NEWLINE;

  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);

  if P_TYPE_RECORD.TYPECODE = 'OBJECT' then

    for a in getAttributes loop

      $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('serializeType() : Processing Attribute: ' || a.ATTR_NAME || '. Data Type: ' || a.ATTR_TYPE_NAME); $END

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

      DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);

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
    DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);
  end if;

  V_PLSQL := '      end;' || C_NEWLINE;
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);

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
  V_PLSQL_BLOCK := PROCEDURE_CHECK_SIZE
                || FUNCTION_BFILE2CHAR
                || FUNCTION_BLOB2HEXBINARY
                || PROCEDURE_SERIALIZE_ANYDATA
                || 'procedure SERIALIZE_OBJECT(P_ANYDATA ANYDATA, P_SERIALIZATION IN OUT NOCOPY CLOB)' || C_NEWLINE
                || 'as' || C_NEWLINE
                || '  V_SINGLE_QUOTE        CONSTANT CHAR(1) := CHR(39);' || C_NEWLINE
                || '  TYPE TYPE_INFO_T is RECORD (' || C_NEWLINE
                || '    TYPE_ID             NUMBER' || C_NEWLINE
                || '   ,PRECISION           NUMBER' || C_NEWLINE
                || '   ,SCALE               NUMBER' || C_NEWLINE
                || '   ,LENGTH              NUMBER' || C_NEWLINE
                || '   ,CSID                NUMBER' || C_NEWLINE
                || '   ,CSFRM               NUMBER' || C_NEWLINE
                || '   ,OWNER               VARCHAR2(128)' || C_NEWLINE
                || '   ,TYPE_NAME           VARCHAR2(128)' || C_NEWLINE
                || '   ,TYPE_VERSION        VARCHAR2(128)' || C_NEWLINE
                || '   ,ATTR_COUNT          NUMBER' || C_NEWLINE
                || '  );' || C_NEWLINE
                || '  TYPE ATTR_INFO_T IS RECORD (' || C_NEWLINE
                || '    TYPE_ID             NUMBER' || C_NEWLINE
                || '   ,PRECISION           NUMBER' || C_NEWLINE
                || '   ,SCALE               NUMBER' || C_NEWLINE
                || '   ,LENGTH              NUMBER' || C_NEWLINE
                || '   ,CSID                NUMBER' || C_NEWLINE
                || '   ,CSFRM               NUMBER' || C_NEWLINE
                || '   ,ATTR_TYPE_METADATA  ANYTYPE' || C_NEWLINE
                || '   ,ATTR_NAME           VARCHAR2(128)' || C_NEWLINE
                || '  );' || C_NEWLINE
                || '  V_TYPE_ID             PLS_INTEGER;' || C_NEWLINE
                || '  V_ANYDATA             ANYDATA;' || C_NEWLINE
                || '  V_TYPE_METADATA       ANYTYPE;' || C_NEWLINE
                || '  V_TYPE_INFO           TYPE_INFO_T;' || C_NEWLINE
                || '  V_ATTR_INFO           ATTR_INFO_T;' || C_NEWLINE
                || '  V_RESULT              PLS_INTEGER;' || C_NEWLINE
                || '  V_OBJECT_CONSTRUCTOR  VARCHAR2(266);' || C_NEWLINE
                || '  V_SERIALIZED_VALUE    VARCHAR2(266);' || C_NEWLINE
                || 'begin' || C_NEWLINE
                || '  V_TYPE_ID := P_ANYDATA.getType(V_TYPE_METADATA);' || C_NEWLINE
                || '  V_TYPE_INFO.TYPE_ID := V_TYPE_METADATA.getInfo(' || C_NEWLINE
                || '    V_TYPE_INFO.PRECISION,' || C_NEWLINE
                || '    V_TYPE_INFO.SCALE,' || C_NEWLINE
                || '    V_TYPE_INFO.LENGTH,' || C_NEWLINE
                || '    V_TYPE_INFO.CSID,' || C_NEWLINE
                || '    V_TYPE_INFO.CSFRM,' || C_NEWLINE
                || '    V_TYPE_INFO.OWNER,' || C_NEWLINE
                || '    V_TYPE_INFO.TYPE_NAME,' || C_NEWLINE
                || '    V_TYPE_INFO.TYPE_VERSION,' || C_NEWLINE
                || '    V_TYPE_INFO.ATTR_COUNT' || C_NEWLINE
                || '  );' || C_NEWLINE
                || '  case' || C_NEWLINE;


  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.serializeTypes(): Type count = ' || P_TYPE_LIST.count); $END

  if (P_TYPE_LIST.count = 0) then
    return NULL;
  end if;

  loop
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.serializeTypes() : Processing[' || V_IDX || '].'); $END
    V_CASE_BLOCK := serializeType(P_TYPE_LIST(V_IDX),P_TYPE_LIST);
    DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_CASE_BLOCK);
    DBMS_LOB.FREETEMPORARY(V_CASE_BLOCK);
    exit when (V_IDX = P_TYPE_LIST.count);
    V_IDX := V_IDX + 1;
  end loop;

  V_SQL_FRAGMENT := '  end case;' || C_NEWLINE
                 || 'end;' || C_NEWLINE;

  DBMS_LOB.writeAppend(V_PLSQL_BLOCK,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

  DBMS_LOB.writeAppend(V_PLSQL_BLOCK,LENGTH(FUNCTION_SERIALIZE_OBJECT),FUNCTION_SERIALIZE_OBJECT);

  return V_PLSQL_BLOCK;

end;
--
function SERIALIZE_TYPE(P_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
   select OWNER, TYPE_NAME, ATTRIBUTES, TYPECODE
     bulk collect into V_TYPE_LIST
     from ALL_TYPES
          start with OWNER = P_OWNER and TYPE_NAME = P_TYPE_NAME
          connect by prior TYPE_NAME = SUPERTYPE_NAME
                       and OWNER = SUPERTYPE_OWNER;
  return serializeTypes(V_TYPE_LIST);
end;
--
function SERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2,P_TABLE_NAME VARCHAR2)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
  select OWNER, TYPE_NAME, ATTRIBUTES, TYPECODE
    bulk collect into V_TYPE_LIST
  from ALL_TYPES at,
       (
         select distinct DATA_TYPE_OWNER,  DATA_TYPE
           from ALL_TAB_COLS atc
          where atc.DATA_TYPE_OWNER is not NULL
            and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
	        and (
	             ((atc.HIDDEN_COLUMN = 'NO') and (atc.VIRTUAL_COLUMN = 'NO'))
                 or 
		         ((atc.HIDDEN_COLUMN = 'YES') and (atc.VIRTUAL_COLUMN = 'YES') and (COLUMN_NAME ='SYS_NC_ROWINFO$'))
		        )       
            and atc.OWNER = P_OWNER
            and atc.TABLE_NAME = P_TABLE_NAME
       ) tlt
       start with at.TYPE_NAME = tlt.DATA_TYPE
              and at.OWNER = tlt.DATA_TYPE_OWNER
                  connect by prior at.TYPE_NAME = SUPERTYPE_NAME
                               and at.OWNER = SUPERTYPE_OWNER;

  return serializeTypes(V_TYPE_LIST);
end;
--
function SERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2,P_TABLE_LIST XDB.XDB$STRING_LIST_T)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
  select OWNER, TYPE_NAME, ATTRIBUTES, TYPECODE
    bulk collect into V_TYPE_LIST
  from ALL_TYPES at,
       (
         select distinct DATA_TYPE_OWNER,  DATA_TYPE
           from ALL_TAB_COLS atc, TABLE(P_TABLE_LIST) tl
          where atc.DATA_TYPE_OWNER is not NULL
            and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
	        and (
	             ((atc.HIDDEN_COLUMN = 'NO') and (atc.VIRTUAL_COLUMN = 'NO'))
                 or 
		         ((atc.HIDDEN_COLUMN = 'YES') and (atc.VIRTUAL_COLUMN = 'YES') and (COLUMN_NAME ='SYS_NC_ROWINFO$'))
		        )       
            and atc.OWNER = P_OWNER
            and atc.TABLE_NAME = tl.COLUMN_VALUE
       ) tlt
       start with at.TYPE_NAME = tlt.DATA_TYPE
              and at.OWNER = tlt.DATA_TYPE_OWNER
                  connect by prior at.TYPE_NAME = SUPERTYPE_NAME
                               and at.OWNER = SUPERTYPE_OWNER;

  return serializeTypes(V_TYPE_LIST);
end;
--
function deserializeTypes(P_TYPE_LIST IN OUT NOCOPY TYPE_LIST_TAB)
return CLOB
as
--
  V_PLSQL_BLOCK    CLOB;
  V_SQL_FRAGMENT   VARCHAR2(32767);
  V_IDX            PLS_INTEGER := 1;

  V_TYPE_REFERENCE VARCHAR2(266);
  
begin

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.deserializeTypes(): Type count = ' || P_TYPE_LIST.count); $END
 
  if (P_TYPE_LIST.count = 0) then
    return NULL;
  end if;
  
  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(FUNCTION_CHAR2BFILE),FUNCTION_CHAR2BFILE);
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,length(FUNCTION_HEXBINARY2BLOB),FUNCTION_HEXBINARY2BLOB);
  for V_IDX in 1 .. P_TYPE_LIST.count loop
    if (P_TYPE_LIST(V_IDX).OWNER = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')) then
      V_TYPE_REFERENCE := '"' || P_TYPE_LIST(V_IDX).TYPE_NAME || '"';
    else 
      V_TYPE_REFERENCE := '"' || P_TYPE_LIST(V_IDX).OWNER|| '"."' || P_TYPE_LIST(V_IDX).TYPE_NAME || '"';
    end if;

    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.deserializeTypes() : Processing[' || V_IDX || '].'); $END

	V_SQL_FRAGMENT := 'function "#' || P_TYPE_LIST(V_IDX).TYPE_NAME || '"(P_SERIALIZATION CLOB)' || C_NEWLINE
	               || 'return ' ||  V_TYPE_REFERENCE || C_NEWLINE
				   || 'as' || C_NEWLINE
				   || '   V_OBJECT ' || V_TYPE_REFERENCE ||';' || C_NEWLINE
				   || 'begin' || C_NEWLINE
				   || '  if (P_SERIALIZATION is NULL) then return NULL; end if;' || C_NEWLINE   
				   || '  EXECUTE IMMEDIATE ''SELECT '' || P_SERIALIZATION || '' FROM DUAL'' into V_OBJECT;' || C_NEWLINE
				   || '  return V_OBJECT;' || C_NEWLINE
				   || 'end;' || C_NEWLINE;
				
    DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  end loop;

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('OBJECT_SERIALIZATION.deserializeTypes(): Size = ' || DBMS_LOB.GETLENGTH(V_PLSQL_BLOCK)); $END

  return V_PLSQL_BLOCK;

end;
--
function DESERIALIZE_TYPE(P_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB := TYPE_LIST_TAB();
begin
  V_TYPE_LIST.extend();
  V_TYPE_LIST(1).OWNER := P_OWNER;
  V_TYPE_LIST(1).TYPE_NAME := P_TYPE_NAME;
  return deserializeTypes(V_TYPE_LIST);
end;
--
function DESERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2,P_TABLE_NAME VARCHAR2)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
  select distinct DATA_TYPE_OWNER, DATA_TYPE, NULL, NULL
    bulk collect into V_TYPE_LIST
    from ALL_TAB_COLS atc
   where atc.DATA_TYPE_OWNER is not NULL
     and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
	 and (
	      ((atc.HIDDEN_COLUMN = 'NO') and (atc.VIRTUAL_COLUMN = 'NO'))
          or 
		  ((atc.HIDDEN_COLUMN = 'YES') and (atc.VIRTUAL_COLUMN = 'YES') and (COLUMN_NAME ='SYS_NC_ROWINFO$'))
		 )       
     and atc.OWNER = P_OWNER
     and atc.TABLE_NAME = P_TABLE_NAME;
  return deserializeTypes(V_TYPE_LIST);
end;
--
function DESERIALIZE_TABLE_TYPES(P_OWNER VARCHAR2,P_TABLE_LIST XDB.XDB$STRING_LIST_T)
return CLOB
as
  V_TYPE_LIST TYPE_LIST_TAB;
begin
  select distinct DATA_TYPE_OWNER, DATA_TYPE, NULL, NULL
    bulk collect into V_TYPE_LIST
    from ALL_TAB_COLS atc, TABLE(P_TABLE_LIST) tl
   where atc.DATA_TYPE_OWNER is not NULL
     and atc.DATA_TYPE not in ('RAW','XMLTYPE','ANYDATA')
	 and (
	      ((atc.HIDDEN_COLUMN = 'NO') and (atc.VIRTUAL_COLUMN = 'NO'))
          or 
		  ((atc.HIDDEN_COLUMN = 'YES') and (atc.VIRTUAL_COLUMN = 'YES') and (COLUMN_NAME ='SYS_NC_ROWINFO$'))
		 )       
     and atc.OWNER = P_OWNER
     and atc.TABLE_NAME = tl.COLUMN_VALUE;
  return deserializeTypes(V_TYPE_LIST);
end;
--
end;
/
show errors;
--