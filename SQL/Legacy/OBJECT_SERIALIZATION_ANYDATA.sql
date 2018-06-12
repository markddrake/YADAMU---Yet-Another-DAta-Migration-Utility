set echo on
spool OBJECT_SERIALIZATION.log
--
ALTER SESSION SET PLSQL_CCFLAGS = 'DEBUG:TRUE'
/
create or replace package OBJECT_SERIALIZATION
as

  TYPE TYPE_INFO_T is RECORD (
    TYPE_ID       NUMBER
   ,PRECISION     NUMBER
   ,SCALE         NUMBER
   ,LENGTH        NUMBER
   ,CSID          NUMBER
   ,CSFRM         NUMBER
   ,SCHEMA_NAME   VARCHAR2(128)
   ,TYPE_NAME     VARCHAR2(128)
   ,TYPE_VERSION  VARCHAR2(128)
   ,ATTR_COUNT    NUMBER
  );
  
  TYPE TYPE_INFO_TAB is TABLE of TYPE_INFO_T;
  
  TYPE ATTR_INFO_T IS RECORD (
    TYPE_ID             NUMBER
   ,PRECISION           NUMBER
   ,SCALE               NUMBER
   ,LENGTH              NUMBER
   ,CSID                NUMBER
   ,CSFRM               NUMBER
   ,ATTR_TYPE_METADATA  ANYTYPE
   ,ATTR_NAME           VARCHAR2(128)
  );
  
  TYPE ATTR_INFO_TAB is table of ATTR_INFO_T;
  
  function serializeObject(P_OBJECT ANYDATA) return CLOB;
  function getTypeInfo(P_OBJECT ANYDATA) return TYPE_INFO_TAB PIPELINED;
  function getAttrTypeInfo(P_OBJECT ANYDATA) return ATTR_INFO_TAB PIPELINED;
end;
/
show errors;
--
create or replace package body OBJECT_SERIALIZATION
as

  G_OBJECT_SERIALIZATION CLOB;
  
function getTypeInfoRecord(P_TYPE_METADATA ANYTYPE)
return TYPE_INFO_T
as
  V_TYPE_INFO      TYPE_INFO_T;
begin
  V_TYPE_INFO.TYPE_ID := P_TYPE_METADATA.getInfo(
    V_TYPE_INFO.PRECISION,
    V_TYPE_INFO.SCALE,
    V_TYPE_INFO.LENGTH,
    V_TYPE_INFO.CSID,
    V_TYPE_INFO.CSFRM,
    V_TYPE_INFO.SCHEMA_NAME,
    V_TYPE_INFO.TYPE_NAME,
    V_TYPE_INFO.TYPE_VERSION,
    V_TYPE_INFO.ATTR_COUNT
  );
  return V_TYPE_INFO;
end;
--
function getAttrTypeInfoRecord(P_TYPE_METADATA ANYTYPE,P_ATTR_IDX NUMBER)
return ATTR_INFO_T
as
  a      ATTR_INFO_T;
begin
  a.TYPE_ID := P_TYPE_METADATA.GETATTRELEMINFO(
       P_ATTR_IDX,
       a.PRECISION,
       a.SCALE,
       a.LENGTH,
       a.CSID,
       a.CSFRM,
       a.ATTR_TYPE_METADATA,
       a.ATTR_NAME
    );
    return a;
end;
--
function object2AnyData(P_OBJECT IN OUT NOCOPY ANYDATA, P_SCHEMA_NAME VARCHAR2, P_TYPE_NAME VARCHAR2) return ANYDATA
as
  V_PLSQL_BLOCK VARCHAR2(4000) := 'declare V_RESULT PLS_INTEGER; V_ATTR "' || P_SCHEMA_NAME || '"."' || P_TYPE_NAME || '"; begin V_RESULT := ANYDATA.getObject(:1,V_ATTR); :2 := ANYDATA.convertObject(V_ATTR); end;';
  V_ATTR_ANYDATA ANYDATA;
begin
  $IF $$DEBUG $THEN 
  DBMS_OUTPUT.PUT_LINE('object2AnyData: PL/SQL = ' || V_PLSQL_BLOCK);
  $END
  EXECUTE IMMEDIATE V_PLSQL_BLOCK using IN OUT P_OBJECT, OUT V_ATTR_ANYDATA;
  return V_ATTR_ANYDATA;
end;
--
function hasNestedObjects(P_TYPE_METADATA ANYTYPE, P_ATTR_COUNT NUMBER) return BOOLEAN
as
  a ATTR_INFO_T;
  
begin
  for V_ATTR_IDX in 1 .. P_ATTR_COUNT loop
    a := getAttrTypeInfoRecord(P_TYPE_METADATA,V_ATTR_IDX);
    if a.TYPE_ID in (DBMS_TYPES.TYPECODE_OBJECT, DBMS_TYPES.TYPECODE_VARRAY, DBMS_TYPES.TYPECODE_TABLE) then
      return true;
    end if;
  end loop;
  return false;
end;
--
function serializeType(P_TYPE_METADATA ANYTYPE)
/*
**
** Returns a PL/SQL 'when' block that will serialize a type using the attribute names. 
**
** Assumes that the type to be serialized is in an PL/SQL ANYDATA Variable called P_ANYDATA
** and the serialized content will be wriite to PL/SQL CLOB Variable called P_SERIALIZATION
**
**/
as
  V_TYPE_INFO        TYPE_INFO_T;

  V_PLSQL_BLOCK CLOB;
  V_ASSIGNMENT       VARCHAR2(4000);
  
  a        ATTR_INFO_T;
  a   TYPE_INFO_T;
begin
  V_TYPE_INFO := getTypeInfoRecord(P_TYPE_METADATA);
  
  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  
  V_PLSQL := '  when V_SCHEMA_NAME = ''' || V_TYPE_INFO.SCHEMA_NAME || ''' and V_TYPE_NAME = ''' || V_TYPE_INFO.TYPE_NAME || ''' then' || CHR(10)
          || '    declare' || CHR(10)
		  || '      V_OBJECT           "' || V_TYPE_INFO.SCHEMA_NAME || '","' || V_TYPE_INFO.TYPE_NAME || '";' || CHR(10)
          || '      V_DIRECTORY_ALIAS  VARCHAR2(128 CHAR);' || CHR(10)
          || '      V_PATH2FILE        VARCHAR2(2000 CHAR);' || CHR(10);
          || '    begin' || CHR(10)
          || '      V_RESULT := P_ANYDATA.getObject(V_OBJECT);' || CHR(10);
		  || '      if (V_OBJECT is NULL) then' || CHR(10);
		  || '        DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || CHR(10);
		  || '        return;' || CHR(10);
		  || '      end if; ' || CHR(10);
          || '      V_OBJECT_CONSTRUCTOR := ''"' || P_TYPE_INFO.SCHEMA_NAME || '"."'|| P_TYPE_INFO.TYPE_NAME || '"('';'|| CHR(10)
          || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT_CONSTRUCTOR),V_OBJECT_CONSTRUCTOR);' || CHR(10);
		  
  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);
  
  for V_ATTR_IDX in 1 .. P_TYPE_INFO.ATTR_COUNT loop
    a := getAttrTypeInfoRecord(P_OBJECT_METADATA,V_ATTR_IDX);
    
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('addSerializatonBlock() : Processing Attribute: ' || a.ATTR_NAME || '. Data Type: ' || a.TYPE_ID); $END
    V_PLSQL := '     if ((V_OBJECT."' || a.ATTR_NAME || '" is NULL) then'
            || '       DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || CHR(10)
            || '     else' || CHR(10);
				 
    case a.TYPE_ID 
      when DBMS_TYPES.TYPECODE_BDOUBLE then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '");' || CHR(10);      
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_BFILE then
	    V_PLSQL := V_PLSQL
	            || '      DBMS_LOB.FILEGETNAME(V_OBJECT."' || a.ATTR_NAME || '",V_DIRECTORY_ALIAS,V_PATH2FILE);' || CHR(10)
                || '      V_SERIALIZED_VALUE := ''BFILENAME('' || V_DIRECTORY_ALIAS || ''',''' || V_PATH2FILE || ''')''';' || CHR(10);
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_BFLOAT then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '");' || CHR(10);
              || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_BLOB then
      --  when DBMS_TYPES.TYPECODE_CFILE then
      when DBMS_TYPES.TYPECODE_CHAR then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || V_OBJECT."' || a.ATTR_NAME || '" || V_SINGLE_QUOTE;' || CHR(10);
              || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_CLOB then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.APPEND(P_SERIALIZATION,V_OBJECT."' || a.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_DATE then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10);
               || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_INTERVAL_DS then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10);
              || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_INTERVAL_YM then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '" || V_SINGLE_QUOTE;' || CHR(10);
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_MLSLABEL then
      when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then
         V_PLSQL := V_PLSQL
                 || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(V_OBJECT."' || a.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      when DBMS_TYPES.TYPECODE_NCHAR then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || V_OBJECT."' || a.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10);
              || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_NCLOB then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.APPEND(P_SERIALIZATION,V_OBJECT."' || a.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_NUMBER then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '");' || CHR(10);      
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
	        -- default
	  when DBMS_TYPES.TYPECODE_NVARCHAR2 then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.APPEND(P_SERIALIZATION,V_OBJECT."' || a.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_OBJECT then
        V_PLSQL := V_PLSQL
                || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertObject(V_OBJECT."' || a.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_OPAQUE then
      when DBMS_TYPES.TYPECODE_RAW then
        V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '");' || CHR(10);      
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_REF then
      when DBMS_TYPES.TYPECODE_TABLE then
        V_PLSQL := V_PLSQL
                || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(V_OBJECT."' || a.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      when DBMS_TYPES.TYPECODE_TIMESTAMP then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10);
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10);
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10);
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_UROWID then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '");' || CHR(10);      
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_VARCHAR2 then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT."' || a.ATTR_NAME || '"),V_OBJECT."' || a.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_VARCHAR  then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT."' || a.ATTR_NAME || '"),V_OBJECT."' || a.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_VARRAY then
	    V_PLSQL := V_PLSQL
                || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(V_OBJECT."' || a.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      else
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || a.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10);
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
    end case;
	 
	V_PLSQL := V_PLSQL 
    
    if (V_ATTR_IDX <  P_TYPE_INFO.ATTR_COUNT) then
      V_PLSQL := V_PLSQL
              || '      DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'','');' || CHR(10)
              || '    end if;' || CHR(10);	
    else 
      V_PLSQL := V_PLSQL
              || '      DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'')'');' || CHR(10);
              || '    end if;' || CHR(10);	
    end if;
    
    DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);  
    
  end loop;
  
  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE(V_PLSQL_BLOCK); $END
               
  return V_PLSQL_BLOCK;
  
end;
--
function serializeTypes(P_TYPE_LIST IN OUT NOCOPY ANYTYPE_LIST_T)
return CLOB
as
--
  V_PLSQL_BLOCK CLOB;
  V_CASE_BLOCK  CLOB;
  V_END_BLOCK  VARCHAR2(128);
  
begin
  V_PLSQL_BLOCK := 'procedure SERIALIZE_OBJECT(P_ANYDATA IN OUT NOCOPY ANYDATA, P_SERIALIZATION IN OUT NOCOPY CLOB)' || CHR(10)
                || 'as' || CHR(10)
				|| '  TYPE TYPE_INFO_T is RECORD (' || CHR(10)
				|| '    TYPE_ID       NUMBER' || CHR(10)
				|| '   ,PRECISION     NUMBER' || CHR(10)
				|| '   ,SCALE         NUMBER' || CHR(10)
				|| '   ,LENGTH        NUMBER' || CHR(10)
				|| '   ,CSID          NUMBER' || CHR(10)
				|| '   ,CSFRM         NUMBER' || CHR(10)
				|| '   ,SCHEMA_NAME   VARCHAR2(128)' || CHR(10)
				|| '   ,TYPE_NAME     VARCHAR2(128)' || CHR(10)
				|| '   ,TYPE_VERSION  VARCHAR2(128)' || CHR(10)
				|| '   ,ATTR_COUNT    NUMBER' || CHR(10)
				|| '  );' || CHR(10)
				|| '  ATTR_INFO_T IS RECORD (' || CHR(10)
				|| '    TYPE_ID             NUMBER' || CHR(10)
				|| '   ,PRECISION           NUMBER' || CHR(10)
				|| '   ,SCALE               NUMBER' || CHR(10)
				|| '   ,LENGTH              NUMBER' || CHR(10)
				|| '   ,CSID                NUMBER' || CHR(10)
				|| '   ,CSFRM               NUMBER' || CHR(10)
				|| '   ,ATTR_TYPE_METADATA  ANYTYPE' || CHR(10)
				|| '   ,ATTR_NAME           VARCHAR2(128)' || CHR(10)
				|| '  );' || CHR(10)
				|| '  V_TYPE_INFO          TYPE_INFO_T;' || CHR(10)
				|| '  a          ATTR_INFO_T;' || CHR(10)
                || '  V_RESULT             PLS_INTEGER;' || CHR(10)
                || '  V_SINGLE_QUOTE       CHAR(1) := CHR(39);' || CHR(10)
                || '  V_OBJECT_CONSTRUCTOR VARCHAR2(266);' || CHR(10)
				|| 'begin' || CHR(10)
                || '  V_TYPE_ID := P_ANYDATA.getType(V_TYPE_METADATA);' || CHR(10)
                || '  V_TYPE_INFO.V_TYPE_ID := V_TYPE_METADATA.getInfo(' || CHR(10)
                || '    V_TYPE_INFO.PRECISION,' || CHR(10)
                || '    V_TYPE_INFO.SCALE,' || CHR(10)
                || '    V_TYPE_INFO.LENGTH,' || CHR(10)
                || '    V_TYPE_INFO.CSID,' || CHR(10)
                || '    V_TYPE_INFO.CSFRM,' || CHR(10)
                || '    V_TYPE_INFO.SCHEMA_NAME,' || CHR(10)
                || '    V_TYPE_INFO.TYPE_NAME,' || CHR(10)
                || '    V_TYPE_INFO.TYPE_VERSION,' || CHR(10)
                || '    V_TYPE_INFO.ATTR_COUNT' || CHR(10)
                || '  );    ' || CHR(10)
                || '  case;    ' || CHR(10);
				
				
  for i in 1..P_TYPE_LIST.count loop
    V_CASE_BLOCK := serializeType(P_TYPE_LIST(i));
    DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_CASE_BLOCK);
    DBMS_LOB.FREETEMPORARY(V_CASE_BLOCK);
  end loop;	

  V_END_BLOCK := '  end case;' || CHR(10)
			  || 'end;' || CHR(10)
  
  DBMS_LOB.writeAppend(V_PLSQL_BLOCK,LENGTH(V_END_BLOCK),V_END_BLOCK);
  
  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE(V_PLSQL_BLOCK); $END
  
  return V_PLSQL_BLOCK;

end;
--
procedure addSerializationBlock(P_OBJECT_METADATA ANYTYPE, P_TYPE_INFO TYPE_INFO_T, P_PLSQL_BLOCK IN OUT NOCOPY CLOB)
as
  V_DECLARATIONS CLOB;
  V_ASSIGNMENTS  CLOB;

  V_DECLARATION      VARCHAR2(1024);
  V_ASSIGNMENT       VARCHAR2(4000);
  
  a        ATTR_INFO_T;
  a   TYPE_INFO_T;
  V_VARIABLE_NAME    VARCHAR2(16);

begin
  V_DECLARATIONS := '  if V_SCHEMA_NAME = ''' || P_TYPE_INFO.SCHEMA_NAME || ''' and V_TYPE_NAME = ''' || P_TYPE_INFO.TYPE_NAME || ''' then' || CHR(10)
                 || '    declare' || CHR(10);

  V_ASSIGNMENTS  := '    begin' || CHR(10)
                 || '      V_OBJECT_CONSTRUCTOR := ''"' || P_TYPE_INFO.SCHEMA_NAME || '"."'|| P_TYPE_INFO.TYPE_NAME || '"('';'|| CHR(10)
                 || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT_CONSTRUCTOR),V_OBJECT_CONSTRUCTOR);' || CHR(10);
  
  for V_ATTR_IDX in 1 .. P_TYPE_INFO.ATTR_COUNT loop
    V_VARIABLE_NAME := 'V_ATTR_' || LPAD(V_ATTR_IDX,4,'0');
    V_DECLARATION := '      ' || V_VARIABLE_NAME || ' ';    
 
    a := getAttrTypeInfoRecord(P_OBJECT_METADATA,V_ATTR_IDX);
    
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('addSerializatonBlock() : Processing Attribute: ' || a.ATTR_NAME || '. Data Type: ' || a.TYPE_ID); $END
    
    case a.TYPE_ID 
       when DBMS_TYPES.TYPECODE_BDOUBLE then
          V_DECLARATION := V_DECLARATION || 'BINARY_DOUBLE;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getBDouble(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BFILE then
          V_DECLARATION := V_DECLARATION || 'BFILE;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getBFile(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BFLOAT then
          V_DECLARATION := V_DECLARATION || 'BINARY_FLOAT;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getBFloat' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BLOB then
          V_DECLARATION := V_DECLARATION || 'BLOB;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getBlob(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CFILE then
          V_DECLARATION := V_DECLARATION || 'CFILE;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getCFile(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CHAR then
          V_DECLARATION := V_DECLARATION || 'CHAR(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getChar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ')' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CLOB then
          V_DECLARATION := V_DECLARATION || 'CLOB;' || CHR(10);
          V_DECLARATION := '    ' || V_VARIABLE_NAME || ' CLOB;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getChar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.APPEND(P_SERIALIZATION,' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_DATE then
          V_DECLARATION := V_DECLARATION || 'DATE;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getDate(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_INTERVAL_DS then
          V_DECLARATION := V_DECLARATION || 'INTERAVAL_DS;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getIntervalDS(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_INTERVAL_YM then
          V_DECLARATION := V_DECLARATION || 'INTERVAL_YM;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getIntervalYM(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_MLSLABEL then
          NULL;
        when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getCollection(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),P_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_NCHAR then
          V_ASSIGNMENT := '      V_ANYDATA_ARRAY(V_ANYDATA_ARRAY.count) := P_OBJECT.AccessNChar();' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_NCLOB then
          V_DECLARATION := V_DECLARATION || 'NCLOB;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getNClob(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.APPEND(P_SERIALIZATION,' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);         
        when DBMS_TYPES.TYPECODE_NUMBER then
          V_DECLARATION := V_DECLARATION || 'NUMBER;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getNumber(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
         when 3 then -- NUMBER(38) in ORDSYS.IMAGE
          V_DECLARATION := V_DECLARATION || 'NUMBER(38);' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getNumber(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);       
	    when DBMS_TYPES.TYPECODE_NVARCHAR2 then
          V_DECLARATION := V_DECLARATION || 'NVARCHAR2(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getNVarchar2(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_OBJECT then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getObject(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertObject(' || V_VARIABLE_NAME || '),P_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_OPAQUE then
          NULL;
        when DBMS_TYPES.TYPECODE_RAW then
          V_DECLARATION := V_DECLARATION || 'RAW(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getRaw(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_REF then
          NULL;
        when DBMS_TYPES.TYPECODE_TABLE then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getCollection@(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),P_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getTimestamp(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ') WITH LOCAL TIME ZONE;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getTimestampLTZ(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ') WITH TIME ZONE;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getTimestampTZ(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_UROWID then
          V_DECLARATION := V_DECLARATION || 'UROWID;' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getIURowid(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARCHAR2 then
          V_DECLARATION := V_DECLARATION || 'VARCHAR2(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getVarchar2(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARCHAR  then
          V_DECLARATION := V_DECLARATION || 'VARCHAR(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getVarchar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARRAY then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '      V_RESULT := V_OBJECT.getCollection@(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '      if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),P_SERIALIZATION); end if;' || CHR(10);
    end case;
       
    if (V_ATTR_IDX <  P_TYPE_INFO.ATTR_COUNT) then
      V_ASSIGNMENT := V_ASSIGNMENT
                   || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'','');' || CHR(10);
    else 
      V_ASSIGNMENT := V_ASSIGNMENT
                   || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,'')'');' || CHR(10);
    end if;
    
    DBMS_LOB.WRITEAPPEND(V_DECLARATIONS,LENGTH(V_DECLARATION),V_DECLARATION);
    DBMS_LOB.WRITEAPPEND(V_ASSIGNMENTS,LENGTH(V_ASSIGNMENT),V_ASSIGNMENT);  
    
  end loop;
  
  V_ASSIGNMENT := '       end;' || CHR(10)
               || '     return; ' || CHR(10)
               || '   end if;' || CHR(10);
  DBMS_LOB.WRITEAPPEND(V_ASSIGNMENTS,LENGTH(V_ASSIGNMENT),V_ASSIGNMENT);  

  DBMS_LOB.APPEND(P_PLSQL_BLOCK,V_DECLARATIONS);
  DBMS_LOB.APPEND(P_PLSQL_BLOCK,V_ASSIGNMENTS);
  
  for V_ATTR_IDX in 1 .. P_TYPE_INFO.ATTR_COUNT loop
    a := getAttrTypeInfoRecord(P_OBJECT_METADATA,V_ATTR_IDX);
    if a.TYPE_ID in (DBMS_TYPES.TYPECODE_OBJECT, DBMS_TYPES.TYPECODE_VARRAY, DBMS_TYPES.TYPECODE_TABLE) then
      $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('addSerializatonBlock() : Processing Non-Scalar Attribute: ' || a.ATTR_NAME ); $END
      a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
      addSerializationBlock(a.ATTR_TYPE_METADATA,a,P_PLSQL_BLOCK);
    end if;
  end loop;

end;
--
procedure generateSerializeObjectFunction(P_OBJECT_METADATA IN OUT NOCOPY ANYTYPE, P_TYPE_INFO IN OUT NOCOPY TYPE_INFO_T)
as
--
  V_PLSQL_BLOCK CLOB;
begin
  V_PLSQL_BLOCK := 'function SERIALIZE_OBJECT(P_OBJECT in ANYDATA) return CLOB' || CHR(10)
                || 'as ' || CHR(10)
                || '  V_SERIALIZATION CLOB;' || CHR(10)
                || 'begin' || CHR(10)
                || '  DBMS_LOB.CREATEREMPORARY(V_SERIALIZATION,TRUE,DBMS_LOB.CALL);' || CHR(10)
                || '  SERIALIZE_OBJECT(P_OBJECT, V_SERIALIZATION);' || CHR(10)
                || '  return V_SERIALIZATION;' || CHR(10)
                || 'end;' || CHR(10)
                || '--' || CHR(10)
                || 'procedure SERIALIZE_OBJECT(P_OBJECT IN ANYDATA, P_SERIALIZATION in out CLOB)' || CHR(10)
                || 'as' || CHR(10)
                || '  V_OBJECT             ANYDATA;' || CHR(10)
                || '  V_RESULT             PLS_INTEGER;' || CHR(10)
                || '  V_TYPE_METADATA      ANYTYPE;' || CHR(10) 
                || '  V_SINGLE_QUOTE       CHAR(1) := CHR(39);' || CHR(10)
                || '  V_CHAR_FORMAT        VARCHAR2(4000); ' || CHR(10)
                || '  V_TYPE_ID            PLS_INTEGER; ' || CHR(10)
                || '  V_PRECISION          NUMBER; ' || CHR(10)
                || '  V_SCALE              NUMBER; ' || CHR(10)
                || '  V_LENGTH             NUMBER; ' || CHR(10)
                || '  V_CSID               NUMBER; ' || CHR(10)
                || '  V_CSFRM              NUMBER; ' || CHR(10)
                || '  V_SCHEMA_NAME        VARCHAR2(128); ' || CHR(10)
                || '  V_TYPE_NAME          VARCHAR2(128); ' || CHR(10)
                || '  V_TYPE_VERSION       VARCHAR2(128); ' || CHR(10)
                || '  V_ATTR_COUNT         NUMBER; ' || CHR(10)
                || '  V_OBJECT_CONSTRUCTOR VARCHAR2(266);' || CHR(10)
                || 'begin' || CHR(10)
                || '  V_OBJECT := P_OBJECT;' || CHR(10)
                || '  V_TYPE_ID := V
                _OBJECT.getType(V_TYPE_METADATA);' || CHR(10)
                || '  V_TYPE_ID := V_TYPE_METADATA.getInfo(' || CHR(10)
                || '    V_PRECISION,' || CHR(10)
                || '    V_SCALE,' || CHR(10)
                || '    V_LENGTH,' || CHR(10)
                || '    V_CSID,' || CHR(10)
                || '    V_CSFRM,' || CHR(10)
                || '    V_SCHEMA_NAME,' || CHR(10)
                || '    V_TYPE_NAME,' || CHR(10)
                || '    V_TYPE_VERSION,' || CHR(10)
                || '    V_ATTR_COUNT' || CHR(10)
                || '  );    ' || CHR(10)
                || '  V_OBJECT.PIECEWISE();' || CHR(10);
  
  addSerializationBlock(P_OBJECT_METADATA,P_TYPE_INFO,V_PLSQL_BLOCK);
  DBMS_LOB.writeAppend(V_PLSQL_BLOCK,4,'end;');

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE(V_PLSQL_BLOCK); $END

end;
--
procedure serializeNonScalarAttributes(P_OBJECT IN OUT NOCOPY ANYDATA, P_OBJECT_METADATA IN OUT NOCOPY ANYTYPE, P_TYPE_INFO IN OUT NOCOPY TYPE_INFO_T)
as
  V_PLSQL_BLOCK  CLOB;
  V_DECLARATIONS CLOB;
  V_ASSIGNMENTS  CLOB;

  V_DECLARATION          VARCHAR2(1024);
  V_ASSIGNMENT           VARCHAR2(4000);
  
  a            ATTR_INFO_T;
  a       TYPE_INFO_T;
  V_VARIABLE_NAME        VARCHAR2(16);
  V_OBJECT_SERIALIZATION CLOB;

begin
  V_DECLARATIONS := 'declare' || CHR(10)
                 || '  V_RESULT             PLS_INTEGER;' || CHR(10)
                 || '  V_SINGLE_QUOTE       CHAR(1) := CHR(39);' || CHR(10)
                 || '  V_CHAR_FORMAT        VARCHAR2(4000); ' || CHR(10)
                 || '  V_OBJECT             ANYDATA;' || CHR(10)
                 || '  V_SERIALIZATION      CLOB;' || CHR(10);

  V_ASSIGNMENTS  := 'begin' || CHR(10)
                 || '  DBMS_LOB.CREATETEMPORARY(V_SERIALIZATION,TRUE,DBMS_LOB.CALL);' || CHR(10)
                 || '  V_OBJECT := :1;' || CHR(10)
				 || '  V_OBJECT.PIECEWISE();' || CHR(10);
  
  DBMS_LOB.createTemporary(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  DBMS_LOB.createTemporary(V_OBJECT_SERIALIZATION,TRUE,DBMS_LOB.CALL);
  
  for V_ATTR_IDX in 1 .. P_TYPE_INFO.ATTR_COUNT loop
    V_VARIABLE_NAME := 'V_ATTR_' || LPAD(V_ATTR_IDX,4,'0');
    V_DECLARATION := '      ' || V_VARIABLE_NAME || ' ';    
 
    a := getAttrTypeInfoRecord(P_OBJECT_METADATA,V_ATTR_IDX);
    
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('serializeNonScalarAttributes(): Processing Attribute: ' || a.ATTR_NAME || '. Data Type: ' || a.TYPE_ID); $END
    
    case a.TYPE_ID 
       when DBMS_TYPES.TYPECODE_BDOUBLE then
          V_DECLARATION := V_DECLARATION || 'BINARY_DOUBLE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBDouble(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BFILE then
          V_DECLARATION := V_DECLARATION || 'BFILE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBFile(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BFLOAT then
          V_DECLARATION := V_DECLARATION || 'BINARY_FLOAT;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBFloat' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BLOB then
          V_DECLARATION := V_DECLARATION || 'BLOB;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBlob(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CFILE then
          V_DECLARATION := V_DECLARATION || 'CFILE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCFile(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CHAR then
          V_DECLARATION := V_DECLARATION || 'CHAR(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getChar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ')' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CLOB then
          V_DECLARATION := V_DECLARATION || 'CLOB;' || CHR(10);
          V_DECLARATION := '    ' || V_VARIABLE_NAME || ' CLOB;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getChar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.APPEND(V_SERIALIZATION,' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_DATE then
          V_DECLARATION := V_DECLARATION || 'DATE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getDate(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_INTERVAL_DS then
          V_DECLARATION := V_DECLARATION || 'INTERAVAL_DS;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getIntervalDS(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_INTERVAL_YM then
          V_DECLARATION := V_DECLARATION || 'INTERVAL_YM;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getIntervalYM(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_MLSLABEL then
          NULL;
        when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCollection(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_NCHAR then
          V_ASSIGNMENT := '  V_ANYDATA_ARRAY(V_ANYDATA_ARRAY.count) := P_OBJECT.AccessNChar();' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_NCLOB then
          V_DECLARATION := V_DECLARATION || 'NCLOB;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNClob(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.APPEND(V_SERIALIZATION,' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);         
        when DBMS_TYPES.TYPECODE_NUMBER then
          V_DECLARATION := V_DECLARATION || 'NUMBER;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNumber(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when 3 then -- NUMBER(38) in ORDSYS.ORDIMAGE
          V_DECLARATION := V_DECLARATION || 'NUMBER(38);' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNumber(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_NVARCHAR2 then
          V_DECLARATION := V_DECLARATION || 'NVARCHAR2(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNVarchar2(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_OBJECT then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getObject(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertObject(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_OPAQUE then
          NULL;
        when DBMS_TYPES.TYPECODE_RAW then
          V_DECLARATION := V_DECLARATION || 'RAW(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getRaw(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_REF then
          NULL;
        when DBMS_TYPES.TYPECODE_TABLE then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCollection@(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getTimestamp(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ') WITH LOCAL TIME ZONE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getTimestampLTZ(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ') WITH TIME ZONE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getTimestampTZ(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_UROWID then
          V_DECLARATION := V_DECLARATION || 'UROWID;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getIURowid(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARCHAR2 then
          V_DECLARATION := V_DECLARATION || 'VARCHAR2(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getVarchar2(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARCHAR  then
          V_DECLARATION := V_DECLARATION || 'VARCHAR(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getVarchar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARRAY then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCollection@(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
		else
          DBMS_OUTPUT.put_line('WTF - serializeNonScalarAttributes() TYPECODE:' || a.TYPE_ID);		
    end case;
    
    if (V_ATTR_IDX <  P_TYPE_INFO.ATTR_COUNT) then
      V_ASSIGNMENT := V_ASSIGNMENT
                   || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'','');' || CHR(10);
    else 
      V_ASSIGNMENT := V_ASSIGNMENT
                   || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'')'');' || CHR(10);
    end if;
    
    DBMS_LOB.WRITEAPPEND(V_DECLARATIONS,LENGTH(V_DECLARATION),V_DECLARATION);
    DBMS_LOB.WRITEAPPEND(V_ASSIGNMENTS,LENGTH(V_ASSIGNMENT),V_ASSIGNMENT);  
    
  end loop;
  
  V_ASSIGNMENT := '  :2 := V_SERIALIZATION;' || CHR(10)
               $IF $$DEBUG $THEN || '  DBMS_OUTPUT.PUT_LINE(V_SERIALIZATION);' || CHR(10) $END
               || 'end;' || CHR(10);
  DBMS_LOB.WRITEAPPEND(V_ASSIGNMENTS,LENGTH(V_ASSIGNMENT),V_ASSIGNMENT);  

  DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_DECLARATIONS);
  DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_ASSIGNMENTS);
  
  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE(V_PLSQL_BLOCK); $END
  
  EXECUTE IMMEDIATE V_PLSQL_BLOCK USING IN OUT P_OBJECT, OUT V_OBJECT_SERIALIZATION;
  DBMS_LOB.APPEND(G_OBJECT_SERIALIZATION,V_OBJECT_SERIALIZATION);
end;
--
procedure serializeScalarAttributes(P_OBJECT IN OUT NOCOPY ANYDATA, P_OBJECT_METADATA IN OUT NOCOPY ANYTYPE, P_TYPE_INFO IN OUT NOCOPY TYPE_INFO_T)
as
  V_SERIALIZED_DATA  CLOB;
  V_RESULT           PLS_INTEGER;
  a        ATTR_INFO_T;

  V_CLOB             CLOB;
  V_BLOB             BLOB;
  V_NCLOB            NCLOB;
  V_BFILE            BFILE;
  
  V_BDOUBLE          BINARY_DOUBLE;
  V_BFLOAT           BINARY_FLOAT;
  V_NUMBER           NUMBER;

  
  V_CHAR             CHAR(4000);
  V_NCHAR            NCHAR(4000);
  V_VARCHAR2         VARCHAR2(32767);
  V_NVARCHAR2        NVARCHAR2(32767);

  V_RAW              RAW(32767);
  
  V_DATE             DATE;
  V_INTERVAL_DS      INTERVAL DAY TO SECOND;
  V_INTERVAL_YM      INTERVAL YEAR TO MONTH;
  V_TIMESTAMP        TIMESTAMP;
  V_TIMESTAMP_TZ     TIMESTAMP WITH TIME ZONE;
  V_TIMESTAMP_LTZ    TIMESTAMP WITH LOCAL TIME ZONE;
  
  V_DIRECTORY_ALIAS  VARCHAR2(128 CHAR);
  V_PATH2FILE        VARCHAR2(2000 CHAR);

  V_SINGLE_QUOTE     CHAR(1) := CHR(39);
begin
  P_OBJECT.PIECEWISE();
  for V_ATTR_IDX in 1 .. P_TYPE_INFO.ATTR_COUNT loop
    V_SERIALIZED_DATA := 'NULL';
    a := getAttrTypeInfoRecord(P_OBJECT_METADATA,V_ATTR_IDX);
    $IF $$DEBUG $THEN  DBMS_OUTPUT.put_line('Processing Attr ' || V_ATTR_IDX || ' ["' || a.ATTR_NAME || '"]. Data Type ' || a.TYPE_ID); $END
    case a.TYPE_ID 
      when DBMS_TYPES.TYPECODE_BDOUBLE then
        V_RESULT := P_OBJECT.getBDouble(V_BDOUBLE);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := TO_CHAR(V_BDOUBLE);  
      when DBMS_TYPES.TYPECODE_BFILE then
        V_RESULT := P_OBJECT.getBFile(V_BFILE);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        DBMS_LOB.FILEGETNAME(V_BFILE,V_DIRECTORY_ALIAS,V_PATH2FILE);
        V_SERIALIZED_DATA := 'BFILENAME(''' || V_DIRECTORY_ALIAS || ''',''' || V_PATH2FILE || ''')';
      when DBMS_TYPES.TYPECODE_BFLOAT then
        V_RESULT := P_OBJECT.getBFloat(V_BFLOAT);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := TO_CHAR(V_BFLOAT);  
      when DBMS_TYPES.TYPECODE_BLOB then
		V_RESULT := P_OBJECT.getBLOB(V_BLOB);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
		V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: NVARCHAR2' || V_SINGLE_QUOTE;
		/*
		DBMS_LOB.CREATETEMPORARY(V_SERIALIZED_DATA,TRUE,DBMS_LOB.CALL);
		DBMS_LOB.WRITEAPPEND(V_SERIALIZED_DATA,1,V_SINGLE_QUOTE);
		DBMS_LOB.APPEND(V_SERIALIZED_DATA, V_BLOB);
		DBMS_LOB.WRITEAPPEND(V_SERIALIZED_DATA,1,V_SINGLE_QUOTE);
		*/
      when DBMS_TYPES.TYPECODE_CFILE then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: CFILE' || V_SINGLE_QUOTE;
        -- V_SERIALIZED_DATA := P_OBJECT.AccessCFILE();
      when DBMS_TYPES.TYPECODE_CHAR then
        V_RESULT := P_OBJECT.getCHAR(V_CHAR);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || SUBSTR(V_CHAR,1,a.LENGTH) || V_SINGLE_QUOTE;
      when DBMS_TYPES.TYPECODE_CLOB then
		V_RESULT := P_OBJECT.getCLOB(V_NCLOB);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
		DBMS_LOB.CREATETEMPORARY(V_SERIALIZED_DATA,TRUE,DBMS_LOB.CALL);
		DBMS_LOB.WRITEAPPEND(V_SERIALIZED_DATA,1,V_SINGLE_QUOTE);
		DBMS_LOB.APPEND(V_SERIALIZED_DATA, V_CLOB);
		DBMS_LOB.WRITEAPPEND(V_SERIALIZED_DATA,1,V_SINGLE_QUOTE);
      when DBMS_TYPES.TYPECODE_DATE then
        V_RESULT := P_OBJECT.getDate(V_DATE);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE  || TO_CHAR(V_DATE,'YYYY-MM-DD"T"HH24:MI:SS') || V_SINGLE_QUOTE;  
      when DBMS_TYPES.TYPECODE_INTERVAL_DS then
        V_RESULT := P_OBJECT.getIntervalDS(V_INTERVAL_DS);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := TO_CHAR(V_INTERVAL_DS);  
      when DBMS_TYPES.TYPECODE_INTERVAL_YM then
        V_RESULT := P_OBJECT.getIntervalYM(V_INTERVAL_YM);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := TO_CHAR(V_INTERVAL_YM);  
      when DBMS_TYPES.TYPECODE_MLSLABEL then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: MLSLABEL' || V_SINGLE_QUOTE;
      when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: NAMEDCOLLECTION' || V_SINGLE_QUOTE;
      when DBMS_TYPES.TYPECODE_NCHAR then
        V_RESULT := P_OBJECT.getNCHAR(V_NCHAR);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || TO_CHAR(V_NCHAR) || V_SINGLE_QUOTE;  
      when DBMS_TYPES.TYPECODE_NCLOB then
		V_RESULT := P_OBJECT.getNCLOB(V_NCLOB);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
		DBMS_LOB.CREATETEMPORARY(V_SERIALIZED_DATA,TRUE,DBMS_LOB.CALL);
		DBMS_LOB.WRITEAPPEND(V_SERIALIZED_DATA,1,V_SINGLE_QUOTE);
		DBMS_LOB.APPEND(V_SERIALIZED_DATA, V_NCLOB);
		DBMS_LOB.WRITEAPPEND(V_SERIALIZED_DATA,1,V_SINGLE_QUOTE);
      when DBMS_TYPES.TYPECODE_NUMBER then
        V_RESULT := P_OBJECT.getNumber(V_NUMBER);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := TO_CHAR(V_NUMBER);  
      when 3 then -- NUMBER(38) in ORDSYS.ORDIMAGE
        V_RESULT := P_OBJECT.getNumber(V_NUMBER);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := TO_CHAR(V_NUMBER);  
      when DBMS_TYPES.TYPECODE_NVARCHAR2 then
	    V_RESULT := P_OBJECT.getNVarchar2(V_NVARCHAR2);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || TO_CHAR(V_NVARCHAR2) || V_SINGLE_QUOTE;  
      when DBMS_TYPES.TYPECODE_OBJECT then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: OBJECT' || V_SINGLE_QUOTE;
      when DBMS_TYPES.TYPECODE_OPAQUE then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: OPAQUE' || V_SINGLE_QUOTE;
      when DBMS_TYPES.TYPECODE_RAW then
	    V_RESULT := P_OBJECT.getNVarchar2(V_RAW);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || TO_CHAR(V_RAW) || V_SINGLE_QUOTE;  
      when DBMS_TYPES.TYPECODE_REF then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: REF' || V_SINGLE_QUOTE;
        -- V_SERIALIZED_DATA := P_OBJECT.AccessRef();
      when DBMS_TYPES.TYPECODE_TABLE then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: TABLE' || V_SINGLE_QUOTE;
        -- V_SERIALIZED_DATA := P_OBJECT.AccessTable();
      when DBMS_TYPES.TYPECODE_TIMESTAMP then
        V_RESULT := P_OBJECT.getTimestamp(V_TIMESTAMP);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SS.FFF') || V_SINGLE_QUOTE ;  
      when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then
        V_RESULT := P_OBJECT.getTimestampLTZ(V_TIMESTAMP_LTZ);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP_LTZ,'YYYY-MM-DD"T"HH24:MI:SS.FFFTZH:TZM') || V_SINGLE_QUOTE;  
      when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then
        V_RESULT := P_OBJECT.getTimestampLTZ(V_TIMESTAMP_TZ);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || TO_CHAR(V_TIMESTAMP_TZ,'YYYY-MM-DD"T"HH24:MI:SS.FFFTZH:TZM') || V_SINGLE_QUOTE;  
      when DBMS_TYPES.TYPECODE_UROWID then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: UROWID' || V_SINGLE_QUOTE;
        -- V_SERIALIZED_DATA := P_OBJECT.AccessURowid();
      when DBMS_TYPES.TYPECODE_VARCHAR2 then
        V_RESULT := P_OBJECT.getVARCHAR2(V_VARCHAR2);
        exit when  (V_RESULT = DBMS_TYPES.NO_DATA);
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || V_VARCHAR2 || V_SINGLE_QUOTE;
      when DBMS_TYPES.TYPECODE_VARCHAR then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: VARCHAR' || V_SINGLE_QUOTE;
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || P_OBJECT.AccessVarchar() || V_SINGLE_QUOTE;
      when DBMS_TYPES.TYPECODE_VARRAY then
        V_SERIALIZED_DATA := V_SINGLE_QUOTE || 'Unsupported Attribute Data Type: VARRAY' || V_SINGLE_QUOTE;      
    end case;
    if (V_ATTR_IDX > 1) then
      DBMS_LOB.WRITEAPPEND(G_OBJECT_SERIALIZATION,1,',');
    end if;
    DBMS_LOB.APPEND(G_OBJECT_SERIALIZATION,V_SERIALIZED_DATA);
	DBMS_LOB.FREETEMPORARY(V_SERIALIZED_DATA);
  end loop;
end;
--
procedure serializeNonScalarAttributes(P_OBJECT IN OUT NOCOPY ANYDATA, P_OBJECT_METADATA IN OUT NOCOPY ANYTYPE, P_TYPE_INFO IN OUT NOCOPY TYPE_INFO_T)
as
  V_PLSQL_BLOCK  CLOB;
  V_DECLARATIONS CLOB;
  V_ASSIGNMENTS  CLOB;

  V_DECLARATION          VARCHAR2(1024);
  V_ASSIGNMENT           VARCHAR2(4000);
  
  a            ATTR_INFO_T;
  a       TYPE_INFO_T;
  V_VARIABLE_NAME        VARCHAR2(16);
  V_OBJECT_SERIALIZATION CLOB;

begin
  V_DECLARATIONS := 'declare' || CHR(10)
                 || '  V_RESULT             PLS_INTEGER;' || CHR(10)
                 || '  V_SINGLE_QUOTE       CHAR(1) := CHR(39);' || CHR(10)
                 || '  V_CHAR_FORMAT        VARCHAR2(4000); ' || CHR(10)
                 || '  V_OBJECT             ANYDATA;' || CHR(10)
                 || '  V_SERIALIZATION      CLOB;' || CHR(10);

  V_ASSIGNMENTS  := 'begin' || CHR(10)
                 || '  DBMS_LOB.CREATETEMPORARY(V_SERIALIZATION,TRUE,DBMS_LOB.CALL);' || CHR(10)
                 || '  V_OBJECT := :1;' || CHR(10)
				 || '  V_OBJECT.PIECEWISE();' || CHR(10);
  
  DBMS_LOB.createTemporary(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);
  DBMS_LOB.createTemporary(V_OBJECT_SERIALIZATION,TRUE,DBMS_LOB.CALL);
  
  for V_ATTR_IDX in 1 .. P_TYPE_INFO.ATTR_COUNT loop
    V_VARIABLE_NAME := 'V_ATTR_' || LPAD(V_ATTR_IDX,4,'0');
    V_DECLARATION := '      ' || V_VARIABLE_NAME || ' ';    
 
    a := getAttrTypeInfoRecord(P_OBJECT_METADATA,V_ATTR_IDX);
    
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('serializeNonScalarAttributes(): Processing Attribute: ' || a.ATTR_NAME || '. Data Type: ' || a.TYPE_ID); $END
    
    case a.TYPE_ID 
       when DBMS_TYPES.TYPECODE_BDOUBLE then
          V_DECLARATION := V_DECLARATION || 'BINARY_DOUBLE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBDouble(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BFILE then
          V_DECLARATION := V_DECLARATION || 'BFILE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBFile(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BFLOAT then
          V_DECLARATION := V_DECLARATION || 'BINARY_FLOAT;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBFloat' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_BLOB then
          V_DECLARATION := V_DECLARATION || 'BLOB;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getBlob(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CFILE then
          V_DECLARATION := V_DECLARATION || 'CFILE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCFile(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CHAR then
          V_DECLARATION := V_DECLARATION || 'CHAR(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getChar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ')' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_CLOB then
          V_DECLARATION := V_DECLARATION || 'CLOB;' || CHR(10);
          V_DECLARATION := '    ' || V_VARIABLE_NAME || ' CLOB;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getChar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.APPEND(V_SERIALIZATION,' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_DATE then
          V_DECLARATION := V_DECLARATION || 'DATE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getDate(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_INTERVAL_DS then
          V_DECLARATION := V_DECLARATION || 'INTERAVAL_DS;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getIntervalDS(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_INTERVAL_YM then
          V_DECLARATION := V_DECLARATION || 'INTERVAL_YM;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getIntervalYM(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_MLSLABEL then
          NULL;
        when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCollection(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_NCHAR then
          V_ASSIGNMENT := '  V_ANYDATA_ARRAY(V_ANYDATA_ARRAY.count) := P_OBJECT.AccessNChar();' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_NCLOB then
          V_DECLARATION := V_DECLARATION || 'NCLOB;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNClob(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.APPEND(V_SERIALIZATION,' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);         
        when DBMS_TYPES.TYPECODE_NUMBER then
          V_DECLARATION := V_DECLARATION || 'NUMBER;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNumber(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when 3 then -- NUMBER(38) in ORDSYS.ORDIMAGE
          V_DECLARATION := V_DECLARATION || 'NUMBER(38);' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNumber(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10);
        when DBMS_TYPES.TYPECODE_NVARCHAR2 then
          V_DECLARATION := V_DECLARATION || 'NVARCHAR2(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getNVarchar2(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_OBJECT then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getObject(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertObject(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_OPAQUE then
          NULL;
        when DBMS_TYPES.TYPECODE_RAW then
          V_DECLARATION := V_DECLARATION || 'RAW(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getRaw(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_REF then
          NULL;
        when DBMS_TYPES.TYPECODE_TABLE then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCollection@(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getTimestamp(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ') WITH LOCAL TIME ZONE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getTimestampLTZ(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then
          V_DECLARATION := V_DECLARATION || 'TIMESTAMP(' || a.LENGTH || ') WITH TIME ZONE;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getTimestampTZ(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_UROWID then
          V_DECLARATION := V_DECLARATION || 'UROWID;' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getIURowid(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  V_CHAR_FORMAT := NVL(TO_CHAR(' || V_VARIABLE_NAME || '),''NULL'');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(V_CHAR_FORMAT),V_CHAR_FORMAT);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARCHAR2 then
          V_DECLARATION := V_DECLARATION || 'VARCHAR2(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getVarchar2(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARCHAR  then
          V_DECLARATION := V_DECLARATION || 'VARCHAR(' || a.LENGTH || ');' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getVarchar(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,LENGTH(' || V_VARIABLE_NAME ||'),' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
        when DBMS_TYPES.TYPECODE_VARRAY then
          a := getTypeInfoRecord(a.ATTR_TYPE_METADATA);
          V_DECLARATION := '  ' || V_VARIABLE_NAME || '     "' || a.SCHEMA_NAME || '"."' || a.TYPE_NAME || '";' || CHR(10);
          V_ASSIGNMENT := '  V_RESULT := V_OBJECT.getCollection@(' || V_VARIABLE_NAME || ');' || CHR(10)
                       || '  if (' || V_VARIABLE_NAME || ' is NULL) then DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(' || V_VARIABLE_NAME || '),V_SERIALIZATION); end if;' || CHR(10);
		else
          DBMS_OUTPUT.put_line('WTF - serializeNonScalarAttributes() TYPECODE:' || a.TYPE_ID);		
    end case;
    
    if (V_ATTR_IDX <  P_TYPE_INFO.ATTR_COUNT) then
      V_ASSIGNMENT := V_ASSIGNMENT
                   || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'','');' || CHR(10);
    else 
      V_ASSIGNMENT := V_ASSIGNMENT
                   || '  DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'')'');' || CHR(10);
    end if;
    
    DBMS_LOB.WRITEAPPEND(V_DECLARATIONS,LENGTH(V_DECLARATION),V_DECLARATION);
    DBMS_LOB.WRITEAPPEND(V_ASSIGNMENTS,LENGTH(V_ASSIGNMENT),V_ASSIGNMENT);  
    
  end loop;
  
  V_ASSIGNMENT := '  :2 := V_SERIALIZATION;' || CHR(10)
               $IF $$DEBUG $THEN || '  DBMS_OUTPUT.PUT_LINE(V_SERIALIZATION);' || CHR(10) $END
               || 'end;' || CHR(10);
  DBMS_LOB.WRITEAPPEND(V_ASSIGNMENTS,LENGTH(V_ASSIGNMENT),V_ASSIGNMENT);  

  DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_DECLARATIONS);
  DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_ASSIGNMENTS);
  
  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE(V_PLSQL_BLOCK); $END
  
  EXECUTE IMMEDIATE V_PLSQL_BLOCK USING IN OUT P_OBJECT, OUT V_OBJECT_SERIALIZATION;
  DBMS_LOB.APPEND(G_OBJECT_SERIALIZATION,V_OBJECT_SERIALIZATION);
end;
--
procedure serializeObject(P_OBJECT IN OUT NOCOPY ANYDATA) 
as

/*
** 
** Can serialize objects whose attributes are scalar without using Dynamic SQL.
** For objects that have one or more non-scalar attributes use Dynancic SQL to 
** convert the object's attriubute values to ANYDATA and then serialize the set 
of ANYDATA objects.
**
** This appproach is required because you cannot access the value of a non-scalar attributes
** without declaring a variable whose type matches the type of the attribute. E.G. you cannot
** access the attribute value directly as an ANYDDATA. 
**
** Also it appears that when you pass the ANYDATA that represents the parent type into an execute
** immediate you loose the context established using piecewise operator. Consequently if an object
** has one more non scalar atttibutes it will be processed by converting all the attibutes into access
** VARRAY of ANYDATA.
**
*/

  V_TYPE_ID          PLS_INTEGER;
  V_TYPE_METADATA    ANYTYPE;
  V_TYPE_DECLARATION VARCHAR2(266);
  V_TYPE_INFO        TYPE_INFO_T;
 begin
  V_TYPE_ID := P_OBJECT.getType(V_TYPE_METADATA);
  V_TYPE_INFO := getTypeInfoRecord(V_TYPE_METADATA);
  $IF $$DEBUG $THEN DBMS_OUTPUT.put_line('Processing Type ' || V_TYPE_INFO.SCHEMA_NAME || '.' || V_TYPE_INFO.TYPE_NAME || '. Data Type ' || V_TYPE_INFO.TYPE_ID || '. Attribute Count = ' || V_TYPE_INFO.ATTR_COUNT); $END
  V_TYPE_DECLARATION := '"' || V_TYPE_INFO.SCHEMA_NAME || '"."' || V_TYPE_INFO.TYPE_NAME || '"(';
  DBMS_LOB.WRITEAPPend(G_OBJECT_SERIALIZATION,LENGTH(V_TYPE_DECLARATION),V_TYPE_DECLARATION);
  if (hasNestedObjects(V_TYPE_METADATA, V_TYPE_INFO.ATTR_COUNT))
  then
    serializeNonScalarAttributes(P_OBJECT,V_TYPE_METADATA,V_TYPE_INFO);
  else 
    serializeScalarAttributes(P_OBJECT,V_TYPE_METADATA,V_TYPE_INFO);
  end if;
  DBMS_LOB.WRITEAPPend(G_OBJECT_SERIALIZATION,1,')');
end;
--
function serializeObject(P_OBJECT ANYDATA) 
return CLOB
as
  V_OBJECT ANYDATA := P_OBJECT;
begin
   DBMS_LOB.createTemporary(G_OBJECT_SERIALIZATION,TRUE,DBMS_LOB.CALL);
   serializeObject(V_OBJECT);
   return G_OBJECT_SERIALIZATION;
end;
--
function getTypeInfo(P_OBJECT ANYDATA)
return TYPE_INFO_TAB PIPELINED
as
  V_TYPE_ID        PLS_INTEGER;
  V_TYPE_METADATA  ANYTYPE;
begin
  V_TYPE_ID := P_OBJECT.getType(V_TYPE_METADATA);
  PIPE ROW (getTypeInfoRecord(V_TYPE_METADATA));
end;
--
function getAttrTypeInfo(P_OBJECT ANYDATA)
return ATTR_INFO_TAB PIPELINED
as
  V_TYPE_ID        PLS_INTEGER;
  V_TYPE_METADATA  ANYTYPE;
  V_TYPE_INFO      TYPE_INFO_T;
  a      ATTR_INFO_T;
begin
  V_TYPE_ID := P_OBJECT.getType(V_TYPE_METADATA);
  V_TYPE_INFO := getTypeInfoRecord(V_TYPE_METADATA);
  for V_ATTR_IDX in 1 .. V_TYPE_INFO.ATTR_COUNT loop
    PIPE ROW (getAttrTypeInfoRecord(V_TYPE_METADATA,V_ATTR_IDX));
  end loop;
end;
--
end;
/
show errors
--
/*
select SCHEMA_NAME, TYPE_NAME, TYPE_VERSION, ATTR_COUNT
  from OE.CUSTOMERS,
       TABLE(OBJECT_SERIALIZATION.getTypeInfo(ANYDATA.convertObject(CUST_ADDRESS))) x
/

select x.*
  from OE.CUSTOMERS,
       TABLE(OBJECT_SERIALIZATION.getAttrTypeInfo(ANYDATA.convertObject(CUST_ADDRESS))) x
/
where oe.ROWNUM < 2
*/
select CUST_ADDRESS,OBJECT_SERIALIZATION.serializeObject(ANYDATA.convertObject(CUST_ADDRESS))
  from OE.CUSTOMERS
 where ROWNUM < 10;
 
select PRODUCT_PHOTO,SErIALIZE_ORDIMAGE(ANYDATA.convertOBJECt(PRODUCT_PHOTO))
  from PM.ONLINE_MEDIA

select PHONE_NUMBERS,OBJECT_SERIALIZATION.serializeObject(ANYDATA.convertCollection(PHONE_NUMBERS))
  from OE.CUSTOMERS
 where ROWNUM < 10;
 
select CUST_GEO_LOCATION,case when CUST_GEO_LOCATION is NULL then to_CLOB('NULL') else OBJECT_SERIALIZATION.serializeObject(ANYDATA.convertObject(CUST_GEO_LOCATION)) end
  from OE.CUSTOMERS
 where CUST_GEO_LOCATION is null
 
