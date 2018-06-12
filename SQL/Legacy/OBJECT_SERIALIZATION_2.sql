set echo on
spool OBJECT_SERIALIZATION.log
--
ALTER SESSION SET PLSQL_CCFLAGS = 'DEBUG:TRUE'
/
create or replace package OBJECT_SERIALIZATION
AUTHID CURRENT_USER
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
  
  TYPE ANYTYPE_TAB is TABLE OF ANYTYPE;
  
  TYPE TYPE_LIST_T is RECORD (
    OWNER               VARCHAR2(128)
  , TYPE_NAME           VARCHAR2(128)
  );
  
  TYPE TYPE_LIST_TAB is TABLE of TYPE_LIST_T;
  
  function serializeType(P_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2) return CLOB;
end;
/
show errors;
--
create or replace package body OBJECT_SERIALIZATION
as
--
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
  V_ATTR_INFO      ATTR_INFO_T;
begin
  V_ATTR_INFO.TYPE_ID := P_TYPE_METADATA.GETATTRELEMINFO(
       P_ATTR_IDX,
       V_ATTR_INFO.PRECISION,
       V_ATTR_INFO.SCALE,
       V_ATTR_INFO.LENGTH,
       V_ATTR_INFO.CSID,
       V_ATTR_INFO.CSFRM,
       V_ATTR_INFO.ATTR_TYPE_METADATA,
       V_ATTR_INFO.ATTR_NAME
    );
    return V_ATTR_INFO;
end;
--
function serializeType(P_TYPE_METADATA ANYTYPE)
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

  V_TYPE_INFO        TYPE_INFO_T;
  V_ATTR_INFO        ATTR_INFO_T;
begin
  V_TYPE_INFO := getTypeInfoRecord(P_TYPE_METADATA);

  DBMS_LOB.CREATETEMPORARY(V_PLSQL_BLOCK,TRUE,DBMS_LOB.CALL);

  V_PLSQL := '  when V_SCHEMA_NAME = ''' || V_TYPE_INFO.SCHEMA_NAME || ''' and V_TYPE_NAME = ''' || V_TYPE_INFO.TYPE_NAME || ''' then' || CHR(10)
          || '    declare' || CHR(10)
		  || '      V_OBJECT           "' || V_TYPE_INFO.SCHEMA_NAME || '","' || V_TYPE_INFO.TYPE_NAME || '";' || CHR(10)
          || '      V_DIRECTORY_ALIAS  VARCHAR2(128 CHAR);' || CHR(10)
          || '      V_PATH2FILE        VARCHAR2(2000 CHAR);' || CHR(10)
          || '    begin' || CHR(10)
          || '      V_RESULT := P_ANYDATA.getObject(V_OBJECT);' || CHR(10)
		  || '      if (V_OBJECT is NULL) then' || CHR(10)
		  || '        DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || CHR(10)
		  || '        return;' || CHR(10)
		  || '      end if; ' || CHR(10)
          || '      V_OBJECT_CONSTRUCTOR := ''"' || V_TYPE_INFO.SCHEMA_NAME || '"."'|| V_TYPE_INFO.TYPE_NAME || '"('';'|| CHR(10)
          || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT_CONSTRUCTOR),V_OBJECT_CONSTRUCTOR);' || CHR(10);

  DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);

  for V_ATTR_IDX in 1 .. V_TYPE_INFO.ATTR_COUNT loop
    V_ATTR_INFO := getAttrTypeInfoRecord(P_TYPE_METADATA,V_ATTR_IDX);

    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('addSerializatonBlock() : Processing Attribute: ' || V_ATTR_INFO.ATTR_NAME || '. Data Type: ' || V_ATTR_INFO.TYPE_ID); $END
    V_PLSQL := '     if ((V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '" is NULL) then'
            || '       DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL'');' || CHR(10)
            || '     else' || CHR(10);

    case V_ATTR_INFO.TYPE_ID
      when DBMS_TYPES.TYPECODE_BDOUBLE then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '");' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_BFILE then
	    V_PLSQL := V_PLSQL
	            || '      DBMS_LOB.FILEGETNAME(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '",V_DIRECTORY_ALIAS,V_PATH2FILE);' || CHR(10)
                || '      V_SERIALIZED_VALUE := BFILENAME('' || V_DIRECTORY_ALIAS || '','' || V_PATH2FILE || '');' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_BFLOAT then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '");' || CHR(10)
              || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_BLOB then
      --  when DBMS_TYPES.TYPECODE_CFILE then
      when DBMS_TYPES.TYPECODE_CHAR then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '" || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_CLOB then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.APPEND(P_SERIALIZATION,V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_DATE then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_INTERVAL_DS then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_INTERVAL_YM then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '" || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_MLSLABEL then
      when DBMS_TYPES.TYPECODE_NAMEDCOLLECTION then
         V_PLSQL := V_PLSQL
                 || '      if (V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '" is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      when DBMS_TYPES.TYPECODE_NCHAR then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_NCLOB then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.APPEND(P_SERIALIZATION,V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_NUMBER then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '");' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
	        -- default
	  when DBMS_TYPES.TYPECODE_NVARCHAR2 then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.APPEND(P_SERIALIZATION,V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_OBJECT then
        V_PLSQL := V_PLSQL
                || '      if (V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '" is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertObject(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_OPAQUE then
      when DBMS_TYPES.TYPECODE_RAW then
        V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '");' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      --  when DBMS_TYPES.TYPECODE_REF then
      when DBMS_TYPES.TYPECODE_TABLE then
        V_PLSQL := V_PLSQL
                || '      if (V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '" is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      when DBMS_TYPES.TYPECODE_TIMESTAMP then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_TIMESTAMP_TZ then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_UROWID then
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '");' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_VARCHAR2 then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '"),V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_VARCHAR  then
	    V_PLSQL := V_PLSQL
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '"),V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '")' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,1,V_SINGLE_QUOTE);' || CHR(10);
      when DBMS_TYPES.TYPECODE_VARRAY then
	    V_PLSQL := V_PLSQL
                || '      if (V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '" is NULL) then DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,4,''NULL''); else serialize_Object(ANYDATA.convertCollection(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '"),P_SERIALIZATION); end if;' || CHR(10);
      else
	    V_PLSQL := V_PLSQL
                || '      V_SERIALIZED_VALUE := V_SINGLE_QUOTE || TO_CHAR(V_OBJECT."' || V_ATTR_INFO.ATTR_NAME || '") || V_SINGLE_QUOTE;' || CHR(10)
                || '      DBMS_LOB.WRITEAPPEND(P_SERIALIZATION,LENGTH(V_SERIALIZED_VALUE),V_SERIALIZED_VALUE);' || CHR(10);
    end case;

    if (V_ATTR_IDX <  V_TYPE_INFO.ATTR_COUNT) then
      V_PLSQL := V_PLSQL
              || '      DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'','');' || CHR(10)
              || '    end if;' || CHR(10);
    else
      V_PLSQL := V_PLSQL
              || '      DBMS_LOB.WRITEAPPEND(V_SERIALIZATION,1,'')'');' || CHR(10)
              || '    end if;' || CHR(10);
    end if;

    DBMS_LOB.WRITEAPPEND(V_PLSQL_BLOCK,LENGTH(V_PLSQL),V_PLSQL);

  end loop;

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE(V_PLSQL_BLOCK); $END

  return V_PLSQL_BLOCK;

end;
-- 
function serializeTypes(P_TYPE_LIST IN OUT NOCOPY ANYTYPE_TAB)
return CLOB
as
--
  V_PLSQL_BLOCK CLOB;
  V_CASE_BLOCK  CLOB;
  V_END_BLOCK  VARCHAR2(128);
  V_ANYTYPE     ANYTYPE;

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
				|| '  V_ATTR_INFO          ATTR_INFO_T;' || CHR(10)
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

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('Type count = ' || P_TYPE_LIST.count); $END	

  for i in 1..P_TYPE_LIST.count loop
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('Processing ANYTYPE[' || i || ']'); $END	
	V_ANYTYPE := P_TYPE_LIST(i);
	if (V_ANYTYPE is NULL) then DBMS_OUTPUT.PUT_LINE('ANYTYPE[' || i || ']: WTF'); end if;
    V_CASE_BLOCK := serializeType(P_TYPE_LIST(i));
    DBMS_LOB.APPEND(V_PLSQL_BLOCK,V_CASE_BLOCK);
    DBMS_LOB.FREETEMPORARY(V_CASE_BLOCK);
  end loop;

  V_END_BLOCK := '  end case;' || CHR(10)
			  || 'end;' || CHR(10);

  DBMS_LOB.writeAppend(V_PLSQL_BLOCK,LENGTH(V_END_BLOCK),V_END_BLOCK);

  $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE(V_PLSQL_BLOCK); $END

  return V_PLSQL_BLOCK;

end;
--
function serializeType(P_OWNER VARCHAR2, P_TYPE_NAME VARCHAR2)
return CLOB
as
  V_TYPE_LIST ANYTYPE_TAB := NEW ANYTYPE_TAB();
  V_ANYTYPE   ANYTYPE;
  
  cursor getTypeHierarchy 
  is
  select OWNER, TYPE_NAME
     from ALL_TYPES
          start with OWNER = P_OWNER and TYPE_NAME = P_TYPE_NAME
          connect by prior TYPE_NAME = SUPERTYPE_NAME
	                   and OWNER = SUPERTYPE_OWNER;
	
begin

  for t in getTypeHierarchy loop
    $IF $$DEBUG $THEN DBMS_OUTPUT.PUT_LINE('Processing "' || t.OWNER || '"."' || t.TYPE_NAME || '".'); $END
	V_ANYTYPE := ANYTYPE.getPersistent(t.OWNER,t.TYPE_NAME);
	if (V_ANYTYPE is NULL) then DBMS_OUTPUT.PUT_LINE('WTF'); else DBMS_OUTPUT.put_line('Not NULL'); end if;
    V_TYPE_LIST.extend();
	V_TYPE_LIST(V_TYPE_LIST.count) := V_ANYTYPE;
  end loop;
  return serializeTypes(V_TYPE_LIST);
exception
  when others then  
    DBMS_OUTPUT.put_line('Oops" ' || SQLERRM);
end;
--
end;
/
select OBJECT_SERIALIZATION.serializeType('MDSYS','ST_GEOMETRY')
  from DUAL
/
show errors
--
