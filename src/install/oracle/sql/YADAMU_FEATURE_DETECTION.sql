--
declare
/*
**
** "Duck Type" support for JSON Operations
**
** Check for TREAT AS JSON Support (18.1)
** Check for CLOB support (18.1 and Patches)
** Check for VARCHAR2(32K) support.
**
** Create a package that can be used with PL/SQL conditional compliation
*/

  C_NEWLINE CONSTANT CHAR(1) := CHR(10);

  CLOB_UNSUPPORTED EXCEPTION;
  PRAGMA EXCEPTION_INIT( CLOB_UNSUPPORTED , -40449);

  OBJECTS_UNSUPPORTED EXCEPTION;
  PRAGMA EXCEPTION_INIT( OBJECTS_UNSUPPORTED , -40654);

  JSON_DATA_TYPE_UNSUPPORTED EXCEPTION;
  PRAGMA EXCEPTION_INIT( JSON_DATA_TYPE_UNSUPPORTED , -40449);

  EXTENDED_STRING_UNSUPPORTED EXCEPTION;
  PRAGMA EXCEPTION_INIT( EXTENDED_STRING_UNSUPPORTED , -1489);

  TREAT_AS_JSON_UNSUPPORTED EXCEPTION;
  PRAGMA EXCEPTION_INIT( TREAT_AS_JSON_UNSUPPORTED , -902);

  WELL_THAT_PLSQL_DOES_NOT_WORK EXCEPTION;
  PRAGMA EXCEPTION_INIT( WELL_THAT_PLSQL_DOES_NOT_WORK , -6550);

  COLLECT_PLSQL_DOES_NOT_WORK EXCEPTION;
  PRAGMA EXCEPTION_INIT( COLLECT_PLSQL_DOES_NOT_WORK , -22814);

  WHAT_IS_CLOUD EXCEPTION;
  PRAGMA EXCEPTION_INIT( WHAT_IS_CLOUD , -2003);

  V_PACKAGE_DEFINITION VARCHAR2(32767);
  
  V_DUMMY                  NUMBER;
  V_ORACLE_MANAGED_SERVICE VARCHAR2(5);
  V_CLOB_SUPPORTED         BOOLEAN := TRUE;
  V_RDBMS_VERSION          VARCHAR2(24);
  V_XML_STORAGE_MODEL      VARCHAR2(17);
  V_INSTALL_TIME           VARCHAR2(28) := TO_CHAR(SYSTIMESTAMP,'YYYY-MM-DD"T"HH24:MI:SS-TZH:TZM');
  V_YADAMU_GUID            VARCHAR2(36) := NULL;
begin

  begin
    execute immediate 'begin :1 := YADAMU_FEATURE_DETECTION.YADAMU_INSTANCE_ID; end;' using V_YADAMU_GUID;
  exception 
    when OTHERS then
	  V_YADAMU_GUID := regexp_replace(rawtohex(sys_guid()), '([A-F0-9]{8})([A-F0-9]{4})([A-F0-9]{4})([A-F0-9]{4})([A-F0-9]{12})', '\1-\2-\3-\4-\5');
  end;

  V_PACKAGE_DEFINITION := 'CREATE OR REPLACE PACKAGE YADAMU_FEATURE_DETECTION AS' || C_NEWLINE
                       || '   YADAMU_INSTANCE_ID                      CONSTANT VARCHAR2(36)  := ''' || V_YADAMU_GUID || ''';' ||  C_NEWLINE
                       || '   YADAMU_INSTALLATION_TIMESTAMP           CONSTANT VARCHAR2(28) := ''' || V_INSTALL_TIME || ''';' || C_NEWLINE;
  
  begin
    select 1
	  into V_DUMMY
	  from ALL_OBJECTS
	 where OWNER = 'MDSYS'
	   and OBJECT_NAME = 'SDO_GEOMETRY' 
	   and OBJECT_TYPE = 'TYPE';
	   
    select 1
	  into V_DUMMY
      from ALL_OBJECTS
     where OWNER = 'MDSYS'
	   and OBJECT_NAME = 'SDO_UTIL' 
	   and OBJECT_TYPE = 'PACKAGE';
	   
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                         || '  SPATIAL_INSTALLED             CONSTANT BOOLEAN      := TRUE;' || C_NEWLINE;
  exception
    when NO_DATA_FOUND then
      V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                           || '  SPATIAL_INSTALLED             CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE;
    when OTHERS then
	  RAISE;
  end;
--
  $IF DBMS_DB_VERSION.VER_LE_11_2 $then
--

  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                         || '  JSON_PARSING_SUPPORTED         CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  JSON_GENERATION_SUPPORTED      CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  CLOB_SUPPORTED                 CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                         || '  EXTENDED_STRING_SUPPORTED      CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                         || '  C_RETURN_TYPE                  CONSTANT VARCHAR2(32) := ''VARCHAR2(4000)'';' || C_NEWLINE
                         || '  C_MAX_STRING_SIZE              CONSTANT NUMBER       := DBMS_LOB.LOBMAXSIZE;' || C_NEWLINE
						 || '  TREAT_AS_JSON_SUPPORTED        CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  OBJECTS_AS_JSON                CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  JSON_DATA_TYPE_SUPPORTED       CONSTANT BOOLEAN      := FALSE;';
                        --                       
  $ELSIF DBMS_DB_VERSION.VER_LE_12_1 $then
--
  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                         || '  JSON_PARSING_SUPPORTED         CONSTANT BOOLEAN      := TRUE;'  || C_NEWLINE
                         || '  JSON_GENERATION_SUPPORTED      CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  CLOB_SUPPORTED                 CONSTANT BOOLEAN      := FALSE;';
	                     
  begin
    select length('X' || RPAD('X',4001)) into V_DUMMY from dual;
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  EXTENDED_STRING_SUPPORTED      CONSTANT BOOLEAN      := TRUE;' || C_NEWLINE
                         || '  C_RETURN_TYPE                  CONSTANT VARCHAR2(32) := ''VARCHAR2(32767)'';' || C_NEWLINE
                         || '  C_MAX_STRING_SIZE              CONSTANT NUMBER       := 32767;';
  exception
    when EXTENDED_STRING_UNSUPPORTED then
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  EXTENDED_STRING_SUPPORTED    CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                           || '  C_RETURN_TYPE                CONSTANT VARCHAR2(32) := ''VARCHAR2(4000)'';' || C_NEWLINE
                           || '  C_MAX_STRING_SIZE            CONSTANT NUMBER       := 4000;';
    when OTHERS then
	  RAISE;
  end;

  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                        || '  TREAT_AS_JSON_SUPPORTED        CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
						|| '  OBJECTS_AS_JSON                CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                        || '  JSON_DATA_TYPE_SUPPORTED       CONSTANT BOOLEAN      := FALSE;';
--
  $ELSE
--
  begin
    V_CLOB_SUPPORTED := TRUE;
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                         || '  JSON_PARSING_SUPPORTED         CONSTANT BOOLEAN      := TRUE;'  || C_NEWLINE
                         || '  JSON_GENERATION_SUPPORTED      CONSTANT BOOLEAN      := TRUE;'  || C_NEWLINE;
						 
    begin
	  -- Test for CLOB Support with JSON_GENERATION
      execute immediate 'select JSON_ARRAY(DUMMY returning CLOB) from DUAL';
      V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  CLOB_SUPPORTED                 CONSTANT BOOLEAN      := TRUE;'     || C_NEWLINE 
                           || '  C_RETURN_TYPE                  CONSTANT VARCHAR2(32) := ''CLOB'';' || C_NEWLINE
                           || '  C_MAX_STRING_SIZE              CONSTANT NUMBER       := DBMS_LOB.LOBMAXSIZE;';
    exception
      when CLOB_UNSUPPORTED then
        V_CLOB_SUPPORTED := FALSE;
	    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                         || '  CLOB_SUPPORTED                 CONSTANT BOOLEAN      := FALSE;';
    when OTHERS then
	  RAISE;
    end;

    begin
      select length('X' || RPAD('X',4001)) into V_DUMMY from dual;
      V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  EXTENDED_STRING_SUPPORTED      CONSTANT BOOLEAN      := TRUE;' || C_NEWLINE;
      if (NOT V_CLOB_SUPPORTED) then
        V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                         || '  C_RETURN_TYPE                CONSTANT VARCHAR2(32) := ''VARCHAR2(32767)'';' || C_NEWLINE
                             || '  C_MAX_STRING_SIZE            CONSTANT NUMBER       := 32767;';
      end if;
    exception
      when EXTENDED_STRING_UNSUPPORTED then
	    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                         || '  EXTENDED_STRING_SUPPORTED    CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE;
        if (NOT V_CLOB_SUPPORTED) then
          V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                           || '  C_RETURN_TYPE              CONSTANT VARCHAR2(32) := ''VARCHAR2(4000)'';' || C_NEWLINE
                               || '  C_MAX_STRING_SIZE          CONSTANT NUMBER       := 4000;';
        end if;
      when OTHERS then
  	  RAISE;
    end;
  
    begin
	  -- Test for Treat as JSON support
      execute immediate 'select TREAT(DUMMY AS JSON) from DUAL';
      V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  TREAT_AS_JSON_SUPPORTED        CONSTANT BOOLEAN := TRUE;';
    exception
      when TREAT_AS_JSON_UNSUPPORTED then
	    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  TREAT_AS_JSON_SUPPORTED      CONSTANT BOOLEAN := FALSE;';
      when OTHERS then  
  	    RAISE;
    end;

    begin
	  -- Test for SQL Object type support with JSON Genearation 
      execute immediate 'select JSON_ARRAY(DBURITYPE(''/sys/dual'')) from DUAL';
      V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  OBJECTS_AS_JSON        CONSTANT BOOLEAN := TRUE;';
    exception
      when OBJECTS_UNSUPPORTED then
	    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  OBJECTS_AS_JSON      CONSTANT BOOLEAN := FALSE;';
      when OTHERS then  
  	    RAISE;
    end;

    begin
	  -- Test for Native JSON Data Type
      execute immediate 'select JSON_QUERY(''{}'',''$'' returning JSON) from dual';
      V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  JSON_DATA_TYPE_SUPPORTED      CONSTANT BOOLEAN      := TRUE;'     || C_NEWLINE;
    exception
      when JSON_DATA_TYPE_UNSUPPORTED then
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  JSON_DATA_TYPE_SUPPORTED      CONSTANT BOOLEAN      := FALSE;'     || C_NEWLINE;
      when OTHERS then
	    RAISE;
    end;
  end;
	

$END
--  
  -- 
  begin
    -- XML Schema Support not avaialble in ATP and ADW
    execute immediate 'begin :1 := dbms_xmlschema.DELETE_CASCADE_FORCE; end;' using out V_DUMMY;
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  XMLSCHEMA_SUPPORTED            CONSTANT BOOLEAN := TRUE;' || C_NEWLINE; 
  exception
    when WELL_THAT_PLSQL_DOES_NOT_WORK then
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  XMLSCHEMA_SUPPORTED          CONSTANT BOOLEAN := FALSE;' || C_NEWLINE;
    when OTHERS then
      RAISE;
  end; 
    
  begin
    -- cast(collect(PLSQL_FUNCTION_RETURNING VARCHAR2 causes ORA-22814: attribute or element value is larger than specified in type in 12.2 and early 18.x releases.
    execute immediate 'select cast(collect(UTL_RAW.CAST_TO_VARCHAR2(''01'')) as T_VC4000_TABLE) from dual';
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '   COLLECT_PLSQL_SUPPORTED       CONSTANT BOOLEAN := TRUE;' || C_NEWLINE; 
  exception
    when COLLECT_PLSQL_DOES_NOT_WORK then
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  COLLECT_PLSQL_SUPPORTED      CONSTANT BOOLEAN := FALSE;' || C_NEWLINE;
    when OTHERS then
      RAISE;
  end; 
  
  begin
    -- Oracle Cloud Service or On-Premise deployment - Eg is Cloud Lockdown in force
    select case 
             when SYS_CONTEXT('USERENV','CLOUD_SERVICE') is NULL then 
	  	      'FALSE' 
		  	 else 
			  'TRUE'
	       end
      into V_ORACLE_MANAGED_SERVICE
      from DUAL;
  exception 
    when WHAT_IS_CLOUD then
	  V_ORACLE_MANAGED_SERVICE := 'FALSE';
	when OTHERS then
	  RAISE;
  end;

  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                   || '  ORACLE_MANAGED_SERVICE           CONSTANT BOOLEAN := ' || V_ORACLE_MANAGED_SERVICE || ';' || C_NEWLINE;
	
  select version 
    into V_RDBMS_VERSION
    from product_component_version 
   where product like '%Oracle%Database%';
--
--  Default XML Storage Mode
--
  begin
    execute immediate 'drop table YADAMU_XML_STORAGE_TEST';
  exception 
    when OTHERS then 
	  NULL;
  end;
 
  execute immediate 'create global temporary table YADAMU_XML_STORAGE_TEST(X XMLTYPE)';
  
  select STORAGE_TYPE 
    into V_XML_STORAGE_MODEL
	from USER_XML_TAB_COLS
   where TABLE_NAME = 'YADAMU_XML_STORAGE_TEST' 
  
  and COLUMN_NAME = 'X';
  
  execute immediate 'drop table YADAMU_XML_STORAGE_TEST';
  
  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                   || '  XML_STORAGE_MODEL CONSTANT VARCHAR2(17) := ''' || V_XML_STORAGE_MODEL || ''';'|| C_NEWLINE;


  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                       || 'END;';
--				      
  execute immediate V_PACKAGE_DEFINITION;
end;
/
--