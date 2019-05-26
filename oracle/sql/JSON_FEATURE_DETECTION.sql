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

  EXTENDED_STRING_UNSUPPORTED EXCEPTION;
  PRAGMA EXCEPTION_INIT( EXTENDED_STRING_UNSUPPORTED , -1489);

  TREAT_AS_JSON_UNSUPPORTED EXCEPTION;
  PRAGMA EXCEPTION_INIT( TREAT_AS_JSON_UNSUPPORTED , -902);

  V_PACKAGE_DEFINITION VARCHAR2(32767);
  
  V_DUMMY NUMBER;
  V_CLOB_SUPPORTED BOOLEAN := TRUE;
begin
  V_PACKAGE_DEFINITION := 'CREATE OR REPLACE PACKAGE JSON_FEATURE_DETECTION AS' || C_NEWLINE;

  $IF DBMS_DB_VERSION.VER_LE_11_2 $THEN
--
  V_CLOB_SUPPORTED := FALSE;
  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                         || '  PARSING_SUPPORTED         CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  GENERATION_SUPPORTED      CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  CLOB_SUPPORTED            CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                         || '  EXTENDED_STRING_SUPPORTED CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                         || '  TREAT_AS_JSON_SUPPORTED   CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                         || '  C_RETURN_TYPE             CONSTANT VARCHAR2(32) := ''VARCHAR2(4000)'';' || C_NEWLINE
                         || '  C_MAX_STRING_SIZE         CONSTANT NUMBER       := 4000;';
--                       
  $ELSIF DBMS_DB_VERSION.VER_LE_12_1 $THEN
--
  V_CLOB_SUPPORTED := FALSE;
  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                         || '  PARSING_SUPPORTED         CONSTANT BOOLEAN      := TRUE;'  || C_NEWLINE
                         || '  GENERATION_SUPPORTED      CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
	                     || '  CLOB_SUPPORTED            CONSTANT BOOLEAN      := FALSE;'

  begin
    select length('X' || RPAD('X',4001)) into V_DUMMY from dual;
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  EXTENDED_STRING_SUPPORTED CONSTANT BOOLEAN      := TRUE;' || C_NEWLINE;
                         || '  C_RETURN_TYPE             CONSTANT VARCHAR2(32) := ''VARCHAR2(32767)'';' || C_NEWLINE
                         || '  C_MAX_STRING_SIZE         CONSTANT NUMBER       := 32767;';
    end if;
  exception
    WHEN EXTENDED_STRING_UNSUPPORTED THEN
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  EXTENDED_STRING_SUPPORTED CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE;
                           || '  C_RETURN_TYPE             CONSTANT VARCHAR2(32) := ''VARCHAR2(4000)'';' || C_NEWLINE
                           || '  C_MAX_STRING_SIZE         CONSTANT NUMBER       := 4000;';
      end if;
    WHEN OTHERS THEN
	  RAISE;
  end;
                         || '  EXTENDED_STRING_SUPPORTED CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE
                         || '  C_RETURN_TYPE             CONSTANT VARCHAR2(32) := NULL;'  || C_NEWLINE
                         || '  C_MAX_STRING_SIZE         CONSTANT NUMBER       := 0;';
--                       
  $ELSE
--
  begin
--
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                         || '  PARSING_SUPPORTED         CONSTANT BOOLEAN      := TRUE;'  || C_NEWLINE
                         || '  GENERATION_SUPPORTED      CONSTANT BOOLEAN      := TRUE;'  || C_NEWLINE;
	                     
    execute immediate 'select JSON_ARRAY(DUMMY returning CLOB) from DUAL';
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  CLOB_SUPPORTED            CONSTANT BOOLEAN      := TRUE;'     || C_NEWLINE 
                         || '  C_RETURN_TYPE             CONSTANT VARCHAR2(32) := ''CLOB'';' || C_NEWLINE
                         || '  C_MAX_STRING_SIZE         CONSTANT NUMBER       := DBMS_LOB.LOBMAXSIZE;';
  exception
    WHEN CLOB_UNSUPPORTED THEN
      V_CLOB_SUPPORTED := FALSE;
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  CLOB_SUPPORTED          CONSTANT BOOLEAN      := FALSE;';
    WHEN OTHERS THEN
	  RAISE;
  end;

  begin
    select length('X' || RPAD('X',4001)) into V_DUMMY from dual;
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  EXTENDED_STRING_SUPPORTED CONSTANT BOOLEAN      := TRUE;' || C_NEWLINE;
    if (NOT V_CLOB_SUPPORTED) then
      V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  C_RETURN_TYPE             CONSTANT VARCHAR2(32) := ''VARCHAR2(32767)'';' || C_NEWLINE
                           || '  C_MAX_STRING_SIZE         CONSTANT NUMBER       := 32767;';
    end if;
  exception
    WHEN EXTENDED_STRING_UNSUPPORTED THEN
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  EXTENDED_STRING_SUPPORTED CONSTANT BOOLEAN      := FALSE;' || C_NEWLINE;
      if (NOT V_CLOB_SUPPORTED) then
        V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                         || '  C_RETURN_TYPE             CONSTANT VARCHAR2(32) := ''VARCHAR2(4000)'';' || C_NEWLINE
                             || '  C_MAX_STRING_SIZE         CONSTANT NUMBER       := 4000;';
      end if;
    WHEN OTHERS THEN
	  RAISE;
  end;
  
  begin
  
    execute immediate 'select TREAT(DUMMY AS JSON) from DUAL';
    V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                     || '  TREAT_AS_JSON_SUPPORTED CONSTANT BOOLEAN := TRUE;';
  exception
    WHEN TREAT_AS_JSON_UNSUPPORTED THEN
	  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
	                       || '  TREAT_AS_JSON_SUPPORTED CONSTANT BOOLEAN := FALSE;';
    WHEN OTHERS THEN
	  RAISE;
  end;
--
$END
--  

  V_PACKAGE_DEFINITION := V_PACKAGE_DEFINITION
                       || 'END;';
                                         
  execute immediate V_PACKAGE_DEFINITION;
end;
/
--