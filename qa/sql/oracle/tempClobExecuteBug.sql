--
-- Oracle 26ai (23.26.3): after an INSERT ... SELECT that calls a function converting XMLSERIALIZE
-- output to a BLOB, EXECUTE IMMEDIATE (or DBMS_SQL.PARSE) of a temporary CLOB holding a valid
-- statement fails at parse with ORA-00900: invalid SQL statement. Reading from the CLOB first
-- (one character is enough) avoids the error. 19c and 12.2 are not affected.
--
-- Found through YADAMU_COMPARE (tracking item 20). Run as any user with a tablespace that
-- supports SecureFile LOBs (USERS here); it creates and drops its own objects.
--
set serveroutput on feedback off lines 200
whenever sqlerror continue
drop table TEMP_CLOB_BUG_XML purge;
drop table TEMP_CLOB_BUG_LOG purge;
create table TEMP_CLOB_BUG_XML (DOC XMLTYPE) tablespace USERS;
create table TEMP_CLOB_BUG_LOG (N NUMBER) tablespace USERS;
insert into TEMP_CLOB_BUG_XML values (XMLTYPE('<a>1</a>'));
commit;
create or replace function TEMP_CLOB_BUG_TO_BLOB(P_CLOB CLOB) return BLOB
as
  V_BLOB         BLOB;
  V_DEST_OFFSET  INTEGER := 1;
  V_SRC_OFFSET   INTEGER := 1;
  V_LANG_CONTEXT INTEGER := 0;
  V_WARNING      INTEGER;
begin
  DBMS_LOB.CREATETEMPORARY(V_BLOB, TRUE);
  DBMS_LOB.CONVERTTOBLOB(V_BLOB, P_CLOB, DBMS_LOB.LOBMAXSIZE, V_DEST_OFFSET, V_SRC_OFFSET, NLS_CHARSET_ID('AL32UTF8'), V_LANG_CONTEXT, V_WARNING);
  return V_BLOB;
end;
/
declare
  C_TRIGGER CONSTANT VARCHAR2(400) := 'insert into TEMP_CLOB_BUG_LOG (N) select count(*) from TEMP_CLOB_BUG_XML t where DBMS_LOB.GETLENGTH(TEMP_CLOB_BUG_TO_BLOB(XMLSERIALIZE(CONTENT t.DOC AS CLOB))) > 0';
  procedure TEST(P_LABEL VARCHAR2, P_RUN_TRIGGER BOOLEAN, P_READ_FIRST BOOLEAN)
  as
    V_STATEMENT CLOB;
    V_FIRST     VARCHAR2(4);
  begin
    if P_RUN_TRIGGER then
      execute immediate C_TRIGGER;
    end if;
    -- A temporary CLOB, built by concatenation, holding a valid statement
    V_STATEMENT := 'select count(*) ' || 'from dual';
    if P_READ_FIRST then
      V_FIRST := DBMS_LOB.SUBSTR(V_STATEMENT,1,1);
    end if;
    begin
      execute immediate V_STATEMENT;
      DBMS_OUTPUT.PUT_LINE(rpad(P_LABEL,64) || 'OK');
    exception
      when others then DBMS_OUTPUT.PUT_LINE(rpad(P_LABEL,64) || SQLERRM);
    end;
    rollback;
  end;
begin
  DBMS_OUTPUT.PUT_LINE(DBMS_DB_VERSION.VERSION || '.' || DBMS_DB_VERSION.RELEASE);
  TEST('1 CLOB statement, no INSERT first (control)', FALSE, FALSE);
  TEST('2 CLOB statement after the INSERT (the bug)', TRUE, FALSE);
  TEST('3 CLOB statement after the INSERT, read first (workaround)', TRUE, TRUE);
  TEST('4 CLOB statement, no INSERT first (control again)', FALSE, FALSE);
end;
/
drop function TEMP_CLOB_BUG_TO_BLOB;
drop table TEMP_CLOB_BUG_XML purge;
drop table TEMP_CLOB_BUG_LOG purge;
exit
